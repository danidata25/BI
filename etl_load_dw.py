#!/usr/bin/env python3
"""
Olist DW ETL Script  -  ELT edition (Postgres does the work, not pandas)
========================================================================
This script loads the Kaggle Olist CSV files into the olist_dw star schema.

Design change vs. the original pandas version
----------------------------------------------
The first version read every CSV into a pandas DataFrame, transformed the
data row-by-row in Python (merge / groupby / apply), and pushed the result
back with execute_values. That keeps the whole dataset in the client's RAM
and does the heavy joins/aggregations in a single Python process - it works,
but it does not scale: at 10x-100x the data it becomes memory-bound and slow.

This version follows the ELT pattern instead:
  1. EXTRACT + LOAD - stream each CSV straight into a raw staging table with
     Postgres COPY (the fastest bulk-load path there is; no row-by-row Python).
  2. TRANSFORM - every join, aggregation and derivation runs as set-based SQL
     inside Postgres (INSERT ... SELECT). The database engine, which is built
     and indexed for exactly this, does the work; the Python process only
     orchestrates and never holds the data.

There is no pandas or numpy import anywhere in this file.

Reproducibility note (synthetic columns)
-----------------------------------------
A few columns are synthetic (documented in the report): customer_age,
customer_gender, customer_signup_date and seller_join_date. The pandas
version generated these with a numpy seed. Here they are derived
DETERMINISTICALLY from a hash of the natural business key (customer_unique_id
/ seller_id), so a given key always yields the same value regardless of row
order or run - a stronger form of reproducibility. The concrete values differ
from the old numpy-seeded run, so the dimension tables must be reloaded (this
script truncates and repopulates them). All non-synthetic data is unchanged.

Usage:
  1. Set PG_DSN in the .env file next to this script.
  2. Run:  python etl_load_dw.py
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load .env from the same directory as this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────
PG_DSN     = os.environ["PG_DSN"]
DW_SCHEMA  = "olist_dw"
STG_SCHEMA = "olist_stg"
DATA_DIR   = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────────────
# LOOKUPS  (kept in Python only to GENERATE SQL - not to process data)
# ─────────────────────────────────────────────────────────────────────
STATE_REGION = {
    "AM": "North",       "PA": "North",       "AC": "North",
    "RO": "North",       "RR": "North",       "AP": "North",       "TO": "North",
    "MA": "Northeast",   "PI": "Northeast",   "CE": "Northeast",   "RN": "Northeast",
    "PB": "Northeast",   "PE": "Northeast",   "AL": "Northeast",   "SE": "Northeast",
    "BA": "Northeast",
    "MT": "Center-West", "MS": "Center-West", "GO": "Center-West", "DF": "Center-West",
    "SP": "Southeast",   "RJ": "Southeast",   "MG": "Southeast",   "ES": "Southeast",
    "PR": "South",       "SC": "South",       "RS": "South",
}

# Keyword → category group. First group whose keyword is a substring wins
# (same precedence as the original Python dict order).
CATEGORY_GROUP_KEYWORDS = {
    "Electronics":      ["electronics", "computer", "telephony", "tablet", "console", "game",
                         "audio", "watch", "signal", "portable"],
    "Home & Garden":    ["furniture", "bed", "bath", "housewares", "home_appliance", "kitchen",
                         "garden", "air_condition", "home_confort", "small_appliance"],
    "Fashion":          ["fashion", "bag", "clothing", "underwear", "shoe", "luggage", "apparel"],
    "Health & Beauty":  ["health", "beauty", "perfumery", "diaper", "hygiene"],
    "Sports & Leisure": ["sport", "leisure", "toy", "baby", "christmas"],
    "Books & Media":    ["book", "music", "dvd", "cd", "musical"],
    "Food & Drinks":    ["food", "drink", "beverage"],
    "Auto":             ["auto", "vehicle"],
    "Construction":     ["construction", "tool", "security", "industry", "agro"],
    "Art & Craft":      ["art", "craft", "stationery", "party"],
    "Other":            [],
}

# ─────────────────────────────────────────────────────────────────────
# STAGING TABLES  (all columns TEXT - cast happens in the SQL transforms)
# ─────────────────────────────────────────────────────────────────────
STAGING = {
    "stg_customers":   (["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                         "customer_city", "customer_state"],
                        "olist_customers_dataset.csv"),
    "stg_orders":      (["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                         "order_approved_at", "order_delivered_carrier_date",
                         "order_delivered_customer_date", "order_estimated_delivery_date"],
                        "olist_orders_dataset.csv"),
    "stg_items":       (["order_id", "order_item_id", "product_id", "seller_id",
                         "shipping_limit_date", "price", "freight_value"],
                        "olist_order_items_dataset.csv"),
    "stg_products":    (["product_id", "product_category_name", "product_name_lenght",
                         "product_description_lenght", "product_photos_qty", "product_weight_g",
                         "product_length_cm", "product_height_cm", "product_width_cm"],
                        "olist_products_dataset.csv"),
    "stg_sellers":     (["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
                        "olist_sellers_dataset.csv"),
    "stg_reviews":     (["review_id", "order_id", "review_score", "review_comment_title",
                         "review_comment_message", "review_creation_date",
                         "review_answer_timestamp"],
                        "olist_order_reviews_dataset.csv"),
    "stg_translation": (["product_category_name", "product_category_name_english"],
                        "product_category_name_translation.csv"),
}

# ─────────────────────────────────────────────────────────────────────
# SQL SNIPPET GENERATORS
# ─────────────────────────────────────────────────────────────────────
STATE_REGION_VALUES = ",".join(f"('{s}','{r}')" for s, r in STATE_REGION.items())


def rnd(key_sql: str, salt: str) -> str:
    """Deterministic pseudo-random value in [0,1) derived from a text key.
    Uses md5(key||salt) → first 32 bits → normalized. Stable across runs."""
    return (f"(((('x' || substr(md5({key_sql} || '{salt}'), 1, 8))::bit(32)::int)::bigint "
            f"+ 2147483648)::numeric / 4294967296.0)")


def group_case_sql(col: str) -> str:
    """CASE expression mapping a category name to its product_category_group,
    using literal substring matching (strpos) to mirror Python's `kw in name`."""
    whens = []
    for group, kws in CATEGORY_GROUP_KEYWORDS.items():
        if not kws:
            continue
        conds = " OR ".join(f"strpos(lower({col}), '{kw}') > 0" for kw in kws)
        whens.append(f"WHEN {conds} THEN '{group}'")
    return "CASE " + "\n         ".join(whens) + "\n         ELSE 'Other' END"


# ─────────────────────────────────────────────────────────────────────
# TRANSFORM SQL  (each runs entirely inside Postgres)
# ─────────────────────────────────────────────────────────────────────
SQL_DIM_DATE = f"""
INSERT INTO {DW_SCHEMA}.dim_date
    (sk_date, date, day, month, month_name, quarter, year, day_of_week, is_weekend)
WITH raw AS (
    SELECT NULLIF(trim(order_purchase_timestamp),      '')::timestamp::date AS dt FROM {STG_SCHEMA}.stg_orders
    UNION SELECT NULLIF(trim(order_approved_at),        '')::timestamp::date FROM {STG_SCHEMA}.stg_orders
    UNION SELECT NULLIF(trim(order_delivered_carrier_date),  '')::timestamp::date FROM {STG_SCHEMA}.stg_orders
    UNION SELECT NULLIF(trim(order_delivered_customer_date), '')::timestamp::date FROM {STG_SCHEMA}.stg_orders
    UNION SELECT NULLIF(trim(order_estimated_delivery_date), '')::timestamp::date FROM {STG_SCHEMA}.stg_orders
),
bounds AS (SELECT min(dt) AS mn, max(dt) AS mx FROM raw WHERE dt IS NOT NULL),
days AS (
    SELECT generate_series(mn, mx, interval '1 day')::date AS dt FROM bounds
)
SELECT to_char(dt, 'YYYYMMDD')::int,
       dt,
       extract(day   FROM dt)::smallint,
       extract(month FROM dt)::smallint,
       trim(to_char(dt, 'Month')),
       extract(quarter FROM dt)::smallint,
       extract(year  FROM dt)::smallint,
       trim(to_char(dt, 'Day')),
       (extract(dow FROM dt) IN (0, 6))
FROM days;
"""

SQL_DIM_CUSTOMER = f"""
INSERT INTO {DW_SCHEMA}.dim_customer
    (customer_unique_id, customer_city, customer_state, customer_region,
     customer_age, customer_age_group, customer_gender, customer_signup_date, customer_segment)
WITH cust AS (
    SELECT c.customer_unique_id AS cuid,
           min(c.customer_city)  AS city,
           min(c.customer_state) AS state,
           count(o.order_purchase_timestamp) AS order_count,
           min(NULLIF(trim(o.order_purchase_timestamp), '')::timestamp) AS first_order
    FROM {STG_SCHEMA}.stg_customers c
    LEFT JOIN {STG_SCHEMA}.stg_orders o ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),
enr AS (
    SELECT cuid, city, state, order_count, first_order,
           (18 + floor({rnd('cuid', 'age')} * 53))::int AS age,
           CASE WHEN {rnd('cuid', 'gender')} < 0.48 THEN 'M' ELSE 'F' END AS gender,
           (first_order::date - (60 + floor({rnd('cuid', 'signup')} * 671))::int) AS signup_date
    FROM cust
)
SELECT enr.cuid, enr.city, enr.state,
       COALESCE(sr.region, 'Other'),
       enr.age::smallint,
       CASE WHEN enr.age <= 24 THEN '18-24'
            WHEN enr.age <= 34 THEN '25-34'
            WHEN enr.age <= 44 THEN '35-44'
            WHEN enr.age <= 54 THEN '45-54'
            WHEN enr.age <= 64 THEN '55-64'
            ELSE '65+' END,
       enr.gender,
       enr.signup_date,
       CASE WHEN enr.order_count = 1 THEN 'Occasional'
            WHEN enr.order_count <= 4 THEN 'Regular'
            ELSE 'Loyal' END
FROM enr
LEFT JOIN (VALUES {STATE_REGION_VALUES}) AS sr(state, region) ON sr.state = enr.state
ORDER BY enr.cuid;
"""

SQL_DIM_SELLER = f"""
INSERT INTO {DW_SCHEMA}.dim_seller
    (seller_id, seller_city, seller_state, seller_region, seller_main_category,
     seller_size_category, seller_tier, seller_join_date, seller_plan,
     subscription_fee_monthly, commission_rate, payment_rate,
     active_months, subscription_revenue_total)
WITH itmcat AS (
    SELECT i.seller_id,
           i.order_item_id,
           i.price::numeric AS price,
           COALESCE(t.product_category_name_english, p.product_category_name) AS category_en
    FROM {STG_SCHEMA}.stg_items i
    LEFT JOIN {STG_SCHEMA}.stg_products p    ON p.product_id = i.product_id
    LEFT JOIN {STG_SCHEMA}.stg_translation t ON t.product_category_name = p.product_category_name
),
seller_agg AS (
    SELECT seller_id,
           count(order_item_id) AS total_items,
           sum(price)           AS total_revenue,
           mode() WITHIN GROUP (ORDER BY category_en) AS main_category
    FROM itmcat GROUP BY seller_id
),
span AS (
    SELECT seller_id,
           min(NULLIF(trim(shipping_limit_date), '')::timestamp) AS dmin,
           max(NULLIF(trim(shipping_limit_date), '')::timestamp) AS dmax
    FROM {STG_SCHEMA}.stg_items GROUP BY seller_id
),
base AS (
    SELECT s.seller_id, s.seller_city, s.seller_state,
           COALESCE(sr.region, 'Other') AS region,
           sa.main_category, sa.total_items, sa.total_revenue,
           CASE WHEN sp.dmin IS NULL OR sp.dmax IS NULL THEN 1
                ELSE GREATEST(1, (extract(year  FROM sp.dmax) - extract(year  FROM sp.dmin))::int * 12
                              + (extract(month FROM sp.dmax) - extract(month FROM sp.dmin))::int + 1)
           END AS active_months,
           (DATE '2016-09-01' - (30 + floor({rnd('s.seller_id', 'join')} * 1066))::int) AS join_date
    FROM {STG_SCHEMA}.stg_sellers s
    LEFT JOIN seller_agg sa ON sa.seller_id = s.seller_id
    LEFT JOIN span sp        ON sp.seller_id = s.seller_id
    LEFT JOIN (VALUES {STATE_REGION_VALUES}) AS sr(state, region) ON sr.state = s.seller_state
),
plans AS (
    SELECT base.*,
           CASE WHEN COALESCE(total_items, 0) >= 500 THEN 'Enterprise'
                WHEN COALESCE(total_items, 0) >= 50  THEN 'Pro'
                WHEN COALESCE(total_items, 0) >= 10  THEN 'Starter'
                ELSE 'Free' END AS plan,
           CASE WHEN COALESCE(total_items, 0) >= 500 THEN 699.00
                WHEN COALESCE(total_items, 0) >= 50  THEN 299.00
                WHEN COALESCE(total_items, 0) >= 10  THEN 99.00
                ELSE 0.00 END AS fee,
           CASE WHEN COALESCE(total_items, 0) >= 500 THEN 0.1000
                WHEN COALESCE(total_items, 0) >= 50  THEN 0.1300
                WHEN COALESCE(total_items, 0) >= 10  THEN 0.1600
                ELSE 0.2000 END AS commission,
           CASE WHEN COALESCE(total_items, 0) >= 500 THEN 0.0247
                WHEN COALESCE(total_items, 0) >= 50  THEN 0.0277
                WHEN COALESCE(total_items, 0) >= 10  THEN 0.0287
                ELSE 0.0297 END AS payment
    FROM base
)
SELECT seller_id, seller_city, seller_state, region, main_category,
       CASE WHEN total_items IS NULL OR total_items < 50 THEN 'Small'
            WHEN total_items < 500 THEN 'Medium'
            ELSE 'Large' END,
       CASE WHEN total_revenue IS NULL OR total_revenue < 5000 THEN 'Bronze'
            WHEN total_revenue < 50000 THEN 'Silver'
            ELSE 'Gold' END,
       join_date, plan, fee, commission, payment,
       active_months::smallint,
       round(fee * active_months, 2)
FROM plans
ORDER BY seller_id;
"""

SQL_DIM_PRODUCT = f"""
INSERT INTO {DW_SCHEMA}.dim_product
    (product_id, product_category, product_category_group, list_price,
     price_band, price_band_rank, unit_cost, is_premium)
WITH avgp AS (
    SELECT product_id, avg(price::numeric) AS list_price
    FROM {STG_SCHEMA}.stg_items GROUP BY product_id
),
base AS (
    SELECT p.product_id,
           COALESCE(t.product_category_name_english, p.product_category_name) AS category,
           round(a.list_price, 2) AS list_price
    FROM {STG_SCHEMA}.stg_products p
    LEFT JOIN {STG_SCHEMA}.stg_translation t ON t.product_category_name = p.product_category_name
    LEFT JOIN avgp a ON a.product_id = p.product_id
)
SELECT product_id,
       category,
       {group_case_sql('category')},
       list_price,
       CASE WHEN list_price IS NULL THEN 'Unknown'
            WHEN list_price < 50   THEN 'Budget'
            WHEN list_price < 200  THEN 'Mid'
            WHEN list_price < 500  THEN 'Premium'
            ELSE 'Luxury' END,
       CASE WHEN list_price IS NULL THEN 0
            WHEN list_price < 50   THEN 1
            WHEN list_price < 200  THEN 2
            WHEN list_price < 500  THEN 3
            ELSE 4 END,
       round(list_price * 0.60, 2),
       (COALESCE(list_price, -1) >= 500)
FROM base
ORDER BY product_id;
"""

SQL_FACT_ORDER_ITEM = f"""
INSERT INTO {DW_SCHEMA}.fact_order_item
    (order_id, order_item_id, sk_date_purchase, sk_date_carrier, sk_date_delivered,
     sk_date_estimated_delivery, sk_customer, sk_seller, sk_product,
     price, freight_value, revenue, gross_profit, unit_cost,
     delivery_days, seller_handling_days, payment_approval_days, seller_prep_days,
     carrier_transit_days, is_on_time, review_score)
WITH best_review AS (
    SELECT order_id, max(review_score::int) AS review_score
    FROM {STG_SCHEMA}.stg_reviews
    WHERE NULLIF(trim(review_score), '') IS NOT NULL
    GROUP BY order_id
),
base AS (
    SELECT i.order_id,
           i.order_item_id::smallint AS order_item_id,
           NULLIF(trim(o.order_purchase_timestamp),      '')::timestamp AS purchase_ts,
           NULLIF(trim(o.order_approved_at),             '')::timestamp AS approved_ts,
           NULLIF(trim(o.order_delivered_carrier_date),  '')::timestamp AS carrier_ts,
           NULLIF(trim(o.order_delivered_customer_date), '')::timestamp AS delivered_ts,
           NULLIF(trim(o.order_estimated_delivery_date), '')::timestamp AS estimated_ts,
           i.price::numeric         AS price,
           i.freight_value::numeric AS freight_value,
           dc.sk_customer, ds.sk_seller, dp.sk_product, dp.unit_cost,
           br.review_score::smallint AS review_score
    FROM {STG_SCHEMA}.stg_items i
    JOIN {STG_SCHEMA}.stg_orders o     ON o.order_id = i.order_id
    LEFT JOIN {STG_SCHEMA}.stg_customers c ON c.customer_id = o.customer_id
    LEFT JOIN {DW_SCHEMA}.dim_customer dc  ON dc.customer_unique_id = c.customer_unique_id
    LEFT JOIN {DW_SCHEMA}.dim_seller ds    ON ds.seller_id = i.seller_id
    LEFT JOIN {DW_SCHEMA}.dim_product dp    ON dp.product_id = i.product_id
    LEFT JOIN best_review br               ON br.order_id = i.order_id
),
calc AS (
    SELECT base.*,
           to_char(purchase_ts,  'YYYYMMDD')::int AS sk_date_purchase,
           to_char(carrier_ts,   'YYYYMMDD')::int AS sk_date_carrier,
           to_char(delivered_ts, 'YYYYMMDD')::int AS sk_date_delivered,
           to_char(estimated_ts, 'YYYYMMDD')::int AS sk_date_estimated_delivery,
           CASE WHEN delivered_ts IS NOT NULL AND purchase_ts IS NOT NULL
                THEN floor(extract(epoch FROM (delivered_ts - purchase_ts)) / 86400)::int END AS delivery_days,
           CASE WHEN carrier_ts IS NOT NULL AND purchase_ts IS NOT NULL
                THEN floor(extract(epoch FROM (carrier_ts - purchase_ts)) / 86400)::int END AS seller_handling_days
    FROM base
),
calc2 AS (
    SELECT calc.*,
           CASE WHEN seller_handling_days IS NULL THEN NULL
                WHEN approved_ts IS NULL OR purchase_ts IS NULL THEN 0
                ELSE GREATEST(0, LEAST(
                        floor(extract(epoch FROM (approved_ts - purchase_ts)) / 86400)::int,
                        seller_handling_days))
           END AS payment_approval_days
    FROM calc
),
calc3 AS (
    SELECT calc2.*,
           CASE WHEN seller_handling_days IS NOT NULL AND payment_approval_days IS NOT NULL
                THEN seller_handling_days - payment_approval_days END AS seller_prep_days,
           CASE WHEN delivery_days IS NOT NULL AND seller_handling_days IS NOT NULL
                THEN delivery_days - seller_handling_days END AS carrier_transit_days,
           CASE WHEN delivered_ts IS NULL OR estimated_ts IS NULL THEN NULL
                WHEN delivered_ts <= estimated_ts THEN 1 ELSE 0 END AS is_on_time
    FROM calc2
)
SELECT order_id, order_item_id,
       sk_date_purchase, sk_date_carrier, sk_date_delivered, sk_date_estimated_delivery,
       sk_customer, sk_seller, sk_product,
       round(price, 2), round(freight_value, 2), round(price + freight_value, 2),
       round(price - COALESCE(unit_cost, 0), 2), unit_cost,
       delivery_days::smallint, seller_handling_days::smallint, payment_approval_days::smallint,
       seller_prep_days::smallint, carrier_transit_days::smallint,
       is_on_time::smallint, review_score
FROM calc3
WHERE sk_date_purchase IS NOT NULL
  AND sk_customer IS NOT NULL
  AND sk_seller   IS NOT NULL
  AND sk_product  IS NOT NULL;
"""

SQL_FACT_DAILY = f"""
INSERT INTO {DW_SCHEMA}.fact_daily_seller_category
    (sk_date, sk_seller, sk_product_category, orders_count, items_count,
     revenue_total, freight_total, gross_profit_total, on_time_deliveries,
     delivered_items, review_score_sum, reviews_count)
WITH foi AS (
    SELECT f.*, COALESCE(dp.product_category, 'Unknown') AS product_category
    FROM {DW_SCHEMA}.fact_order_item f
    JOIN {DW_SCHEMA}.dim_product dp ON dp.sk_product = f.sk_product
),
cat_map AS (
    SELECT product_category,
           row_number() OVER (ORDER BY product_category) AS sk_product_category
    FROM (SELECT DISTINCT product_category FROM foi) d
)
SELECT f.sk_date_purchase,
       f.sk_seller,
       cm.sk_product_category,
       count(DISTINCT f.order_id),
       count(*),
       round(sum(f.revenue), 2),
       round(sum(f.freight_value), 2),
       round(sum(f.gross_profit), 2),
       sum(CASE WHEN f.is_on_time = 1 THEN 1 ELSE 0 END),
       count(f.sk_date_delivered),
       sum(f.review_score),
       count(f.review_score)
FROM foi f
JOIN cat_map cm ON cm.product_category = f.product_category
GROUP BY f.sk_date_purchase, f.sk_seller, cm.sk_product_category;
"""

SQL_FACT_SUBSCRIPTION = f"""
INSERT INTO {DW_SCHEMA}.fact_seller_subscription (sk_seller, sk_date, subscription_amount)
WITH span AS (
    SELECT seller_id,
           min(NULLIF(trim(shipping_limit_date), '')::timestamp) AS dmin,
           max(NULLIF(trim(shipping_limit_date), '')::timestamp) AS dmax
    FROM {STG_SCHEMA}.stg_items GROUP BY seller_id
),
seller_fee AS (
    SELECT ds.sk_seller, ds.subscription_fee_monthly AS fee, span.dmin, span.dmax
    FROM span
    JOIN {DW_SCHEMA}.dim_seller ds ON ds.seller_id = span.seller_id
    WHERE ds.subscription_fee_monthly > 0 AND span.dmin IS NOT NULL AND span.dmax IS NOT NULL
),
months AS (
    SELECT sk_seller, fee,
           generate_series(date_trunc('month', dmin), date_trunc('month', dmax),
                           interval '1 month')::date AS mstart
    FROM seller_fee
),
bounds AS (SELECT min(sk_date) AS sk_lo, max(sk_date) AS sk_hi FROM {DW_SCHEMA}.dim_date),
resolved AS (
    SELECT m.sk_seller, m.fee,
           COALESCE(
               (SELECT min(d.sk_date) FROM {DW_SCHEMA}.dim_date d
                 WHERE d.year  = extract(year  FROM m.mstart)::int
                   AND d.month = extract(month FROM m.mstart)::int),
               CASE WHEN (extract(year FROM m.mstart) * 10000
                          + extract(month FROM m.mstart) * 100 + 1) < b.sk_lo
                    THEN b.sk_lo ELSE b.sk_hi END
           ) AS sk_date
    FROM months m CROSS JOIN bounds b
)
SELECT sk_seller, sk_date, round(sum(fee), 2)
FROM resolved
GROUP BY sk_seller, sk_date;
"""

TRANSFORMS = [
    ("dim_date",                   SQL_DIM_DATE),
    ("dim_customer",               SQL_DIM_CUSTOMER),
    ("dim_seller",                 SQL_DIM_SELLER),
    ("dim_product",                SQL_DIM_PRODUCT),
    ("fact_order_item",            SQL_FACT_ORDER_ITEM),
    ("fact_daily_seller_category", SQL_FACT_DAILY),
    ("fact_seller_subscription",   SQL_FACT_SUBSCRIPTION),
]

DW_TABLES = ["dim_date", "dim_customer", "dim_seller", "dim_product",
             "fact_order_item", "fact_daily_seller_category", "fact_seller_subscription"]


# ─────────────────────────────────────────────────────────────────────
# STEPS
# ─────────────────────────────────────────────────────────────────────
def build_staging(cur):
    print("\n[1] Rebuilding staging schema and COPYing raw CSVs...")
    cur.execute(f"DROP SCHEMA IF EXISTS {STG_SCHEMA} CASCADE")
    cur.execute(f"CREATE SCHEMA {STG_SCHEMA}")
    for table, (cols, fname) in STAGING.items():
        col_ddl = ", ".join(f"{c} TEXT" for c in cols)
        cur.execute(f"CREATE TABLE {STG_SCHEMA}.{table} ({col_ddl})")
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            print(f"  [WARN] Missing file: {fname}  →  {table} left empty")
            continue
        with open(path, "r", encoding="utf-8", newline="") as fh:
            cur.copy_expert(
                f"COPY {STG_SCHEMA}.{table} FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
        cur.execute(f"SELECT count(*) FROM {STG_SCHEMA}.{table}")
        print(f"  [COPY] {table:<16} {cur.fetchone()[0]:>10,} rows  ({fname})")


def truncate_dw(cur):
    print("\n[2] Truncating warehouse tables (RESTART IDENTITY CASCADE)...")
    tbls = ", ".join(f"{DW_SCHEMA}.{t}" for t in DW_TABLES)
    cur.execute(f"TRUNCATE TABLE {tbls} RESTART IDENTITY CASCADE")


def run_transforms(cur, conn):
    print("\n[3] Running SQL transforms (Postgres does the work)...")
    for i, (name, sql) in enumerate(TRANSFORMS, start=1):
        cur.execute(sql)
        conn.commit()
        cur.execute(f"SELECT count(*) FROM {DW_SCHEMA}.{name}")
        print(f"  [{i}/{len(TRANSFORMS)}] {name:<28} {cur.fetchone()[0]:>10,} rows")


def drop_staging(cur):
    cur.execute(f"DROP SCHEMA IF EXISTS {STG_SCHEMA} CASCADE")


def main():
    print("=" * 62)
    print("Olist DW ETL  -  ELT edition (COPY + set-based SQL, no pandas)")
    print("=" * 62)

    try:
        conn = psycopg2.connect(PG_DSN)
    except psycopg2.OperationalError as e:
        print(f"[ERROR] Cannot connect to DB: {e}")
        sys.exit(1)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        build_staging(cur)
        conn.commit()
        truncate_dw(cur)
        conn.commit()
        run_transforms(cur, conn)
        drop_staging(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        cur.close()
        conn.close()
        raise

    print("\n" + "=" * 62)
    print("ETL complete. Final warehouse row counts:")
    for t in DW_TABLES:
        cur.execute(f"SELECT count(*) FROM {DW_SCHEMA}.{t}")
        print(f"  {t:<35} {cur.fetchone()[0]:>10,}")
    cur.close()
    conn.close()
    print("=" * 62)


if __name__ == "__main__":
    main()
