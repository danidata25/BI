#!/usr/bin/env python3
"""
Olist DW ETL Script
====================
Reads the Kaggle Olist CSV files from the BI directory, transforms the data,
and loads it into the olist_dw PostgreSQL schema.

Expected CSV files in the same folder as this script:
  - olist_customers_dataset.csv
  - olist_orders_dataset.csv
  - olist_order_items_dataset.csv
  - olist_products_dataset.csv
  - olist_sellers_dataset.csv
  - olist_order_reviews_dataset.csv
  - product_category_name_translation.csv

Usage:
  1. Adjust DB_* constants below to match your pgAdmin connection
  2. Run:  python etl_load_dw.py
"""

import os
import sys
import random
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta
from dotenv import load_dotenv

# Load .env from the same directory as this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# ─────────────────────────────────────────────────────────────────────
# DB CONNECTION  –  loaded from .env (PG_DSN)
# ─────────────────────────────────────────────────────────────────────
PG_DSN    = os.environ["PG_DSN"]   # e.g. "dbname=postgres user=postgres password=... host=127.0.0.1 port=5432"
DB_SCHEMA = "olist_dw"

# ─────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────────────
# REPRODUCIBILITY
# ─────────────────────────────────────────────────────────────────────
random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────
# LOOKUP TABLES
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

# Keyword → category group mapping (checked against translated category names)
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


def category_to_group(cat: str) -> str:
    if not cat:
        return "Other"
    c = str(cat).lower()
    for group, keywords in CATEGORY_GROUP_KEYWORDS.items():
        if any(kw in c for kw in keywords):
            return group
    return "Other"


def price_band(price) -> str:
    if pd.isna(price):
        return "Unknown"
    if price < 50:
        return "Budget"
    if price < 200:
        return "Mid"
    if price < 500:
        return "Premium"
    return "Luxury"


def nan_to_none(val):
    """Convert numpy/pandas NaN/NaT to Python None, and numpy scalars to
    Python native types so psycopg2 can serialize them correctly."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    # Numpy integer → Python int
    if isinstance(val, np.integer):
        return int(val)
    # Numpy float → Python float
    if isinstance(val, np.floating):
        return float(val)
    # Numpy bool → Python bool
    if isinstance(val, np.bool_):
        return bool(val)
    return val


def row_to_none(row):
    """Apply nan_to_none to every element in a tuple/list."""
    return tuple(nan_to_none(v) for v in row)


# ─────────────────────────────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────────────────────────────
def get_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    return conn


def truncate_table(cur, table: str):
    cur.execute(f"TRUNCATE TABLE {DB_SCHEMA}.{table} CASCADE")


def bulk_insert(cur, table: str, columns: list, rows: list, page_size: int = 2000):
    if not rows:
        print(f"  [SKIP] {table} – no rows to insert")
        return
    col_str = ", ".join(columns)
    sql = f"INSERT INTO {DB_SCHEMA}.{table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cur, sql, rows, page_size=page_size)
    print(f"  [OK]   {table}: {len(rows):,} rows")


# ─────────────────────────────────────────────────────────────────────
# LOAD CSVs
# ─────────────────────────────────────────────────────────────────────
def load_csvs() -> dict:
    files = {
        "customers":    "olist_customers_dataset.csv",
        "orders":       "olist_orders_dataset.csv",
        "items":        "olist_order_items_dataset.csv",
        "products":     "olist_products_dataset.csv",
        "sellers":      "olist_sellers_dataset.csv",
        "reviews":      "olist_order_reviews_dataset.csv",
        "translation":  "product_category_name_translation.csv",
    }
    dfs = {}
    for key, fname in files.items():
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            print(f"  [WARN] Missing file: {fname}  →  skipping")
            dfs[key] = pd.DataFrame()
        else:
            dfs[key] = pd.read_csv(path, low_memory=False)
            print(f"  [CSV] {fname}: {len(dfs[key]):,} rows")
    return dfs


# ─────────────────────────────────────────────────────────────────────
# STEP 1 – dim_date
# ─────────────────────────────────────────────────────────────────────
def build_dim_date(orders: pd.DataFrame) -> list:
    """
    Collect every date referenced in orders, then generate a dim_date row
    for each calendar day in the min–max range.
    sk_date = YYYYMMDD integer (e.g. 20180101 → 20180101).
    """
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    all_dates = set()
    for col in date_cols:
        if col in orders.columns:
            parsed = pd.to_datetime(orders[col], errors="coerce").dropna().dt.date
            all_dates.update(parsed)

    if not all_dates:
        print("  [WARN] No dates found in orders – dim_date will be empty")
        return []

    min_date = min(all_dates)
    max_date = max(all_dates)

    rows = []
    current = min_date
    while current <= max_date:
        sk = int(current.strftime("%Y%m%d"))
        rows.append((
            sk,
            current,
            current.day,
            current.month,
            current.strftime("%B"),
            (current.month - 1) // 3 + 1,
            current.year,
            current.strftime("%A"),
            current.weekday() >= 5,
        ))
        current += timedelta(days=1)

    print(f"  [dim_date] date range: {min_date} – {max_date}  →  {len(rows):,} rows")
    return rows


# ─────────────────────────────────────────────────────────────────────
# STEP 2 – dim_customer
# ─────────────────────────────────────────────────────────────────────
def build_dim_customer(customers: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """
    One row per customer_unique_id.
    Simulated fields (documented): customer_age, customer_age_group,
    customer_gender, customer_signup_date, customer_segment.
    """
    # First order date per unique customer (for segment + signup_date simulation)
    cust_orders = customers.merge(
        orders[["customer_id", "order_purchase_timestamp"]],
        on="customer_id", how="left"
    )
    cust_orders["order_purchase_timestamp"] = pd.to_datetime(
        cust_orders["order_purchase_timestamp"], errors="coerce"
    )

    agg = cust_orders.groupby("customer_unique_id").agg(
        customer_city=("customer_city", "first"),
        customer_state=("customer_state", "first"),
        order_count=("order_purchase_timestamp", "count"),
        first_order=("order_purchase_timestamp", "min"),
    ).reset_index()

    # Region
    agg["customer_region"] = agg["customer_state"].map(STATE_REGION).fillna("Other")

    # ── Simulated fields ──────────────────────────────────────────────
    n = len(agg)
    # Age: uniform 18–70, seeded for reproducibility
    ages = np.random.randint(18, 71, size=n)
    agg["customer_age"] = ages
    agg["customer_age_group"] = pd.cut(
        agg["customer_age"],
        bins=[0, 24, 34, 44, 54, 64, 100],
        labels=["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    ).astype(str)
    agg["customer_gender"] = np.random.choice(["M", "F"], size=n, p=[0.48, 0.52])

    # Signup date: 60–730 days before first order (or fixed fallback)
    def random_signup(first_order_ts):
        if pd.isna(first_order_ts):
            return None
        delta = random.randint(60, 730)
        return (first_order_ts - timedelta(days=delta)).date()

    agg["customer_signup_date"] = agg["first_order"].apply(random_signup)

    # Segment: based on order count
    def segment(cnt):
        if cnt == 1:
            return "Occasional"
        if cnt <= 4:
            return "Regular"
        return "Loyal"

    agg["customer_segment"] = agg["order_count"].apply(segment)
    # ─────────────────────────────────────────────────────────────────

    return agg


# ─────────────────────────────────────────────────────────────────────
# STEP 3 – dim_seller
# ─────────────────────────────────────────────────────────────────────
def build_dim_seller(sellers: pd.DataFrame, items: pd.DataFrame,
                     products: pd.DataFrame, translation: pd.DataFrame) -> pd.DataFrame:
    """
    One row per seller_id.
    Simulated fields: seller_join_date.
    Derived fields:   seller_main_category, seller_size_category, seller_tier.
    """
    # Translate category names
    if not translation.empty and "product_category_name" in translation.columns:
        cat_map = translation.set_index("product_category_name")["product_category_name_english"].to_dict()
    else:
        cat_map = {}

    # Join items → products to get category per item
    if not products.empty and "product_category_name" in products.columns:
        prod_cat = products[["product_id", "product_category_name"]].copy()
        prod_cat["product_category_en"] = prod_cat["product_category_name"].map(cat_map).fillna(
            prod_cat["product_category_name"]
        )
        items_cat = items.merge(prod_cat[["product_id", "product_category_en"]], on="product_id", how="left")
    else:
        items_cat = items.copy()
        items_cat["product_category_en"] = "Unknown"

    # Main category per seller = most frequent category sold
    main_cat = (
        items_cat.groupby("seller_id")["product_category_en"]
        .agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "Unknown")
        .reset_index()
        .rename(columns={"product_category_en": "seller_main_category"})
    )

    # Volume stats per seller for tier / size
    seller_stats = items_cat.groupby("seller_id").agg(
        total_items=("order_item_id", "count"),
        total_revenue=("price", "sum"),
    ).reset_index()

    df = sellers.merge(main_cat, on="seller_id", how="left")
    df = df.merge(seller_stats, on="seller_id", how="left")

    df["seller_region"] = df["seller_state"].map(STATE_REGION).fillna("Other")

    # Size category (by item count)
    def size_cat(cnt):
        if pd.isna(cnt) or cnt < 50:
            return "Small"
        if cnt < 500:
            return "Medium"
        return "Large"

    df["seller_size_category"] = df["total_items"].apply(size_cat)

    # Tier (by revenue)
    def tier(rev):
        if pd.isna(rev) or rev < 5000:
            return "Bronze"
        if rev < 50000:
            return "Silver"
        return "Gold"

    df["seller_tier"] = df["total_revenue"].apply(tier)

    # Simulated: join_date = 1–3 years before dataset start (2016-09-01)
    dataset_start = date(2016, 9, 1)
    n = len(df)
    df["seller_join_date"] = [
        dataset_start - timedelta(days=random.randint(30, 1095)) for _ in range(n)
    ]

    return df


# ─────────────────────────────────────────────────────────────────────
# STEP 4 – dim_product
# ─────────────────────────────────────────────────────────────────────
def build_dim_product(products: pd.DataFrame, items: pd.DataFrame,
                      translation: pd.DataFrame) -> pd.DataFrame:
    """
    One row per product_id.
    list_price    = average price across all order items for that product.
    unit_cost     = list_price * 0.60  (assumed 40% gross margin).
    price_band    = derived from list_price.
    is_premium    = list_price >= 500.
    """
    if not translation.empty and "product_category_name" in translation.columns:
        cat_map = translation.set_index("product_category_name")["product_category_name_english"].to_dict()
    else:
        cat_map = {}

    # Average price per product from order items
    avg_price = items.groupby("product_id")["price"].mean().reset_index().rename(
        columns={"price": "list_price"}
    )

    df = products[["product_id", "product_category_name"]].copy()
    df["product_category"] = df["product_category_name"].map(cat_map).fillna(df["product_category_name"])
    df["product_category_group"] = df["product_category"].apply(category_to_group)
    df = df.merge(avg_price, on="product_id", how="left")
    df["list_price"] = df["list_price"].round(2)
    df["unit_cost"] = (df["list_price"] * 0.60).round(2)
    df["price_band"] = df["list_price"].apply(price_band)
    df["is_premium"] = df["list_price"] >= 500

    return df


# ─────────────────────────────────────────────────────────────────────
# STEP 5 – fact_order_item
# ─────────────────────────────────────────────────────────────────────
def build_fact_order_item(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    customers: pd.DataFrame,
    reviews: pd.DataFrame,
    dim_customer_df: pd.DataFrame,
    dim_seller_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
) -> list:
    """
    One row per (order_id, order_item_id).
    revenue      = price + freight_value
    gross_profit = price - unit_cost
    delivery_days = (delivered - purchase).days
    is_on_time   = 1 if delivered <= estimated else 0
    review_score = from order_reviews (first review per order)
    """
    # Parse dates
    for col in ["order_purchase_timestamp", "order_delivered_customer_date",
                "order_estimated_delivery_date"]:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # Best review per order
    if not reviews.empty and "order_id" in reviews.columns:
        rev = reviews.sort_values("review_score", ascending=False).drop_duplicates("order_id")
        rev = rev[["order_id", "review_score"]]
    else:
        rev = pd.DataFrame(columns=["order_id", "review_score"])

    # customer_unique_id → sk_customer lookup
    cust_sk = customers[["customer_id", "customer_unique_id"]].merge(
        dim_customer_df[["customer_unique_id", "sk_customer"]],
        on="customer_unique_id", how="left"
    )[["customer_id", "sk_customer"]]

    # seller_id → sk_seller lookup
    seller_sk = dim_seller_df[["seller_id", "sk_seller"]].copy()

    # product_id → sk_product + unit_cost lookup
    product_sk = dim_product_df[["product_id", "sk_product", "unit_cost"]].copy()

    # Date → sk_date lookup (YYYYMMDD int)
    def date_to_sk(dt):
        if pd.isna(dt):
            return None
        return int(dt.strftime("%Y%m%d"))

    # Build fact rows
    fact = items.merge(
        orders[["order_id", "customer_id", "order_purchase_timestamp",
                "order_delivered_customer_date", "order_estimated_delivery_date"]],
        on="order_id", how="left"
    )
    fact = fact.merge(rev, on="order_id", how="left")
    fact = fact.merge(cust_sk, on="customer_id", how="left")
    fact = fact.merge(seller_sk, on="seller_id", how="left")
    fact = fact.merge(product_sk, on="product_id", how="left")

    # Derived measures
    fact["revenue"] = (fact["price"] + fact["freight_value"]).round(2)
    fact["gross_profit"] = (fact["price"] - fact["unit_cost"].fillna(0)).round(2)

    purchase_date = fact["order_purchase_timestamp"].dt.date
    delivered_date = fact["order_delivered_customer_date"].dt.date
    estimated_date = fact["order_estimated_delivery_date"].dt.date

    fact["sk_date_purchase"] = purchase_date.apply(
        lambda d: None if pd.isna(d) else int(d.strftime("%Y%m%d"))
    )
    fact["sk_date_delivered"] = fact["order_delivered_customer_date"].apply(date_to_sk)
    fact["sk_date_estimated_delivery"] = fact["order_estimated_delivery_date"].apply(date_to_sk)

    # delivery_days
    def calc_delivery(row):
        if pd.isna(row["order_delivered_customer_date"]) or pd.isna(row["order_purchase_timestamp"]):
            return None
        return (row["order_delivered_customer_date"] - row["order_purchase_timestamp"]).days

    fact["delivery_days"] = fact.apply(calc_delivery, axis=1)

    # is_on_time (1/0/None)
    def calc_on_time(row):
        if pd.isna(row["order_delivered_customer_date"]) or pd.isna(row["order_estimated_delivery_date"]):
            return None
        return 1 if row["order_delivered_customer_date"] <= row["order_estimated_delivery_date"] else 0

    fact["is_on_time"] = fact.apply(calc_on_time, axis=1)

    # Build output tuples – drop rows missing mandatory FKs
    required_cols = ["order_id", "order_item_id", "sk_date_purchase",
                     "sk_customer", "sk_seller", "sk_product"]
    fact_clean = fact.dropna(subset=required_cols)

    rows = []
    for _, r in fact_clean.iterrows():
        rows.append(row_to_none((
            r["order_id"],          # order_id (VARCHAR, natural business key)
            int(r["order_item_id"]),
            int(r["sk_date_purchase"]),
            nan_to_none(r.get("sk_date_delivered")),
            nan_to_none(r.get("sk_date_estimated_delivery")),
            int(r["sk_customer"]),
            int(r["sk_seller"]),
            int(r["sk_product"]),
            float(r["price"]),
            float(r["freight_value"]),
            float(r["revenue"]),
            nan_to_none(r.get("gross_profit")),
            nan_to_none(r.get("unit_cost")),
            nan_to_none(r.get("delivery_days")),
            nan_to_none(r.get("is_on_time")),
            nan_to_none(r.get("review_score")),
        )))

    return rows


# ─────────────────────────────────────────────────────────────────────
# STEP 6 – fact_daily_seller_category
# ─────────────────────────────────────────────────────────────────────
def build_fact_daily_seller_category(
    fact_rows: list,
    dim_seller_df: pd.DataFrame,
    dim_product_df: pd.DataFrame,
) -> tuple:
    """
    Aggregated from fact_order_item rows.
    PK: (sk_date, sk_seller, sk_product_category)
    sk_product_category is a sequential integer ID assigned per unique category string.
    Returns (rows, category_map) where category_map is {category_str: sk_product_category}.
    """
    if not fact_rows:
        return [], {}

    cols = [
        "order_id", "order_item_id", "sk_date_purchase", "sk_date_delivered",
        "sk_date_estimated_delivery",
        "sk_customer", "sk_seller", "sk_product",
        "price", "freight_value", "revenue", "gross_profit", "unit_cost",
        "delivery_days", "is_on_time", "review_score",
    ]
    df = pd.DataFrame(fact_rows, columns=cols)

    # Attach product_category via sk_product
    prod_cat = dim_product_df[["sk_product", "product_category"]].copy()
    df = df.merge(prod_cat, on="sk_product", how="left")
    df["product_category"] = df["product_category"].fillna("Unknown")

    # Build deterministic category → integer mapping (sorted alphabetically)
    unique_cats = sorted(df["product_category"].unique())
    category_map = {cat: idx + 1 for idx, cat in enumerate(unique_cats)}
    df["sk_product_category"] = df["product_category"].map(category_map)

    # Use purchase date as the date dimension for aggregation
    grp = df.groupby(["sk_date_purchase", "sk_seller", "sk_product_category"])

    agg = grp.agg(
        orders_count=("order_id", pd.Series.nunique),
        items_count=("order_item_id", "count"),
        revenue_total=("revenue", "sum"),
        freight_total=("freight_value", "sum"),
        gross_profit_total=("gross_profit", "sum"),
        on_time_deliveries=("is_on_time", lambda x: (x == 1).sum()),
        delivered_items=("sk_date_delivered", lambda x: x.notna().sum()),
        review_score_sum=("review_score", "sum"),
        reviews_count=("review_score", "count"),
    ).reset_index()

    rows = []
    for _, r in agg.iterrows():
        rows.append(row_to_none((
            int(r["sk_date_purchase"]),
            int(r["sk_seller"]),
            int(r["sk_product_category"]),
            int(r["orders_count"]),
            int(r["items_count"]),
            round(float(r["revenue_total"]), 2),
            round(float(r["freight_total"]), 2),
            nan_to_none(r.get("gross_profit_total")),
            int(r["on_time_deliveries"]),
            int(r["delivered_items"]),
            nan_to_none(int(r["review_score_sum"]) if not pd.isna(r["review_score_sum"]) else None),
            int(r["reviews_count"]),
        )))

    return rows, category_map


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Olist DW ETL – loading CSVs")
    print("=" * 60)
    dfs = load_csvs()

    customers   = dfs["customers"]
    orders      = dfs["orders"]
    items       = dfs["items"]
    products    = dfs["products"]
    sellers     = dfs["sellers"]
    reviews     = dfs["reviews"]
    translation = dfs["translation"]

    # ── Connect ───────────────────────────────────────────────────────
    print("\nConnecting to PostgreSQL...")
    try:
        conn = get_conn()
    except psycopg2.OperationalError as e:
        print(f"[ERROR] Cannot connect to DB: {e}")
        sys.exit(1)

    cur = conn.cursor()
    cur.execute(f"SET search_path TO {DB_SCHEMA}")

    # ── Truncate in reverse FK order ──────────────────────────────────
    print("\nTruncating existing data (cascade)...")
    for tbl in ["fact_daily_seller_category", "fact_order_item",
                "dim_product", "dim_seller", "dim_customer", "dim_date"]:
        truncate_table(cur, tbl)
    conn.commit()

    # ── dim_date ──────────────────────────────────────────────────────
    print("\n[1/6] Building dim_date...")
    date_rows = build_dim_date(orders)
    bulk_insert(cur, "dim_date",
                ["sk_date", "date", "day", "month", "month_name",
                 "quarter", "year", "day_of_week", "is_weekend"],
                date_rows)
    conn.commit()

    # ── dim_customer ──────────────────────────────────────────────────
    print("\n[2/6] Building dim_customer...")
    dim_cust = build_dim_customer(customers, orders)
    cust_rows = []
    for _, r in dim_cust.iterrows():
        cust_rows.append(row_to_none((
            r["customer_unique_id"],
            r["customer_city"],
            r["customer_state"],
            r["customer_region"],
            int(r["customer_age"]) if not pd.isna(r["customer_age"]) else None,
            r["customer_age_group"],
            r["customer_gender"],
            r["customer_signup_date"],
            r["customer_segment"],
        )))
    bulk_insert(cur, "dim_customer",
                ["customer_unique_id", "customer_city", "customer_state",
                 "customer_region", "customer_age", "customer_age_group",
                 "customer_gender", "customer_signup_date", "customer_segment"],
                cust_rows)
    conn.commit()

    # Reload dim_customer with auto-generated sk_customer
    cur.execute(f"SELECT sk_customer, customer_unique_id FROM {DB_SCHEMA}.dim_customer")
    rows_sk = cur.fetchall()
    dim_cust_sk = pd.DataFrame(rows_sk, columns=["sk_customer", "customer_unique_id"])

    # ── dim_seller ────────────────────────────────────────────────────
    print("\n[3/6] Building dim_seller...")
    dim_sell = build_dim_seller(sellers, items, products, translation)
    sell_rows = []
    for _, r in dim_sell.iterrows():
        sell_rows.append(row_to_none((
            r["seller_id"],
            r["seller_city"],
            r["seller_state"],
            r["seller_region"],
            r["seller_main_category"],
            r["seller_size_category"],
            r["seller_tier"],
            r["seller_join_date"],
        )))
    bulk_insert(cur, "dim_seller",
                ["seller_id", "seller_city", "seller_state", "seller_region",
                 "seller_main_category", "seller_size_category",
                 "seller_tier", "seller_join_date"],
                sell_rows)
    conn.commit()

    cur.execute(f"SELECT sk_seller, seller_id FROM {DB_SCHEMA}.dim_seller")
    dim_sell_sk = pd.DataFrame(cur.fetchall(), columns=["sk_seller", "seller_id"])

    # ── dim_product ───────────────────────────────────────────────────
    print("\n[4/6] Building dim_product...")
    dim_prod = build_dim_product(products, items, translation)
    prod_rows = []
    for _, r in dim_prod.iterrows():
        prod_rows.append(row_to_none((
            r["product_id"],
            r["product_category"],
            r["product_category_group"],
            nan_to_none(r["list_price"]),
            r["price_band"],
            nan_to_none(r["unit_cost"]),
            bool(r["is_premium"]) if not pd.isna(r["is_premium"]) else None,
        )))
    bulk_insert(cur, "dim_product",
                ["product_id", "product_category", "product_category_group",
                 "list_price", "price_band", "unit_cost", "is_premium"],
                prod_rows)
    conn.commit()

    cur.execute(f"SELECT sk_product, product_id, unit_cost, product_category FROM {DB_SCHEMA}.dim_product")
    dim_prod_sk = pd.DataFrame(cur.fetchall(), columns=["sk_product", "product_id", "unit_cost", "product_category"])
    dim_prod_sk["unit_cost"] = dim_prod_sk["unit_cost"].astype(float)

    # ── fact_order_item ───────────────────────────────────────────────
    print("\n[5/6] Building fact_order_item...")
    fact_rows = build_fact_order_item(
        orders, items, customers, reviews,
        dim_cust_sk, dim_sell_sk, dim_prod_sk,
    )
    bulk_insert(cur, "fact_order_item",
                ["order_id", "order_item_id", "sk_date_purchase", "sk_date_delivered",
                 "sk_date_estimated_delivery",
                 "sk_customer", "sk_seller", "sk_product",
                 "price", "freight_value", "revenue", "gross_profit", "unit_cost",
                 "delivery_days", "is_on_time", "review_score"],
                fact_rows)
    conn.commit()

    # ── fact_daily_seller_category ────────────────────────────────────
    print("\n[6/6] Building fact_daily_seller_category...")
    daily_rows, category_map = build_fact_daily_seller_category(fact_rows, dim_sell_sk, dim_prod_sk)
    print(f"  [INFO] sk_product_category map: {len(category_map)} unique categories")
    bulk_insert(cur, "fact_daily_seller_category",
                ["sk_date", "sk_seller", "sk_product_category",
                 "orders_count", "items_count", "revenue_total", "freight_total",
                 "gross_profit_total", "on_time_deliveries", "delivered_items",
                 "review_score_sum", "reviews_count"],
                daily_rows)
    conn.commit()

    # ── Summary ───────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("ETL complete. Row counts:")
    for tbl in ["dim_date", "dim_customer", "dim_seller", "dim_product",
                "fact_order_item", "fact_daily_seller_category"]:
        cur.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{tbl}")
        cnt = cur.fetchone()[0]
        print(f"  {tbl:<35} {cnt:>10,}")

    cur.close()
    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    main()
