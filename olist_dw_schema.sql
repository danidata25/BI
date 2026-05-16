-- =====================================================================
-- Olist Data Warehouse - Star Schema DDL (PostgreSQL)
-- Run in pgAdmin against your project database, then use
--   Right-click on schema olist_dw -> "ERD For Schema"
-- to generate the entity-relationship diagram.
-- =====================================================================

DROP SCHEMA IF EXISTS olist_dw CASCADE;
CREATE SCHEMA olist_dw;
SET search_path TO olist_dw;

-- ---------------------------------------------------------------------
-- Dimension: dim_date
-- ---------------------------------------------------------------------
CREATE TABLE dim_date (
    sk_date              INTEGER     PRIMARY KEY,
    date                 DATE        NOT NULL UNIQUE,
    day                  SMALLINT    NOT NULL,
    month                SMALLINT    NOT NULL,
    month_name           VARCHAR(10) NOT NULL,
    quarter              SMALLINT    NOT NULL,
    year                 SMALLINT    NOT NULL,
    day_of_week          VARCHAR(10) NOT NULL,
    is_weekend           BOOLEAN     NOT NULL
);

-- ---------------------------------------------------------------------
-- Dimension: dim_customer
-- ---------------------------------------------------------------------
CREATE TABLE dim_customer (
    sk_customer          SERIAL       PRIMARY KEY,
    customer_unique_id   VARCHAR(32)  NOT NULL UNIQUE,
    customer_city        VARCHAR(50),
    customer_state       VARCHAR(2),
    customer_region      VARCHAR(15),
    customer_age         SMALLINT,
    customer_age_group   VARCHAR(10),
    customer_gender      VARCHAR(1),
    customer_signup_date DATE,
    customer_segment     VARCHAR(10)
);

-- ---------------------------------------------------------------------
-- Dimension: dim_seller
-- ---------------------------------------------------------------------
CREATE TABLE dim_seller (
    sk_seller            SERIAL       PRIMARY KEY,
    seller_id            VARCHAR(32)  NOT NULL UNIQUE,
    seller_city          VARCHAR(50),
    seller_state         VARCHAR(2),
    seller_region        VARCHAR(15),
    seller_main_category VARCHAR(50),
    seller_size_category VARCHAR(10),
    seller_tier          VARCHAR(10),
    seller_join_date     DATE
);

-- ---------------------------------------------------------------------
-- Dimension: dim_product
-- ---------------------------------------------------------------------
CREATE TABLE dim_product (
    sk_product             SERIAL         PRIMARY KEY,
    product_id             VARCHAR(32)    NOT NULL UNIQUE,
    product_category       VARCHAR(50),
    product_category_group VARCHAR(30),
    list_price             NUMERIC(10,2),
    price_band             VARCHAR(10),
    unit_cost              NUMERIC(10,2),
    is_premium             BOOLEAN
);

-- ---------------------------------------------------------------------
-- Fact: fact_order_item  (high granularity - one row per order line)
-- ---------------------------------------------------------------------
CREATE TABLE fact_order_item (
    sk_order                    VARCHAR(32)   NOT NULL,
    order_item_id               SMALLINT      NOT NULL,
    sk_date_purchase            INTEGER       NOT NULL REFERENCES dim_date(sk_date),
    sk_date_delivered           INTEGER       REFERENCES dim_date(sk_date),
    sk_date_estimated_delivery  INTEGER       REFERENCES dim_date(sk_date),
    sk_customer                 INTEGER       NOT NULL REFERENCES dim_customer(sk_customer),
    sk_seller                   INTEGER       NOT NULL REFERENCES dim_seller(sk_seller),
    sk_product                  INTEGER       NOT NULL REFERENCES dim_product(sk_product),
    price                       NUMERIC(10,2) NOT NULL,
    freight_value               NUMERIC(10,2) NOT NULL,
    revenue                     NUMERIC(10,2) NOT NULL,
    gross_profit                NUMERIC(10,2),
    unit_cost                   NUMERIC(10,2),
    delivery_days               SMALLINT,
    is_on_time                  SMALLINT,
    review_score                SMALLINT,
    PRIMARY KEY (sk_order, order_item_id)
);

CREATE INDEX ix_foi_date_purchase  ON fact_order_item (sk_date_purchase);
CREATE INDEX ix_foi_date_delivered ON fact_order_item (sk_date_delivered);
CREATE INDEX ix_foi_customer       ON fact_order_item (sk_customer);
CREATE INDEX ix_foi_seller         ON fact_order_item (sk_seller);
CREATE INDEX ix_foi_product        ON fact_order_item (sk_product);

-- ---------------------------------------------------------------------
-- Fact: fact_daily_seller_category  (low granularity summary)
-- ---------------------------------------------------------------------
CREATE TABLE fact_daily_seller_category (
    sk_date              INTEGER       NOT NULL REFERENCES dim_date(sk_date),
    sk_seller            INTEGER       NOT NULL REFERENCES dim_seller(sk_seller),
    sk_product_category  INTEGER       NOT NULL,
    orders_count         INTEGER,
    items_count          INTEGER,
    revenue_total        NUMERIC(12,2),
    freight_total        NUMERIC(12,2),
    gross_profit_total   NUMERIC(12,2),
    on_time_deliveries   INTEGER,
    delivered_items      INTEGER,
    review_score_sum     INTEGER,
    reviews_count        INTEGER,
    PRIMARY KEY (sk_date, sk_seller, sk_product_category)
);

CREATE INDEX ix_fdsc_date     ON fact_daily_seller_category (sk_date);
CREATE INDEX ix_fdsc_seller   ON fact_daily_seller_category (sk_seller);
CREATE INDEX ix_fdsc_prodcat  ON fact_daily_seller_category (sk_product_category);
