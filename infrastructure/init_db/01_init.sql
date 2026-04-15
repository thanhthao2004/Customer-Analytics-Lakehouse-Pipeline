-- Schema & tables created before dbt runs
-- Run via: psql -U analytics -d customer_analytics -f init_db/01_init.sql

-- ============================================================
-- Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS metabase;

-- ============================================================
-- Staging — raw ingest from Spark JDBC
-- ============================================================
DROP TABLE IF EXISTS staging.reviews;
CREATE TABLE staging.reviews (
    platform            VARCHAR(20),
    brand               VARCHAR(200),
    item_id             BIGINT,
    product_name        VARCHAR(500),
    skin_type           VARCHAR(200),
    total_like          INT,
    formula             VARCHAR(500),
    time_delivery       VARCHAR(200),
    price               VARCHAR(50),
    flash_sale          VARCHAR(10),
    rating              SMALLINT,
    comment             TEXT,
    reviewer_name       VARCHAR(200),
    review_time         TIMESTAMP,
    sentiment_score     FLOAT,
    sentiment_label     VARCHAR(20),
    review_date         DATE,
    year_month          VARCHAR(7)
);

-- ============================================================
-- Warehouse — built by dbt
-- (dbt will CREATE OR REPLACE these, pre-declaring for safety)
-- ============================================================
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key     SERIAL PRIMARY KEY,
    item_id         BIGINT,
    platform        VARCHAR(20),
    brand           VARCHAR(200),
    product_name    VARCHAR(500),
    skin_type       VARCHAR(200),
    formula         VARCHAR(500),
    UNIQUE (item_id, platform)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_reviews (
    review_key          BIGSERIAL PRIMARY KEY,
    product_key         INT REFERENCES warehouse.dim_product(product_key),
    platform            VARCHAR(20),
    rating              SMALLINT,
    sentiment_score     FLOAT,
    sentiment_label     VARCHAR(20),
    total_like          INT,
    price               VARCHAR(50),
    flash_sale          VARCHAR(10),
    time_delivery       VARCHAR(200),
    review_time         TIMESTAMP,
    review_date         DATE,
    year_month          VARCHAR(7),
    reviewer_name       VARCHAR(200),
    comment             TEXT
);

-- ============================================================
-- Metabase schema — dbt mart layer for dashboards
-- ============================================================
CREATE TABLE IF NOT EXISTS metabase.daily_sentiment (
    report_date         DATE,
    platform            VARCHAR(20),
    product_name        VARCHAR(500),
    total_reviews       INT,
    avg_rating          FLOAT,
    avg_sentiment       FLOAT,
    positive_count      INT,
    negative_count      INT,
    neutral_count       INT
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_staging_platform ON staging.reviews(platform);
CREATE INDEX IF NOT EXISTS idx_staging_item_id ON staging.reviews(item_id);
CREATE INDEX IF NOT EXISTS idx_staging_review_date ON stag