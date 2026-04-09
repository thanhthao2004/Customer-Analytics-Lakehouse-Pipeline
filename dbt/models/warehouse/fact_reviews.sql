-- models/warehouse/fact_reviews.sql
-- Core fact table — one row per review

{{ config(materialized='table', schema='warehouse') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),
products AS (
    SELECT product_key, item_id, platform
    FROM {{ ref('dim_product') }}
)
SELECT
    -- surrogate key for the fact
    {{ dbt_utils.generate_surrogate_key(['base.platform', 'base.item_id', 'base.reviewer_name', 'base.review_time']) }} AS review_key,
    p.product_key,
    base.platform,
    base.item_id,
    base.rating,
    base.sentiment_score,
    base.sentiment_label,
    base.total_like,
    base.time_delivery,
    base.price,
    base.flash_sale,
    base.review_time,
    base.review_date,
    base.year_month,
    base.reviewer_name,
    base.comment
FROM base
LEFT JOIN products p
    ON base.item_id = p.item_id
    AND base.platform = p.platform
