-- models/warehouse/dim_product.sql
-- Product dimension — one row per (item_id, platform)

{{ config(materialized='table', schema='warehouse') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['item_id', 'platform']) }}   AS product_key,
    item_id,
    platform,
    (ARRAY_AGG(brand ORDER BY review_time DESC))[1]                    AS brand,
    (ARRAY_AGG(product_name ORDER BY review_time DESC))[1]             AS product_name,
    (ARRAY_AGG(skin_type ORDER BY review_time DESC))[1]                AS skin_type,
    (ARRAY_AGG(formula ORDER BY review_time DESC))[1]                  AS formula,
    MIN(review_date)                                                   AS first_seen_date,
    MAX(review_date)                                                   AS last_seen_date
FROM {{ ref('stg_reviews') }}
WHERE item_id IS NOT NULL
GROUP BY item_id, platform
