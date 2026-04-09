-- models/marts/mart_rating_distribution.sql
-- Rating distribution per product and platform

{{ config(materialized='table', schema='metabase') }}

SELECT
    dp.product_name,
    fr.platform,
    fr.rating,
    COUNT(*)                                                AS review_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
        PARTITION BY dp.product_name, fr.platform
    ), 2)                                                   AS pct_of_total
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_product') }} dp USING (product_key)
GROUP BY dp.product_name, fr.platform, fr.rating
ORDER BY dp.product_name, fr.platform, fr.rating DESC
