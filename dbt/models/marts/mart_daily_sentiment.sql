-- models/marts/mart_daily_sentiment.sql
-- Aggregated daily sentiment per product + platform — consumed by Metabase

{{ config(materialized='table', schema='metabase') }}

SELECT
    review_date                                         AS report_date,
    platform,
    dp.product_name,
    COUNT(*)                                            AS total_reviews,
    ROUND(AVG(rating)::NUMERIC, 2)                      AS avg_rating,
    ROUND(AVG(sentiment_score)::NUMERIC, 4)             AS avg_sentiment,
    COUNT(*) FILTER (WHERE sentiment_label = 'positive') AS positive_count,
    COUNT(*) FILTER (WHERE sentiment_label = 'negative') AS negative_count,
    COUNT(*) FILTER (WHERE sentiment_label = 'neutral')  AS neutral_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE sentiment_label = 'positive') / COUNT(*),
        2
    )                                                   AS positive_pct
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_product') }} dp USING (product_key)
GROUP BY review_date, platform, dp.product_name
ORDER BY review_date DESC, total_reviews DESC
