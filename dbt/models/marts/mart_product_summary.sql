-- models/marts/mart_product_summary.sql
-- Single-row-per-product summary card metrics for Metabase

{{ config(materialized='table', schema='metabase') }}

SELECT
    dp.product_name,
    fr.platform,
    COUNT(*)                                                AS total_reviews,
    ROUND(AVG(fr.rating)::NUMERIC, 2)                       AS avg_rating,
    ROUND(AVG(fr.sentiment_score)::NUMERIC, 4)              AS avg_sentiment,
    COUNT(*) FILTER (WHERE fr.sentiment_label = 'positive') AS positive_count,
    COUNT(*) FILTER (WHERE fr.sentiment_label = 'negative') AS negative_count,
    COUNT(*) FILTER (WHERE fr.sentiment_label = 'neutral')  AS neutral_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE fr.sentiment_label = 'positive') / COUNT(*), 2
    )                                                       AS positive_pct,
    MIN(fr.review_date)                                     AS first_review_date,
    MAX(fr.review_date)                                     AS last_review_date,
    SUM(fr.total_like)                                      AS total_helpful_votes
FROM {{ ref('fact_reviews') }} fr
JOIN {{ ref('dim_product') }} dp USING (product_key)
GROUP BY dp.product_name, fr.platform
ORDER BY total_reviews DESC
