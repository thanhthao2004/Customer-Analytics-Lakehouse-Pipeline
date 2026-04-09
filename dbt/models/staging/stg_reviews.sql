-- models/staging/stg_reviews.sql
-- Lightweight view over the raw staging table; casts types and filters garbage data.

{{ config(materialized='view', schema='staging') }}

SELECT
    platform,
    brand,
    item_id::BIGINT                         AS item_id,
    TRIM(product_name)                      AS product_name,
    skin_type,
    total_like::INT                         AS total_like,
    formula,
    time_delivery,
    price,
    flash_sale,
    rating::SMALLINT                        AS rating,
    TRIM(comment)                           AS comment,
    TRIM(reviewer_name)                     AS reviewer_name,
    review_time::TIMESTAMP                  AS review_time,
    sentiment_score::FLOAT                  AS sentiment_score,
    sentiment_label                         AS sentiment_label,
    review_date::DATE                       AS review_date,
    year_month                              AS year_month
FROM staging.reviews
WHERE
    comment IS NOT NULL
    AND LENGTH(TRIM(comment)) > 3
    AND rating BETWEEN 1 AND 5
