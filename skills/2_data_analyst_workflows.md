# Role: Data Analyst (Workflows & Dashboards)

## Core Workflow
As a Data Analyst on this project, the workflow centers around exploiting the robust Star Schema built via `dbt` and loaded into PostgreSQL, and serving those tables directly to non-technical stakeholders via Metabase.

## Data Modeling Strategy (dbt)
Rather than querying a massive, scattered raw data table (`staging.reviews`), we utilize specialized Data Marts. This abstracts the complexity and accelerates dashboard rendering times.

* **staging.reviews:** The raw, cleaned output from Spark. Contains every comment, sentiment label, and URL.
* **dim_product:** Normalizes the product strings into unique integer IDs, extracting `brand`, `skin_type`, and `formula` attributes.
* **fact_reviews:** Contains mutable KPIs: `rating`, `total_like`, `price`, and boolean flags connecting dynamically to the `dim_product`.

## Key Metabase Data Marts Engineered
We utilize `dbt` to generate single-row-per-dimension summary tables ("Marts") designed exclusively for fast Metabase charts:

1. **`mart_daily_sentiment` (Time Series Analysis)**
   * **Query Structure:** Aggregates `avg_sentiment` and total volume by `report_date` and `platform`.
   * **Visualization:** Spline chart overlaying Shopee vs. Lazada brand sentiment drops over time.

2. **`mart_product_summary` (Leaderboards & Scorecards)**
   * **Query Structure:** Aggregates a single row per product containing `total_reviews`, `avg_rating`, and a computed `positive_pct` metric (calculated dynamically via SQL filters).
   * **Visualization:** Bar charts showing the top 10 most loved skincare formulas in Vietnam.

3. **`mart_rating_distribution` (Cohort Analysis)**
   * **Query Structure:** Groups counts by 1-5 star brackets per product.
   * **Visualization:** 100% Stacked Bar chart visually representing the polarization of a product. 

## Technical Depth
By controlling the SQL transformation layer through `dbt`, the Data Analyst guarantees that complex business rules (like classifying sentiments into percentages) are version-controlled in Git, rather than hidden inside fragile, undocumented Metabase GUI metrics.
