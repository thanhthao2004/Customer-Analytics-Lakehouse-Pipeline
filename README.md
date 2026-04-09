# Customer Experience Analytics Platform

End-to-end data pipeline: **Scrape → Parquet → PySpark → PostgreSQL → dbt → Metabase**

```
scrapers/         — Shopee + Lazada review scrapers (requests + BeautifulSoup)
spark/            — PySpark processing pipeline
dbt/              — Transformation layers (staging → warehouse → marts)
init_db/          — PostgreSQL schema initialization SQL
scripts/          — Utilities (fake data generator for local testing)
data/raw/         — Raw Parquet files from scrapers (git-ignored)
data/processed/   — Processed Parquet partitioned by platform+month
logs/             — Application logs
```

---

## Quick Start

### 1. Start Infrastructure

```bash
docker compose up -d
```

PostgreSQL → `localhost:5432`  
Metabase → `http://localhost:3000`

---

### 2. Install Python Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

### 3a. Generate Fake Data (recommended for local testing)

```bash
python scripts/generate_fake_data.py
```

Writes ~1,200 realistic fake reviews to `data/raw/shopee/` and `data/raw/lazada/`.

### 3b. Run Real Scrapers (optional)

Edit the product list in each scraper, then:

```bash
# Shopee
python scrapers/shopee_scraper.py

# Lazada
python scrapers/lazada_scraper.py
```

---

### 4. Run Spark Processing

Downloads the PostgreSQL JDBC driver automatically via Maven.

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  spark/pipeline.py
```

Or via the orchestrator:
```bash
python run_pipeline.py --step spark
```

---

### 5. Run dbt Transformations

```bash
cd dbt
dbt deps --profiles-dir .
dbt run  --profiles-dir .
dbt test --profiles-dir .
```

Or via the orchestrator:
```bash
python run_pipeline.py --step dbt
```

---

### 6. Open Metabase Dashboard

Navigate to `http://localhost:3000` and connect to:

| Field    | Value              |
|----------|--------------------|
| Host     | `postgres`         |
| Port     | `5432`             |
| Database | `customer_analytics`|
| User     | `analytics`         |
| Password | `analytics123`      |

**Suggested Metabase charts from the `metabase` schema:**

| Table | Chart type | Description |
|-------|-----------|-------------|
| `mart_daily_sentiment` | Line chart | Avg sentiment over time per product |
| `mart_daily_sentiment` | Bar chart | Review volume per day |
| `mart_product_summary` | Scorecard | Avg rating, positive % |
| `mart_rating_distribution` | Bar chart | 1–5 star breakdown |

---

### Run Everything End-to-End

```bash
python run_pipeline.py --step all
```

---

## Architecture

```
Shopee / Lazada
      │
      ▼
  [scrapers/]  ──→  data/raw/{platform}/*.parquet
      │
      ▼
  [spark/pipeline.py]
      │  clean + sentiment score
      ▼
  data/processed/reviews/  (partitioned Parquet)
      │
      ▼  JDBC
  PostgreSQL: staging.reviews
      │
      ▼
  [dbt/]
      ├── stg_reviews        (staging view)
      ├── dim_product        (warehouse table)
      ├── fact_reviews       (warehouse table)
      ├── mart_daily_sentiment   (metabase table)
      ├── mart_product_summary   (metabase table)
      └── mart_rating_distribution (metabase table)
      │
      ▼
  [Metabase]  →  Dashboard :3000
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Scraping | Python, requests, BeautifulSoup, fake-useragent, tenacity |
| Storage | Apache Parquet (Snappy compression) |
| Processing | PySpark 3.5 |
| Database | PostgreSQL 15 |
| Transformation | dbt-core 1.7 + dbt-postgres |
| Dashboard | Metabase |
| Infrastructure | Docker Compose |