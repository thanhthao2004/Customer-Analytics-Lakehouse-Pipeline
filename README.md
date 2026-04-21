# Customer Analytics Lakehouse Pipeline

> **End-to-end Data Engineering platform** that scrapes skincare product reviews from Shopee and Lazada (VN), processes them using PySpark with Delta Lake, applies AI-powered NLP sentiment analysis via Gemini, and serves executive dashboards through Metabase. Fully orchestrated via Apache Airflow with an event-driven S3KeySensor architecture.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        PIPELINE SYSTEM ARCHITECTURE                     │
│                                                                         │
│  [Mac Host]                    [Ubuntu VM — Docker]                     │
│  ─────────                     ──────────────────                       │
│  src/ingestion/                                                         │
│    scrapers/         ──upload──►  MinIO (S3)   ◄──sensor──  Airflow    │
│    upload_to_minio.py            raw-data/                      │       │
│                                       │                         │       │
│                                  PySpark                        │       │
│                                  Δ Delta Lake                   │       │
│                                  Gemini NLP                     │       │
│                                       │                         │       │
│                                  PostgreSQL    ◄──dbt──────────►│       │
│                                  staging.reviews                        │
│                                  mart_*                                 │
│                                       │                                 │
│                                  Metabase  (:3000)                      │
│                                  Control Center UI  (:5500 / :8000)     │
└─────────────────────────────────────────────────────────────────────────┘

Tech Stack:
  Scraping    →  Python · Selenium · undetected_chromedriver
  Data Lake   →  MinIO (S3-compatible) · Delta Lake · Parquet
  Processing  →  Apache Spark 3.5 · Gemini Flash API (NLP)
  Warehouse   →  PostgreSQL 15 · dbt Core
  Orchestration → Apache Airflow 2.8 · S3KeySensor
  Dashboard   →  Metabase
  Control UI  →  FastAPI · Vanilla JS
  CI/CD       →  GitHub Actions · flake8 · pytest
```

---

## Project Structure

```
Customer-Analytics-Lakehouse-Pipeline/
│
├── 📁 src/                          # All application source code
│   ├── 📁 ingestion/                # Extraction Layer (runs on Mac Host)
│   │   ├── 📁 scrapers/
│   │   │   ├── shopee_scraper.py    # Shopee VN review scraper (anti-bot bypass)
│   │   │   ├── lazada_scraper.py    # Lazada VN review scraper
│   │   │   └── config.py           # Shared settings loaded from .env
│   │   └── upload_to_minio.py      # Uploads raw Parquet to MinIO Data Lake
│   │
│   ├── 📁 processing/               # Processing Layer (runs on Ubuntu VM)
│   │   └── spark_pipeline.py       # PySpark: ingest → quality check → NLP → Delta Lake → Postgres
│   │
│   ├── 📁 orchestration/            # Orchestration Layer
│   │   ├── run_pipeline.py         # CLI menu to trigger individual pipeline steps
│   │   └── 📁 dags/
│   │       └── lakehouse_pipeline_dag.py  # Airflow DAG with S3KeySensor
│   │
│   └── 📁 api/                      # Pipeline Control Center Backend
│       └── main.py                  # FastAPI: status, triggers, log streaming, metrics
│
├── 📁 frontend/                     # Pipeline Control Center UI (static HTML)
│   ├── index.html                   # Dashboard entry point
│   ├── style.css                    # Dark glassmorphism design system
│   └── app.js                       # API polling, log streaming, DAG control
│
├── 📁 dbt/                          # Transformation Layer
│   ├── dbt_project.yml
│   ├── profiles.yml                 # PostgreSQL connection (reads from .env)
│   ├── packages.yml
│   └── 📁 models/
│       ├── schema.yml
│       ├── 📁 staging/
│       │   └── stg_reviews.sql
│       ├── 📁 warehouse/
│       │   ├── dim_product.sql
│       │   └── fact_reviews.sql
│       └── 📁 marts/
│           ├── mart_daily_sentiment.sql
│           ├── mart_product_summary.sql
│           └── mart_rating_distribution.sql
│
├── 📁 infrastructure/               # Docker & DB configurations
│   ├── Dockerfile.airflow           # Airflow + Spark + dbt image
│   ├── Dockerfile.api               # FastAPI backend image
│   └── 📁 init_db/
│       └── 01_init.sql              # PostgreSQL schema bootstrap
│
├── 📁 data/                         # Local data directories (git-ignored)
│   ├── 📁 raw/shopee/               # Chunked Parquet files from scraper
│   └── 📁 raw/lazada/
│
├── 📁 documents/                    # Architecture docs & reference
│   ├── pipeline-system.png          # System architecture diagram
│   └── commands_reference.md        # All operational commands
│
├── 📁 skills/                       # AI context files for Claude / ChatGPT
│   ├── 0_master_prompt.txt          # Master AI injection prompt
│   ├── 1_business_analyst_impact.md
│   ├── 2_data_analyst_workflows.md
│   └── 3_data_engineer_architecture.md
│
├── 📁 tests/                        # Test suites
│   ├── test_shopee.py
│   └── test_lazada.py
│
├── 📁 scripts/                      # Utility scripts
│   └── generate_fake_data.py        # 1,200 fake reviews for local dev
│
├── 📁 .github/workflows/
│   └── main_lakehouse.yml           # CI/CD: flake8 + pytest on every push
│
├── docker-compose.yml               # Full infrastructure stack definition
├── requirements.txt                 # All Python dependencies
├── .env                             # Local secrets (never committed)
└── README.md
```

---

## Quick Start

### Prerequisites
- **Mac Host:** Python 3.10+, Chrome browser
- **Ubuntu VM (UTM):** Docker CE, Git, 4+ GB RAM

### Step 1 — Clone on Ubuntu VM

```bash
# SSH into Ubuntu VM
ssh savannah@10.211.71.4

# Clone the repository
git clone https://github.com/thanhthao2004/Customer-Analytics-Lakehouse-Pipeline.git ~/Customer-Analytics-Lakehouse-Pipeline
cd ~/Customer-Analytics-Lakehouse-Pipeline
```

### Step 2 — Start the Infrastructure Stack

```bash
# On Ubuntu VM
docker compose up --build -d

# Verify all containers are running
docker compose ps
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Pipeline Control Center | `http://10.211.71.4:8000` | — |
| Apache Airflow | `http://10.211.71.4:8080` | `admin / adminpassword` |
| MinIO Console | `http://10.211.71.4:9001` | `admin / adminpassword` |
| Metabase | `http://10.211.71.4:3000` | Set on first login |
| PostgreSQL | `10.211.71.4:5432` | `analytics / analytics123` |

### Step 3 — Run Scrapers (Mac Host)

```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Scrape Shopee (auto-resumes from checkpoint)
python3 src/ingestion/scrapers/shopee_scraper.py
```

### Step 4 — Upload & Trigger Pipeline

```bash
# Upload raw Parquet files to MinIO (triggers Airflow S3Sensor automatically)
python3 src/ingestion/upload_to_minio.py

# Or run the full pipeline manually via CLI
python3 src/orchestration/run_pipeline.py --step all
```

### Step 5 — Open the Dashboard UI

Open `frontend/index.html` in your Mac browser. The Control Center will auto-connect to the Ubuntu VM API and refresh every 30 seconds.

---

## Data Schema

### Raw Review Fields (Parquet)
| Field | Type | Description |
|-------|------|-------------|
| `platform` | string | `shopee` or `lazada` |
| `brand` | string | Skincare brand name |
| `item_id` | int64 | Auto-incremented surrogate key |
| `product_name` | string | Full product title |
| `rating` | int32 | 1–5 star rating |
| `comment` | string | Review text |
| `reviewer_name` | string | Reviewer username |
| `review_time` | timestamp | Review submission time |
| `skin_type` | string | Extracted product spec |
| `formula` | string | Product formula type |
| `price` | string | Listed price in ₫ |
| `flash_sale` | string | `Yes` / `No` |
| `time_delivery` | string | Estimated shipping spec |
| `total_like` | int32 | Number of helpful votes |

---

## Development

```bash
# Run tests
pytest tests/ -v

# Lint check
flake8 src/ tests/ --max-line-length=127 --count --exit-zero

# Add fake data for local dev (no scraping needed)
python3 scripts/generate_fake_data.py
```

---

## Architecture Deep Dive

See [`documents/commands_reference.md`](documents/commands_reference.md) for all operational commands.

See [`skills/`](skills/) for AI-ready context documents explaining the business impact, data modeling, and engineering decisions behind this platform.