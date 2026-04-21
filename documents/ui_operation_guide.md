# Pipeline Control Center — UI Operation Guide

> This document explains **exactly what you can do through the browser UI** vs. what still requires terminal commands — and how to operate every feature of the dashboard.

---

## UI Coverage Audit

| Operation | Terminal (CLI) | Control Center UI |
|-----------|---------------|-------------------|
| Check all service health | `docker compose ps` | ✅ **Service Health Bar** — live auto-refresh |
| View total reviews in Postgres | `psql -c "SELECT COUNT(*)"` | ✅ **Data Health Metrics** card |
| View sentiment breakdown | SQL query | ✅ **Data Health Metrics** cards |
| View MinIO file count | `mc ls` | ✅ **MinIO Files** metric card |
| View Shopee / Lazada split | SQL query | ✅ **Platform Breakdown** section |
| Upload Parquet to MinIO | `python3 upload_to_minio.py` | ✅ **Upload to MinIO** button |
| Run PySpark job | `spark-submit ...` | ✅ **Run Spark** button |
| Run dbt transformations | `dbt run --profiles-dir .` | ✅ **Run dbt** button |
| Run full pipeline | `run_pipeline.py --step all` | ✅ **Run Full Pipeline** button |
| See pipeline step progress | — | ✅ **Pipeline Flow Bar** (Upload → Spark → dbt → Done) |
| Watch live logs | `docker compose logs -f` | ✅ **Live Log Viewer** with color-coded streaming |
| Check Airflow DAG status | `airflow dags list-runs` | ✅ **DAG Status badge** auto-refreshes |
| Manually trigger Airflow DAG | `airflow dags trigger ...` | ✅ **Trigger DAG Manually** button |
| Stream Spark logs live | `journalctl -f` | ✅ **Stream ⚡ button** in Log Viewer |
| Stream dbt logs live | `journalctl -f` | ✅ **Stream ⚙ button** in Log Viewer |
| **Run Scrapers (Mac-side)** | `python3 shopee_scraper.py` | ❌ **NOT in UI** — must run on Mac terminal |
| **Docker management** | `docker compose up/down` | ❌ **NOT in UI** — must SSH to Ubuntu VM |
| **Git sync / pull** | `git pull origin main` | ❌ **NOT in UI** — must use terminal |
| **PostgreSQL schema debug** | `psql -c "\dt staging.*"` | ❌ **NOT in UI** — use Metabase or terminal |
| **Run tests / linting** | `pytest` / `flake8` | ❌ **NOT in UI** — Mac terminal or CI/CD |

> The scrapers run on your Mac (not the Ubuntu VM Docker), so they cannot be triggered from the server-side FastAPI. This is by design — they require a real headless Chrome browser.

---

## How to Open the Control Center

### Option A — Open HTML file directly (simplest)
On your Mac, open the file in your browser:
```bash
open ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline/frontend/index.html
```

### Option B — Serve via Python (recommended, avoids CORS issues)
```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline/frontend
python3 -m http.server 5500
```
Then open: **`http://localhost:5500`**

---

## UI Feature Walkthrough

### 1. Service Health Bar
The top row of cards auto-updates every 30 seconds. Each card shows **green** (ok) or **red** (offline) for:
- `Control API` (FastAPI at `:8000`)
- `Airflow` (Airflow webserver at `:8080`)
- `MinIO` (S3 object store at `:9001`)
- `PostgreSQL` (DB at `:5432`)
- `Metabase` (link opens `:3000` in new tab)

> If all dots are red — verify the Ubuntu VM is running and `docker compose up -d` was executed.

### 2. Data Health Metrics
Five KPI cards refreshed from the FastAPI backend:
- **Total Reviews (Staging)** — row count from `staging.reviews` in PostgreSQL
- **Positive / Negative Sentiment** — count breakdown of `sentiment_label` column
- **MinIO Files** — total Parquet + Delta files across all buckets
- **Last Ingest** — timestamp of the most recent record loaded into staging

### 3. Pipeline Controls
Four action buttons mapped to backend API calls:

| Button | API Called | What it runs |
|--------|-----------|--------------|
| Upload to MinIO | `POST /pipeline/upload` | `python3 src/ingestion/upload_to_minio.py` |
| Run Spark | `POST /pipeline/spark` | `spark-submit src/processing/spark_pipeline.py` |
| Run dbt | `POST /pipeline/dbt` | `dbt deps && dbt run && dbt test` |
| Run Full Pipeline | `POST /pipeline/run-all` | All three above in sequence |

The **Pipeline Flow Bar** (`Upload → Spark → dbt → Done`) lights up step-by-step:
- 🔵 Blue = currently running
- 🟢 Green = completed successfully
- 🔴 Red = failed (log viewer shows the error)

### 4. Live Log Viewer
- Logs from any pipeline button action stream directly into the log box
- Color coding: Blue = INFO, Yellow = WARNING, Red = ERROR, Green = SUCCESS
- Click **⚡** to stream a new Spark job live (Server-Sent Events)
- Click **⚙** to stream dbt live
- Click the trash icon to clear the log box

### 5. Airflow DAG Status Panel
- Shows the latest DAG run status: `success` / `running` / `failed`
- Click ↺ Refresh to manually poll the latest status
- Click **Trigger DAG Manually** to bypass the S3 sensor and force an immediate run
- Auto-refreshes every 30 seconds

### 6. Platform Breakdown
- Bottom section shows Shopee vs. Lazada review counts from PostgreSQL
- Data sourced from `platform_breakdown` query in `/postgres/metrics`

---

## Changing the API Target
By default, the dashboard points to `http://10.211.71.4:8000`.
If your VM IP changes, update the **API input box** in the top-right header — the dashboard will immediately reconnect.

---

## What Still Requires Terminal

These operations are intentionally **outside the UI** because they run on your Mac or require privileged VM access:

### Run Scrapers (Mac terminal)
```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
source .venv/bin/activate
python3 src/ingestion/scrapers/shopee_scraper.py
```
After scraping completes, use the **Upload to MinIO** button in the UI.

### Manage Docker Infrastructure (Ubuntu VM SSH)
```bash
ssh savannah@10.211.71.4
cd ~/Customer-Analytics-Lakehouse-Pipeline

# Pull latest code
git pull origin main

# Rebuild after Dockerfile changes
docker compose up --build -d

# View container logs
docker compose logs -f airflow-webserver
```

### Run Tests (Mac terminal)
```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
source .venv/bin/activate
pytest tests/ -v
flake8 src/ tests/ --max-line-length=127 --exit-zero
```
