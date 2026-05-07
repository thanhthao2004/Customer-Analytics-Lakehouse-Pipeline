#  Customer Analytics Lakehouse — Commands Reference

> Save this file. Every command you need to operate the full pipeline end-to-end is documented here, grouped by stage and environment.

---

##  Environment Cheatsheet

| Role | Machine | User |
|------|---------|------|
| Scrapers, upload scripts | **Mac Host** (your laptop) | `tranthithanhthao` |
| Docker, Spark, Airflow, dbt | **Ubuntu VM** (UTM) | `savannah` |
| VM IP Address | `10.211.71.4` | SSH Port `22` |

---

## 1.  SSH Into Ubuntu VM (from Mac)

```bash
# Basic SSH
ssh savannah@10.211.71.4

# SSH with project folder auto-navigation
ssh savannah@10.211.71.4 "cd ~/Customer-Analytics-Lakehouse-Pipeline && bash"
```

---

## 2.  Project Setup (First Time Only)

### On Mac Host
```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline

# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install all pipeline dependencies
pip install -r requirements.txt
```

### On Ubuntu VM
```bash
cd ~/Customer-Analytics-Lakehouse-Pipeline

# Install Docker (if not already installed)
sudo apt-get update && sudo apt-get install -y docker.io docker-compose

# Allow Docker without sudo (logout/login required after)
sudo usermod -aG docker savannah
```

---

## 3.  Docker Infrastructure (Ubuntu VM)

### Start All Services (PostgreSQL + MinIO + Airflow + Metabase)
```bash
# On Ubuntu VM:
cd ~/Customer-Analytics-Lakehouse-Pipeline

docker compose up -d
```

### Force Full Rebuild (after adding new dependencies to Dockerfile.airflow)
```bash
docker compose down
docker compose up --build -d
```

### Check All Container Status
```bash
docker compose ps
```

### View Logs for a Specific Service
```bash
docker compose logs -f airflow-webserver
docker compose logs -f postgres
docker compose logs -f minio
```

### Stop All Services
```bash
docker compose down
```

### Nuclear Reset (wipe all volumes — WARNING: deletes all data)
```bash
docker compose down -v
```

---

## 4.  Run Scrapers (Mac Host)

```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
source .venv/bin/activate

# Run Shopee scraper (resumes from checkpoint automatically)
python3 src/ingestion/scrapers/shopee_scraper.py

# Run Lazada scraper
python3 src/ingestion/scrapers/lazada_scraper.py
```

>  Scrapers save chunked Parquet files to `data/raw/shopee/` and `data/raw/lazada/`.  
> If killed mid-run, just run again — `scraped_urls.json` checkpoint skips completed products.

---

## 5.  Upload Raw Data to MinIO (Mac Host → Ubuntu VM)

```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
source .venv/bin/activate

# Upload all raw Parquet files to MinIO raw-data bucket
python3 src/ingestion/upload_to_minio.py
```

> This triggers the Airflow `S3KeySensor` — Spark will auto-start within 5 minutes after this completes.

---

## 6.  Run Spark Manually (Ubuntu VM)

```bash
# On Ubuntu VM:
cd ~/Customer-Analytics-Lakehouse-Pipeline

spark-submit src/processing/spark_pipeline.py
```

Or via the orchestration CLI:
```bash
python3 src/orchestration/run_pipeline.py --step spark
```

---

## 7.  Run dbt Transformations (Ubuntu VM)

```bash
cd ~/Customer-Analytics-Lakehouse-Pipeline/dbt

# Install dbt packages (first time only)
dbt deps --profiles-dir .

# Run all models (staging → warehouse → marts)
dbt run --profiles-dir .

# Test data quality assertions
dbt test --profiles-dir .

# Full run + test in one command
dbt deps --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .
```

---

## 8.  Airflow UI & DAG Management (Ubuntu VM)

| URL | Credentials |
|-----|-------------|
| `http://10.211.71.4:8080` | `admin` / `admin` |

```bash
# Manually trigger the full DAG (skips S3 sensor)
docker exec analytics_airflow_web airflow dags trigger customer_lakehouse_pipeline

# List all DAGs
docker exec analytics_airflow_web airflow dags list

# Check DAG run status
docker exec analytics_airflow_web airflow dags list-runs -d customer_lakehouse_pipeline
```

---

## 9.  Access Dashboards

| Tool | URL | Credentials |
|------|-----|-------------|
| **Metabase** | `http://10.211.71.4:3000` | Set on first login |
| **MinIO Console** | `http://10.211.71.4:9001` | `admin` / `adminpassword` |
| **Airflow** | `http://10.211.71.4:8080` | `admin` / `admin` |

---

## 10.  Full Pipeline — One-Command Orchestration (Mac Host)

```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
source .venv/bin/activate

# Run everything: scrape → upload → spark → dbt
python3 src/orchestration/run_pipeline.py --step all
```

---

## 11.  Run Tests & Linting (Mac Host)

```bash
cd ~/Documents/GitHub/Customer-Analytics-Lakehouse-Pipeline
source .venv/bin/activate

# Run unit tests
pytest tests/ -v

# Run flake8 linting (syntax errors and undefined names only)
flake8 src/ tests/ --count --select=E9,F63,F7,F82 --show-source --statistics

# Run full linting (warnings allowed)
flake8 src/ tests/ --count --exit-zero --max-line-length=127 --statistics
```

---

## 12.  Useful Debugging Commands

```bash
# Check PostgreSQL tables inside Docker
docker exec -it analytics_postgres psql -U analytics -d customer_analytics -c "\dt staging.*"

# Check MinIO bucket contents
docker exec -it analytics_minio mc ls local/raw-data/

# Tail scraper log
tail -f logs/shopee_scraper.log

# Watch Spark job output live
docker compose logs -f airflow-scheduler | grep spark
```

---

## 13.  Git Workflow

```bash
# On Mac — commit and push latest changes
git add .
git commit -m "feat: [describe your change]"
git push origin main

# On Ubuntu VM — pull latest changes
cd ~/Customer-Analytics-Lakehouse-Pipeline
git pull origin main

# Then rebuild Docker if infrastructure files changed
docker compose up --build -d
```
