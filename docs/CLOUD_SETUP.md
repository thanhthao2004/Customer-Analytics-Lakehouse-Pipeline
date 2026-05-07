# Cloud Setup Guide — First-Time Setup

> **Target stack:** Cloudflare R2 + Railway (PostgreSQL + FastAPI + Metabase) + GitHub Actions  
> **Cost:** $0/month on free tiers (stay within limits below)

---

## Step 1 — Cloudflare R2 Storage

1. Sign up at [cloudflare.com](https://cloudflare.com) (free plan).
2. Go to **R2 Object Storage** in the left sidebar → **Create bucket**.
3. Name it `customer-analytics-raw`, region `Automatic`.
4. Go to **Manage R2 API Tokens** → **Create API Token**.
   - Permissions: **Object Read & Write**
   - Scope: **Specific bucket → customer-analytics-raw**
5. Copy the **Access Key ID** and **Secret Access Key** — save immediately.
6. Note your **Account ID** from the R2 overview page (e.g. `abc123...`).
7. Your S3 endpoint: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`

**Free tier limits:** 10 GB storage, 10M Class-B ops/month, 1M Class-A ops/month, **zero egress**.

---

## Step 2 — Railway PostgreSQL

1. Sign up at [railway.app](https://railway.app) (GitHub login recommended).
2. **New Project** → **Provision PostgreSQL**.
3. Wait ~30 seconds for provisioning.
4. Click the PostgreSQL service → **Connect** tab → **Public Network**.
5. Copy: **Host**, **Port**, **User**, **Password**, **Database**.
6. Run the database bootstrap SQL **once**:

```bash
# Install psql if needed: brew install libpq
psql "postgres://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=require" \
  -f infrastructure/init_db/01_init.sql
```

Or use the Railway web terminal: click the PostgreSQL service → **Shell** tab and paste the SQL.

**Free tier limits:** 500 MB storage, shared CPU.

---

## Step 3 — GitHub Secrets

Add every secret from `docs/GITHUB_SECRETS.md` to:  
**GitHub → Repo → Settings → Secrets and variables → Actions**

Minimum required to run the pipeline end-to-end:
```
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, S3_BUCKET, S3_ENDPOINT_URL, AWS_REGION
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
GEMINI_API_KEY
```

---

## Step 4 — Deploy FastAPI to Railway

1. In Railway: **New Project** → **Deploy from GitHub repo** → select this repo.
2. Railway auto-detects `railway.json` and uses `infrastructure/Dockerfile.api`.
3. Add environment variables in Railway → FastAPI service → **Variables**:
   ```
   S3_BUCKET, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
   GH_TOKEN, GH_REPO
   ```
4. Railway assigns a public URL like `https://your-service.up.railway.app`.
5. Update `frontend/index.html` → replace `your-fastapi-service.up.railway.app` with the real URL.

---

## Step 5 — Deploy Metabase to Railway

1. In Railway: **New Project** → **Docker Image** → `metabase/metabase:latest`.
2. Add environment variables:
   ```
   MB_DB_TYPE=postgres
   MB_DB_HOST=<Railway PostgreSQL host — can be the SAME DB or a separate one>
   MB_DB_PORT=<port>
   MB_DB_USER=<user>
   MB_DB_PASS=<password>
   MB_DB_DBNAME=metabase   # Create a separate DB or use a different schema
   ```
3. Railway auto-assigns a URL. Open it to complete Metabase first-run setup.
4. Connect Metabase to your analytics PostgreSQL (different credentials — the `customer_analytics` DB).

---

## Step 6 — Add RAILWAY_TOKEN for CI Deploy

1. Railway Dashboard → **Account Settings** → **Tokens** → **Create token**.
2. Add as GitHub Secret `RAILWAY_TOKEN`.
3. The `deploy-api` job in the workflow will now auto-deploy on every push to `main`.

---

## Step 7 — First Manual Pipeline Run

Trigger the workflow manually from GitHub:

1. Go to **Actions** → **Lakehouse Pipeline** → **Run workflow**.
2. Select branch `main`, set `run_full_pipeline` = `true`.
3. Watch the jobs run in parallel:
   - `lint-and-test` + `scrape-shopee` + `scrape-lazada` run simultaneously.
   - `spark-processing` starts when both scrapers finish.
   - `dbt-transform` starts when Spark finishes.
   - `deploy-api` starts on push to main.
   - `notify` always runs last.

---

## Step 8 — Anti-Ban Proxy Setup (Optional but recommended)

### Option A — Webshare.io (10 free proxies)
1. Sign up at [webshare.io](https://webshare.io) → free plan → **Proxy List**.
2. Copy any proxy's **Host**, **Port**, **Username**, **Password**.
3. Add as GitHub Secrets: `PROXY_HOST`, `PROXY_PORT`, `PROXY_USER`, `PROXY_PASS`.

### Option B — ScraperAPI (1,000 free requests/month)
1. Sign up at [scraperapi.com](https://scraperapi.com) → free plan.
2. Copy your **API Key**.
3. Add as GitHub Secret: `SCRAPER_API_KEY`.

### Option C — Self-hosted runner (VPS, most reliable)
```bash
# On a fresh Ubuntu VPS ($4/mo DigitalOcean):
sudo apt-get update && sudo apt-get install -y curl

# Register as a GitHub Actions runner:
mkdir actions-runner && cd actions-runner
curl -o actions-runner-linux-x64.tar.gz -L \
  https://github.com/actions/runner/releases/download/v2.316.1/actions-runner-linux-x64-2.316.1.tar.gz
tar xzf ./actions-runner-linux-x64.tar.gz
./config.sh --url https://github.com/YOUR_ORG/YOUR_REPO --token YOUR_RUNNER_TOKEN
sudo ./svc.sh install && sudo ./svc.sh start
```
Then change `runs-on: ubuntu-latest` → `runs-on: self-hosted` in the scraper jobs.

---

## Monthly Budget Estimate

| Service | Free Tier | Expected usage | Cost |
|---|---|---|---|
| GitHub Actions | 2,000 min/month | ~288 min (schedule only), ~600 min (with spark/dbt on push) | $0 |
| Cloudflare R2 | 10 GB storage | < 1 GB Parquet | $0 |
| Railway PostgreSQL | 500 MB | < 200 MB | $0 |
| Railway FastAPI | $5 credit/month | 1 instance | $0–$1 |
| Metabase | $5 credit/month | 1 instance | $0–$1 |
| Webshare.io | 10 proxies | Scraping only | $0 |
| Gemini API | Free tier | < 1,000 requests | $0 |
| **Total** | | | **$0–$2/month** |

---

## Troubleshooting

### Scraper blocked by Shopee/Lazada
- Enable proxy: add `PROXY_HOST`, `PROXY_PORT`, `PROXY_USER`, `PROXY_PASS` secrets.
- Or switch to self-hosted runner (Option C above).

### Spark job runs out of memory
- Reduce `SPARK_DRIVER_MEMORY` to `3g` in the workflow env.
- Reduce `GEMINI_BATCH_SIZE` in `spark_pipeline.py` to `25`.

### dbt can't connect to PostgreSQL
- Verify `POSTGRES_HOST` is the **public** Railway host (not internal).
- Railway public hosts look like `monorail.proxy.rlwy.net`.
- Add `?sslmode=require` if getting SSL errors (update `dbt/profiles.yml`).

### Railway deploy fails
- Check `railway.json` is at repo root.
- Verify `RAILWAY_TOKEN` secret is set and not expired.
- Use `railway logs` CLI to debug.
