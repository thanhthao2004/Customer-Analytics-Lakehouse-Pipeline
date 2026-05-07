# GitHub Secrets — Complete Setup Guide

Every secret listed here must be added at:  
**GitHub → Your Repo → Settings → Secrets and variables → Actions → New repository secret**

---

## 1. Cloudflare R2 Storage (Recommended — 10 GB free, zero egress)

| Secret Name | Value | Where to get it |
|---|---|---|
| `R2_ACCESS_KEY_ID` | R2 API token Access Key ID | Cloudflare Dashboard → R2 → Manage R2 API Tokens → Create Token (Object Read & Write) |
| `R2_SECRET_ACCESS_KEY` | R2 API token Secret | Same token creation page — copy immediately, shown once |
| `S3_BUCKET` | `customer-analytics-raw` | The bucket name you created in Cloudflare R2 |
| `S3_ENDPOINT_URL` | `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` | Cloudflare Dashboard → R2 → Bucket → Settings — your Account ID appears in the endpoint |
| `AWS_REGION` | `auto` | Hardcode to `auto` for R2 (R2 ignores region) |

> **Alternative — AWS S3 free tier** (5 GB, 20k GET, 2k PUT/month):  
> Replace `R2_ACCESS_KEY_ID` → `AWS_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` → `AWS_SECRET_ACCESS_KEY`, leave `S3_ENDPOINT_URL` blank, set `AWS_REGION` to `ap-southeast-1`.

---

## 2. Railway PostgreSQL

| Secret Name | Value | Where to get it |
|---|---|---|
| `POSTGRES_HOST` | e.g. `monorail.proxy.rlwy.net` | Railway → Project → PostgreSQL service → Connect → Public Network → Host |
| `POSTGRES_PORT` | e.g. `12345` | Same Connect panel → Port |
| `POSTGRES_USER` | e.g. `postgres` | Same panel → User |
| `POSTGRES_PASSWORD` | (Railway auto-generates) | Same panel → Password |
| `POSTGRES_DB` | e.g. `railway` | Same panel → Database |

---

## 3. Gemini NLP

| Secret Name | Value | Where to get it |
|---|---|---|
| `GEMINI_API_KEY` | `AIza...` | [Google AI Studio](https://aistudio.google.com/) → Get API Key → Create API Key |

---

## 4. Railway Deploy Token (FastAPI deploy-api job)

| Secret Name | Value | Where to get it |
|---|---|---|
| `RAILWAY_TOKEN` | `railway_...` | Railway Dashboard → Account Settings → Tokens → Create Token |

---

## 5. Anti-Ban Proxy (Option A — Webshare.io free tier: 10 proxies)

| Secret Name | Value | Where to get it |
|---|---|---|
| `PROXY_HOST` | e.g. `p.webshare.io` | Webshare.io → Proxy → List → Proxy address |
| `PROXY_PORT` | e.g. `80` | Same list |
| `PROXY_USER` | Webshare username | Webshare.io → Proxy → Username |
| `PROXY_PASS` | Webshare password | Webshare.io → Proxy → Password |

> **Alternative — ScraperAPI (Option B, 1000 req/month free)**:  
> Set `SCRAPER_API_KEY` = your ScraperAPI key from [scraperapi.com](https://scraperapi.com/).  
> The scrapers check for this key and use the `http://api.scraperapi.com?api_key=KEY&url=TARGET` endpoint as a fallback.

---

## 6. Monitoring & Notifications

| Secret Name | Value | Where to get it |
|---|---|---|
| `SLACK_WEBHOOK_URL` | `https://hooks.slack.com/services/T.../...` | Slack → Apps → Incoming Webhooks → Add → Copy URL |
| `GIST_TOKEN` | GitHub PAT with `gist` scope | GitHub → Settings → Developer Settings → Personal Access Tokens → New (classic) → check `gist` |
| `STATUS_GIST_ID` | e.g. `abc123def456` | Create a public Gist at gist.github.com → copy the ID from the URL |

---

## 7. FastAPI Cloud URL (for GitHub Actions + frontend)

| Secret Name | Value | Where to get it |
|---|---|---|
| `GH_TOKEN` | GitHub PAT with `repo` + `actions:write` | GitHub → Settings → Developer Settings → PAT → New (classic) → check `repo`, `workflow` |
| `GH_REPO` | `your-username/Customer-Analytics-Lakehouse-Pipeline` | Your repo's full name |

---

## Quick Summary Table

```
R2_ACCESS_KEY_ID          ← Cloudflare R2 API token
R2_SECRET_ACCESS_KEY      ← Cloudflare R2 API secret
S3_BUCKET                 ← "customer-analytics-raw"
S3_ENDPOINT_URL           ← https://<ACCOUNT_ID>.r2.cloudflarestorage.com
AWS_REGION                ← "auto"
POSTGRES_HOST             ← Railway PostgreSQL host
POSTGRES_PORT             ← Railway PostgreSQL port
POSTGRES_USER             ← Railway PostgreSQL user
POSTGRES_PASSWORD         ← Railway PostgreSQL password
POSTGRES_DB               ← Railway PostgreSQL database name
GEMINI_API_KEY            ← Google AI Studio API key
RAILWAY_TOKEN             ← Railway deploy token
PROXY_HOST                ← Webshare.io proxy host (optional)
PROXY_PORT                ← Webshare.io proxy port (optional)
PROXY_USER                ← Webshare.io username (optional)
PROXY_PASS                ← Webshare.io password (optional)
SCRAPER_API_KEY           ← ScraperAPI key (optional, alternative to proxy)
SLACK_WEBHOOK_URL         ← Slack incoming webhook (optional)
GIST_TOKEN                ← GitHub PAT with gist scope (optional)
STATUS_GIST_ID            ← Gist ID for pipeline status JSON (optional)
GH_TOKEN                  ← GitHub PAT for Actions API access (optional)
GH_REPO                   ← "owner/repo" full name (optional)
```
