# Crawl Targets & Storage Estimates

## Target Brands (10 brands, both platforms)

| Brand | Category Focus | Est. Products/Platform | Reviews/Product | Daily Reviews/Brand |
|---|---|---|---|---|
| CeraVe | Moisturiser, Cleanser | 10 | 50 | 500 |
| La Roche-Posay | Sunscreen, Serum | 10 | 50 | 500 |
| Innisfree | Sheet Mask, Toner | 10 | 50 | 500 |
| The Ordinary | Serum, AHA/BHA | 10 | 50 | 500 |
| Bioderma | Micellar Water | 10 | 50 | 500 |
| Hada Labo | Lotion, Gel | 10 | 50 | 500 |
| Klairs | Vitamin C, Toner | 10 | 50 | 500 |
| Anessa | Sunscreen | 10 | 50 | 500 |
| Senka | Cleanser, Mask | 10 | 50 | 500 |
| Neutrogena | Sunscreen, Retinol | 10 | 50 | 500 |

---

## Daily Volume per Run

| Platform | Brands | Products/Brand | Reviews/Product | Total Reviews/Day |
|---|---|---|---|---|
| Shopee VN | 10 | 10 | 50 | **5,000** |
| Lazada VN | 10 | 10 | 50 | **5,000** |
| **Combined** | | | | **10,000/day** |

---

## Monthly Accumulation (after 30 daily runs)

| Metric | Shopee | Lazada | Total |
|---|---|---|---|
| Raw reviews scraped | 150,000 | 150,000 | 300,000 |
| After deduplication (~10% dups) | ~135,000 | ~135,000 | ~270,000 |
| Unique products tracked | ~300 | ~300 | ~600 |
| Brands covered | 10 | 10 | 10 |

---

## Storage Estimates

### Cloudflare R2 (raw Parquet)

| Item | Calculation | Size |
|---|---|---|
| Avg bytes per review (Parquet compressed) | ~200 bytes | — |
| Daily Parquet per platform | 5,000 × 200 B | ~1 MB/day |
| Monthly Parquet per platform | 30 × 1 MB | ~30 MB/month |
| Both platforms combined | 30 MB × 2 | **~60 MB/month** |
| Annual accumulation | 60 MB × 12 | **~720 MB/year** |
| R2 free tier | 10 GB | ✅ 72× headroom |

### Railway PostgreSQL (processed data)

| Table | Rows (monthly) | Avg row size | Size |
|---|---|---|---|
| `staging.reviews` | 270,000 | ~500 bytes | ~135 MB |
| `mart.product_sentiment_summary` | ~600 rows | ~200 bytes | ~0.1 MB |
| `mart.brand_performance` | ~10 rows/month | ~200 bytes | negligible |
| `mart.monthly_trends` | ~120 rows | ~200 bytes | negligible |
| PostgreSQL overhead (indexes, WAL) | — | ~50% of data | ~70 MB |
| **Total after 1 month** | | | **~205 MB** |
| **Total after 2.5 months** | | | **~500 MB** |
| Railway free tier | 500 MB | ✅ 2.5 months at full rate |

> **Note:** After 2.5 months, you'll need to either:
> - Archive old `staging.reviews` rows to R2 (export as Parquet, delete from PG)
> - Upgrade Railway ($5/month for 1 GB plan)
> - Or keep only the last 30 days in `staging.reviews` (dbt handles mart tables)

---

## GitHub Actions Minutes Budget

| Run type | Duration | Frequency | Monthly minutes |
|---|---|---|---|
| scrape-shopee + scrape-lazada | 10 min each | Daily | 600 min |
| spark-processing | 18 min | Daily | 540 min |
| dbt-transform | 3 min | Daily | 90 min |
| lint-and-test | 5 min | Daily + PRs | ~150 min + PRs |
| notify | 1 min | Daily | 30 min |
| **Total (daily pipeline)** | **~42 min** | **30×/month** | **~1,260 min** |
| GHA free tier (private repo) | — | — | 2,000 min ✅ |
| GHA free tier (public repo) | — | — | Unlimited ✅ |

> The 1,260 min/month estimate gives **740 min buffer** for re-runs and debugging on private repos.

---

## Verdict: All Free Tiers Sufficient

| Service | Usage | Free Limit | Status |
|---|---|---|---|
| Cloudflare R2 | 60 MB/month | 10 GB | ✅ 166× headroom |
| Railway PostgreSQL | 500 MB over 2.5 months | 500 MB | ✅ with archival plan |
| GitHub Actions | 1,260 min/month | 2,000 min (private) | ✅ 740 min buffer |
| Gemini API | ~196 calls/day × 30 = 5,880/month | 1,500/day free | ✅ |
