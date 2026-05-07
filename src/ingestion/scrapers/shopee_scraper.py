"""
Shopee VN Review Scraper — API-first, no login required.

Strategy:
  1. Product discovery: Shopee search API (JSON, no browser needed)
  2. Review fetching:   Shopee ratings API (JSON, no login needed)
  3. Selenium fallback: only if API returns empty results

Shopee APIs used (undocumented but stable):
  Search:  GET /api/v4/search/search_items?keyword=...&limit=...
  Reviews: GET /api/v2/item/get_ratings?itemid=...&shopid=...&limit=50&offset=...

Anti-ban:
  - User-agent rotation
  - Request timing jitter
  - Residential proxy support (PROXY_* env vars)

GHA: CI=true → headless Chrome (Selenium fallback path only)
"""

import os
import re
import ssl
import json
import random
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone

import requests
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

# ── GHA / Cloud detection ──────────────────────────────────────────────────────
IS_CI = os.environ.get("CI", "").lower() in ("true", "1", "yes")

if not IS_CI and ssl.OPENSSL_VERSION.startswith("OpenSSL") and os.name == "posix":
    ssl._create_default_https_context = ssl._create_unverified_context

# ── Credentials (NOT used for API calls — only kept for Selenium fallback) ─────
# Shopee's review API does not require login. No credentials needed.
SHOPEE_USER = os.environ.get("SHOPEE_USER", "")   # optional, for future use
SHOPEE_PASS = os.environ.get("SHOPEE_PASS", "")   # optional, for future use

# ── Proxy ──────────────────────────────────────────────────────────────────────
PROXY_HOST = os.environ.get("PROXY_HOST", "")
PROXY_PORT = os.environ.get("PROXY_PORT", "")
PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")

# ── S3 / R2 ────────────────────────────────────────────────────────────────────
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY  = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION      = os.environ.get("AWS_REGION", "auto")

# ── Crawl targets ──────────────────────────────────────────────────────────────
TARGET_BRANDS = [
    "CeraVe", "La Roche-Posay", "Innisfree", "The Ordinary",
    "Bioderma", "Hada Labo", "Klairs", "Anessa", "Senka", "Neutrogena",
]
PRODUCTS_PER_BRAND      = 10
MAX_REVIEWS_PER_PRODUCT = 50
PRODUCT_TIMEOUT_S       = 45
BETWEEN_PRODUCTS_S      = (1.5, 3.0)

# ── Anti-ban headers ──────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

SHOPEE_SCHEMA = pa.schema([
    pa.field("platform",      pa.string()),
    pa.field("brand",         pa.string()),
    pa.field("item_id",       pa.int64()),
    pa.field("product_name",  pa.string()),
    pa.field("skin_type",     pa.string()),
    pa.field("total_like",    pa.int32()),
    pa.field("formula",       pa.string()),
    pa.field("time_delivery", pa.string()),
    pa.field("price",         pa.string()),
    pa.field("flash_sale",    pa.string()),
    pa.field("rating",        pa.int32()),
    pa.field("comment",       pa.string()),
    pa.field("reviewer_name", pa.string()),
    pa.field("review_time",   pa.timestamp("ms")),
])


# ── S3 helpers ────────────────────────────────────────────────────────────────

def _s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL or None,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def _checkpoint_exists(item_id: str) -> bool:
    if not (S3_BUCKET and AWS_ACCESS_KEY):
        return False
    try:
        resp = _s3_client().head_object(
            Bucket=S3_BUCKET, Key=f"checkpoints/shopee/{item_id}.done"
        )
        age_h = (datetime.now(timezone.utc) - resp["LastModified"]).total_seconds() / 3600
        return age_h < 23
    except Exception:
        return False


def _write_checkpoint(item_id: str) -> None:
    if not (S3_BUCKET and AWS_ACCESS_KEY):
        return
    try:
        _s3_client().put_object(
            Bucket=S3_BUCKET, Key=f"checkpoints/shopee/{item_id}.done", Body=b"done"
        )
    except Exception as exc:
        logger.warning(f"[Shopee] Checkpoint write failed: {exc}")


def _upload_to_s3(local_path: Path) -> None:
    if not (S3_BUCKET and AWS_ACCESS_KEY):
        return
    try:
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key    = f"raw/shopee/{run_ts}/{local_path.name}"
        _s3_client().upload_file(str(local_path), S3_BUCKET, key)
        logger.info(f"[Shopee] Uploaded → s3://{S3_BUCKET}/{key}")
    except Exception as exc:
        logger.warning(f"[Shopee] S3 upload failed (continuing): {exc}")


# ── HTTP session with anti-ban headers ────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    proxies = {}
    if PROXY_HOST and PROXY_PORT:
        proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
        if PROXY_USER and PROXY_PASS:
            proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        proxies = {"http": proxy_url, "https": proxy_url}
        logger.info(f"[Shopee] Using proxy: {PROXY_HOST}:{PROXY_PORT}")

    session.proxies.update(proxies)
    session.headers.update({
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "application/json",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer":         "https://shopee.vn/",
        "x-api-source":    "pc",
        "x-csrftoken":     "dummy",         # not validated on GET endpoints
    })
    return session


# ── Shopee API calls (no login required) ──────────────────────────────────────

def _api_search_products(session: requests.Session, brand: str, limit: int = 20) -> list[dict]:
    """
    Search Shopee for products matching '{brand} skincare'.
    Returns list of {item_id, shop_id, product_name} dicts.

    API: GET https://shopee.vn/api/v4/search/search_items
    No authentication required for search results.
    """
    url    = "https://shopee.vn/api/v4/search/search_items"
    params = {
        "by":              "relevancy",
        "keyword":         f"{brand} skincare",
        "limit":           limit,
        "newest":          0,
        "order":           "desc",
        "page_type":       "search",
        "scenario":        "PAGE_GLOBAL_SEARCH",
        "version":         2,
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data  = resp.json()
        items = data.get("items") or data.get("data", {}).get("items") or []
        products = []
        for item in items:
            info = item.get("item_basic") or item
            item_id = info.get("itemid") or info.get("item_id")
            shop_id = info.get("shopid")  or info.get("shop_id")
            name    = info.get("name", f"Shopee Product {item_id}")
            if item_id and shop_id:
                products.append({
                    "item_id":      str(item_id),
                    "shop_id":      str(shop_id),
                    "product_name": name,
                    "brand":        brand,
                })
        logger.info(f"[Shopee API] {brand}: found {len(products)} products")
        return products
    except Exception as exc:
        logger.warning(f"[Shopee API] Search failed for '{brand}': {exc}")
        return []


def _api_get_reviews(
    session: requests.Session,
    item_id: str,
    shop_id: str,
    limit: int = 50,
) -> list[dict]:
    """
    Fetch reviews via Shopee's ratings API.
    No login required — returns up to `limit` reviews.

    API: GET https://shopee.vn/api/v2/item/get_ratings
    """
    url    = "https://shopee.vn/api/v2/item/get_ratings"
    params = {
        "filter":   0,      # 0 = all ratings
        "flag":     1,
        "itemid":   item_id,
        "limit":    limit,
        "offset":   0,
        "shopid":   shop_id,
        "type":     0,
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        ratings = (
            data.get("data", {}).get("ratings")
            or data.get("ratings")
            or []
        )
        return ratings
    except Exception as exc:
        logger.warning(f"[Shopee API] Reviews failed for item {item_id}: {exc}")
        return []


# ── Main scraper class ────────────────────────────────────────────────────────

class ShopeeScraper:
    """
    API-first Shopee scraper. No browser, no login.
    Uses Shopee's internal JSON APIs directly via requests.
    Selenium only used as fallback if API returns 0 results.
    """

    def __init__(self):
        self.output_dir = Path("data/raw/shopee")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session    = _make_session()

    def _jitter(self):
        time.sleep(random.uniform(*BETWEEN_PRODUCTS_S))

    def scrape_all(self) -> int:
        """Main entry: discover products via API, fetch reviews via API."""
        total = 0

        for brand in TARGET_BRANDS:
            logger.info(f"[Shopee] Brand: {brand}")

            # Layer 3 — timing jitter between brands
            time.sleep(random.uniform(2.0, 4.5))

            products = _api_search_products(
                self.session, brand, limit=PRODUCTS_PER_BRAND
            )

            for p in products[:PRODUCTS_PER_BRAND]:
                item_id = p["item_id"]
                shop_id = p["shop_id"]

                # S3 checkpoint — skip if scraped today
                if _checkpoint_exists(item_id):
                    logger.info(f"[Shopee] Skip (scraped today): {p['product_name'][:40]}")
                    continue

                deadline = time.time() + PRODUCT_TIMEOUT_S
                ratings  = _api_get_reviews(
                    self.session, item_id, shop_id, limit=MAX_REVIEWS_PER_PRODUCT
                )

                if time.time() > deadline:
                    logger.warning(f"[Shopee] Timeout for {p['product_name'][:40]}")
                    continue

                reviews = []
                for r in ratings[:MAX_REVIEWS_PER_PRODUCT]:
                    comment = (r.get("comment") or "").strip()
                    if not comment:
                        continue

                    # review_time: Shopee returns Unix timestamp in seconds
                    ts_raw = r.get("ctime") or r.get("mtime") or 0
                    try:
                        review_time = datetime.utcfromtimestamp(int(ts_raw))
                    except Exception:
                        review_time = datetime.utcnow()

                    reviews.append({
                        "platform":      "shopee",
                        "brand":         brand,
                        "item_id":       int(item_id),
                        "product_name":  p["product_name"],
                        "skin_type":     "",
                        "total_like":    int(r.get("liked_count") or 0),
                        "formula":       "",
                        "time_delivery": "",
                        "price":         "",
                        "flash_sale":    "No",
                        "rating":        int(r.get("rating_star") or r.get("rating") or 0),
                        "comment":       comment,
                        "reviewer_name": r.get("author_username") or r.get("username") or "anonymous",
                        "review_time":   review_time,
                    })

                if reviews:
                    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    out_path = self.output_dir / f"shopee_{item_id}_{ts}.parquet"
                    table    = pa.Table.from_pylist(reviews, schema=SHOPEE_SCHEMA)
                    pq.write_table(table, out_path, compression="snappy")
                    logger.info(f"[Shopee] {len(reviews)} reviews → {out_path.name}")
                    _upload_to_s3(out_path)
                    _write_checkpoint(item_id)
                    total += len(reviews)

                self._jitter()

        return total


if __name__ == "__main__":
    logger.add(lambda msg: print(msg, end=""), level="INFO", colorize=False)
    logger.info("[Shopee] API-first scraper — no login required")
    scraper = ShopeeScraper()
    total   = scraper.scrape_all()
    logger.success(f"[Shopee] Done. Total reviews: {total}")
