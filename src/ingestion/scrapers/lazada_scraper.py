"""
Lazada VN Review Scraper — API-first, no login required.

Strategy:
  1. Product discovery: Lazada search API (JSON, no browser needed)
  2. Review fetching:   Lazada review API (JSON, no login needed)

Lazada APIs used:
  Search:  GET https://www.lazada.vn/catalog/?q=...&ajax=true
  Reviews: GET https://my.lazada.vn/pdp/review/getReviewList?itemId=...&pageSize=50&pageNo=1

Anti-ban:
  - User-agent rotation
  - Request timing jitter
  - Residential proxy support (PROXY_* env vars)

GHA: CI=true → fully headless (API, no Chrome needed)
Credentials: NOT required. Lazada review API is public.
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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

LAZADA_SCHEMA = pa.schema([
    pa.field("platform",             pa.string()),
    pa.field("item_id",              pa.string()),
    pa.field("product_name",         pa.string()),
    pa.field("rating",               pa.int32()),
    pa.field("comment",              pa.string()),
    pa.field("reviewer_name",        pa.string()),
    pa.field("helpful_count",        pa.int32()),
    pa.field("review_time",          pa.timestamp("ms")),
    pa.field("is_verified_purchase", pa.bool_()),
    pa.field("scraped_at",           pa.timestamp("ms")),
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
            Bucket=S3_BUCKET, Key=f"checkpoints/lazada/{item_id}.done"
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
            Bucket=S3_BUCKET, Key=f"checkpoints/lazada/{item_id}.done", Body=b"done"
        )
    except Exception as exc:
        logger.warning(f"[Lazada] Checkpoint write failed: {exc}")


def _upload_to_s3(local_path: Path) -> None:
    if not (S3_BUCKET and AWS_ACCESS_KEY):
        return
    try:
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key    = f"raw/lazada/{run_ts}/{local_path.name}"
        _s3_client().upload_file(str(local_path), S3_BUCKET, key)
        logger.info(f"[Lazada] Uploaded → s3://{S3_BUCKET}/{key}")
    except Exception as exc:
        logger.warning(f"[Lazada] S3 upload failed (continuing): {exc}")


# ── HTTP session ──────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    proxies = {}
    if PROXY_HOST and PROXY_PORT:
        proxy_url = f"http://{PROXY_HOST}:{PROXY_PORT}"
        if PROXY_USER and PROXY_PASS:
            proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        proxies = {"http": proxy_url, "https": proxy_url}
        logger.info(f"[Lazada] Using proxy: {PROXY_HOST}:{PROXY_PORT}")

    session.proxies.update(proxies)
    session.headers.update({
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer":         "https://www.lazada.vn/",
    })
    return session


# ── Lazada API calls (no login required) ─────────────────────────────────────

def _api_search_products(session: requests.Session, brand: str, limit: int = 20) -> list[dict]:
    """
    Search Lazada catalog via AJAX endpoint.
    Returns list of {item_id, product_name, brand} dicts.

    API: GET https://www.lazada.vn/catalog/?q={brand+skincare}&ajax=true
    No authentication required.
    """
    url    = "https://www.lazada.vn/catalog/"
    params = {
        "q":    f"{brand} skincare",
        "ajax": "true",
        "page": 1,
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data  = resp.json()
        mods  = data.get("mods", {})

        # Lazada AJAX returns listItems under mods.listItems
        items = (
            mods.get("listItems", [])
            or data.get("rgn", {}).get("modules", {}).get("item", {}).get("data", {}).get("rows", [])
        )

        products = []
        for item in items[:limit]:
            item_id = str(
                item.get("itemId")
                or item.get("skuId")
                or item.get("productId")
                or ""
            )
            name = item.get("name") or item.get("productName") or f"Lazada Product {item_id}"
            if item_id:
                products.append({
                    "item_id":      item_id,
                    "product_name": name,
                    "brand":        brand,
                })

        # Fallback: extract itemId from URL patterns in raw HTML/JSON text
        if not products:
            raw_text = resp.text
            ids_found = re.findall(r'"itemId"\s*:\s*"?(\d+)"?', raw_text)
            names_found = re.findall(r'"name"\s*:\s*"([^"]{5,80})"', raw_text)
            for i, iid in enumerate(set(ids_found[:limit])):
                products.append({
                    "item_id":      iid,
                    "product_name": names_found[i] if i < len(names_found) else f"Lazada Product {iid}",
                    "brand":        brand,
                })

        logger.info(f"[Lazada API] {brand}: found {len(products)} products")
        return products

    except Exception as exc:
        logger.warning(f"[Lazada API] Search failed for '{brand}': {exc}")
        return []


def _api_get_reviews(
    session: requests.Session,
    item_id: str,
    page_size: int = 50,
) -> list[dict]:
    """
    Fetch reviews via Lazada's public review API.
    No login required.

    API: GET https://my.lazada.vn/pdp/review/getReviewList
         params: itemId, pageSize, pageNo, filter, sort
    """
    url    = "https://my.lazada.vn/pdp/review/getReviewList"
    params = {
        "itemId":   item_id,
        "pageSize": page_size,
        "pageNo":   1,
        "filter":   0,
        "sort":     0,
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Navigate the response structure
        reviews = (
            data.get("model", {}).get("items")
            or data.get("data", {}).get("reviews")
            or data.get("reviews")
            or []
        )
        return reviews

    except Exception as exc:
        logger.warning(f"[Lazada API] Reviews failed for item {item_id}: {exc}")
        return []


# ── Main scraper class ────────────────────────────────────────────────────────

class LazadaScraper:
    """
    API-first Lazada scraper. No browser, no login.
    Uses Lazada's internal JSON APIs directly via requests.
    """

    def __init__(self):
        self.output_dir = Path("data/raw/lazada")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session    = _make_session()

    def _jitter(self):
        time.sleep(random.uniform(*BETWEEN_PRODUCTS_S))

    def scrape_all(self) -> int:
        total = 0
        now   = datetime.utcnow()

        for brand in TARGET_BRANDS:
            logger.info(f"[Lazada] Brand: {brand}")
            time.sleep(random.uniform(2.0, 4.5))  # jitter between brands

            products = _api_search_products(
                self.session, brand, limit=PRODUCTS_PER_BRAND
            )

            for p in products[:PRODUCTS_PER_BRAND]:
                item_id = p["item_id"]

                if _checkpoint_exists(item_id):
                    logger.info(f"[Lazada] Skip (scraped today): {p['product_name'][:40]}")
                    continue

                deadline = time.time() + PRODUCT_TIMEOUT_S
                raw_reviews = _api_get_reviews(
                    self.session, item_id, page_size=MAX_REVIEWS_PER_PRODUCT
                )

                if time.time() > deadline:
                    logger.warning(f"[Lazada] Timeout for {p['product_name'][:40]}")
                    continue

                reviews = []
                for r in raw_reviews[:MAX_REVIEWS_PER_PRODUCT]:
                    comment = (
                        r.get("reviewContent")
                        or r.get("comment")
                        or r.get("body")
                        or ""
                    ).strip()
                    if not comment:
                        continue

                    rating = int(
                        r.get("rating")
                        or r.get("score")
                        or r.get("ratingScore")
                        or 0
                    )

                    reviewer = (
                        r.get("reviewer", {}).get("name")
                        or r.get("buyerName")
                        or r.get("nickname")
                        or "anonymous"
                    )

                    helpful = int(r.get("likeCount") or r.get("helpfulCount") or 0)
                    verified = bool(r.get("verifiedBuyer") or r.get("reviewSources") == "verified_buyer")

                    # Parse review time
                    ts_raw = (
                        r.get("reviewTime")
                        or r.get("createdAt")
                        or r.get("publishTime")
                        or ""
                    )
                    review_time = now
                    if ts_raw:
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                            try:
                                review_time = datetime.strptime(str(ts_raw)[:19], fmt[:len(str(ts_raw)[:19])])
                                break
                            except Exception:
                                continue

                    reviews.append({
                        "platform":             "lazada",
                        "item_id":              item_id,
                        "product_name":         p["product_name"],
                        "rating":               min(rating, 5),
                        "comment":              comment,
                        "reviewer_name":        reviewer,
                        "helpful_count":        helpful,
                        "review_time":          review_time,
                        "is_verified_purchase": verified,
                        "scraped_at":           now,
                    })

                if reviews:
                    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    out_path = self.output_dir / f"lazada_{item_id}_{ts}.parquet"
                    table    = pa.Table.from_pylist(reviews, schema=LAZADA_SCHEMA)
                    pq.write_table(table, out_path, compression="snappy")
                    logger.info(f"[Lazada] {len(reviews)} reviews → {out_path.name}")
                    _upload_to_s3(out_path)
                    _write_checkpoint(item_id)
                    total += len(reviews)

                self._jitter()

        return total


if __name__ == "__main__":
    logger.add(lambda msg: print(msg, end=""), level="INFO", colorize=False)
    logger.info("[Lazada] API-first scraper — no login required")
    scraper = LazadaScraper()
    total   = scraper.scrape_all()
    logger.success(f"[Lazada] Done. Total reviews: {total}")
