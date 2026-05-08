"""
Lazada VN Scraper — Login & Cookie Support
"""

import os
import re
import json
import random
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

# ── Env Config ──────────────────────────────────────────────────────────────
IS_CI = os.environ.get("CI", "").lower() in ("true", "1", "yes")

LAZADA_COOKIE = os.environ.get("LAZADA_COOKIE", "")
LAZADA_USER   = os.environ.get("LAZADA_USER", "")
LAZADA_PASS   = os.environ.get("LAZADA_PASS", "")

S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY  = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION      = os.environ.get("AWS_REGION", "auto")

TARGET_BRANDS = ["CeraVe", "La Roche-Posay", "Innisfree", "The Ordinary", "Bioderma", "Hada Labo", "Klairs", "Anessa", "Senka", "Neutrogena"]
PRODUCTS_PER_BRAND = 10
MAX_REVIEWS_PER_PRODUCT = 50

LAZADA_SCHEMA = pa.schema([
    pa.field("platform", pa.string()), pa.field("item_id", pa.string()),
    pa.field("product_name", pa.string()), pa.field("rating", pa.int32()),
    pa.field("comment", pa.string()), pa.field("reviewer_name", pa.string()),
    pa.field("helpful_count", pa.int32()), pa.field("review_time", pa.timestamp("ms")),
    pa.field("is_verified_purchase", pa.bool_()), pa.field("scraped_at", pa.timestamp("ms")),
])

# ── S3 helpers ────────────────────────────────────────────────────────────────

def _s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL or None, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION, config=Config(signature_version="s3v4"))

def _checkpoint_exists(item_id):
    if not (S3_BUCKET and AWS_ACCESS_KEY): return False
    try:
        resp = _s3_client().head_object(Bucket=S3_BUCKET, Key=f"checkpoints/lazada/{item_id}.done")
        return (datetime.now(timezone.utc) - resp["LastModified"]).total_seconds() / 3600 < 23
    except: return False

def _write_checkpoint(item_id):
    if not (S3_BUCKET and AWS_ACCESS_KEY): return
    try: _s3_client().put_object(Bucket=S3_BUCKET, Key=f"checkpoints/lazada/{item_id}.done", Body=b"done")
    except Exception as e: logger.warning(f"Checkpoint failed: {e}")

def _upload_to_s3(path):
    if not (S3_BUCKET and AWS_ACCESS_KEY): return
    try:
        key = f"raw/lazada/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}/{path.name}"
        _s3_client().upload_file(str(path), S3_BUCKET, key)
    except Exception as e: logger.warning(f"Upload failed: {e}")

# ── Login Logic ──────────────────────────────────────────────────────────────

def get_lazada_cookies():
    if LAZADA_COOKIE:
        logger.info("Using provided LAZADA_COOKIE.")
        return LAZADA_COOKIE

    if not LAZADA_USER or not LAZADA_PASS: return ""

    logger.info(f"Attempting Lazada login for: {LAZADA_USER}")
    options = uc.ChromeOptions()
    if IS_CI:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
    
    # use_subprocess=True prevents hangs in GHA
    driver = uc.Chrome(options=options, use_subprocess=True)
    try:
        driver.get("https://member.lazada.vn/user/login")
        time.sleep(3)
        driver.find_element(By.CSS_SELECTOR, "input[type='text']").send_keys(LAZADA_USER)
        driver.find_element(By.CSS_SELECTOR, "input[type='password']").send_keys(LAZADA_PASS)
        driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        time.sleep(10)
        
        cookies = driver.get_cookies()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    except Exception as e:
        logger.error(f"Lazada login failed: {e}")
        return ""
    finally:
        driver.quit()

# ── Scraper ──────────────────────────────────────────────────────────────────

class LazadaScraper:
    def __init__(self):
        self.output_dir = Path("data/raw/lazada")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cookie = get_lazada_cookies()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Cookie": self.cookie,
            "Referer": "https://www.lazada.vn/",
            "x-requested-with": "XMLHttpRequest"
        })

    def search_products(self, brand):
        url = "https://www.lazada.vn/catalog/"
        try:
            resp = self.session.get(url, params={"q": f"{brand} skincare", "ajax": "true"}, timeout=15)
            # If JSON fails, try regex from HTML
            ids = re.findall(r'"itemId"\s*:\s*"(\d+)"', resp.text)
            names = re.findall(r'"name"\s*:\s*"([^"]+)"', resp.text)
            return [{"item_id": i, "product_name": n} for i, n in zip(ids, names)]
        except: return []

    def get_reviews(self, item_id):
        url = "https://www.lazada.vn/pdp/review/getReviewList"
        try:
            resp = self.session.get(url, params={"itemId": item_id, "pageSize": 50, "pageNo": 1}, timeout=15)
            data = resp.json()
            return data.get("model", {}).get("items", [])
        except: return []

    def run(self):
        total = 0
        for brand in TARGET_BRANDS:
            logger.info(f"Processing Lazada Brand: {brand}")
            products = self.search_products(brand)
            for p in products[:PRODUCTS_PER_BRAND]:
                if _checkpoint_exists(p['item_id']): continue
                
                raw_reviews = self.get_reviews(p['item_id'])
                reviews = []
                for r in raw_reviews:
                    comment = r.get("reviewContent", "").strip()
                    if not comment: continue
                    reviews.append({
                        "platform": "lazada", "item_id": p['item_id'], "product_name": p['product_name'],
                        "rating": int(r.get("rating", 0)), "comment": comment,
                        "reviewer_name": r.get("buyerName", "anon"), "helpful_count": 0,
                        "review_time": datetime.utcnow(), "is_verified_purchase": True, "scraped_at": datetime.utcnow()
                    })
                
                if reviews:
                    out = self.output_dir / f"lazada_{p['item_id']}.parquet"
                    pq.write_table(pa.Table.from_pylist(reviews, schema=LAZADA_SCHEMA), out)
                    _upload_to_s3(out)
                    _write_checkpoint(p['item_id'])
                    total += len(reviews)
            time.sleep(random.uniform(2, 5))
        logger.success(f"Lazada Done: {total} reviews")

if __name__ == "__main__":
    LazadaScraper().run()
