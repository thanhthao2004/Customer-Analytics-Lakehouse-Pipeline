"""
Shopee VN Scraper — Login & Cookie Support
Strategy:
  1. Use SHOPEE_COOKIE if available (Most reliable).
  2. If not, use SHOPEE_USER/PASS with Selenium to log in and get cookies.
  3. Use the session/cookies to fetch data via API (Fastest).
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ── Env Config ──────────────────────────────────────────────────────────────
IS_CI = os.environ.get("CI", "").lower() in ("true", "1", "yes")

# Credentials & Cookies
SHOPEE_COOKIE = os.environ.get("SHOPEE_COOKIE", "")
SHOPEE_USER   = os.environ.get("SHOPEE_USER", "")
SHOPEE_PASS   = os.environ.get("SHOPEE_PASS", "")

PROXY_HOST = os.environ.get("PROXY_HOST", "")
PROXY_PORT = os.environ.get("PROXY_PORT", "")
PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")

S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY  = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION      = os.environ.get("AWS_REGION", "auto")

TARGET_BRANDS = ["CeraVe", "La Roche-Posay", "Innisfree", "The Ordinary", "Bioderma", "Hada Labo", "Klairs", "Anessa", "Senka", "Neutrogena"]
PRODUCTS_PER_BRAND = 10
MAX_REVIEWS_PER_PRODUCT = 50

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

SHOPEE_SCHEMA = pa.schema([
    pa.field("platform", pa.string()), pa.field("brand", pa.string()),
    pa.field("item_id", pa.int64()), pa.field("product_name", pa.string()),
    pa.field("skin_type", pa.string()), pa.field("total_like", pa.int32()),
    pa.field("formula", pa.string()), pa.field("time_delivery", pa.string()),
    pa.field("price", pa.string()), pa.field("flash_sale", pa.string()),
    pa.field("rating", pa.int32()), pa.field("comment", pa.string()),
    pa.field("reviewer_name", pa.string()), pa.field("review_time", pa.timestamp("ms")),
])

# ── Helpers ──────────────────────────────────────────────────────────────────

def _s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL or None, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION, config=Config(signature_version="s3v4"))

def _checkpoint_exists(item_id):
    if not (S3_BUCKET and AWS_ACCESS_KEY): return False
    try:
        resp = _s3_client().head_object(Bucket=S3_BUCKET, Key=f"checkpoints/shopee/{item_id}.done")
        return (datetime.now(timezone.utc) - resp["LastModified"]).total_seconds() / 3600 < 23
    except: return False

def _write_checkpoint(item_id):
    if not (S3_BUCKET and AWS_ACCESS_KEY): return
    try: _s3_client().put_object(Bucket=S3_BUCKET, Key=f"checkpoints/shopee/{item_id}.done", Body=b"done")
    except Exception as e: logger.warning(f"Checkpoint failed: {e}")

def _upload_to_s3(path):
    if not (S3_BUCKET and AWS_ACCESS_KEY): return
    try:
        key = f"raw/shopee/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}/{path.name}"
        _s3_client().upload_file(str(path), S3_BUCKET, key)
    except Exception as e: logger.warning(f"Upload failed: {e}")

# ── Login Logic ──────────────────────────────────────────────────────────────

def get_session_cookies():
    """Tries to get a valid session. Returns cookie string."""
    if SHOPEE_COOKIE:
        logger.info("Using provided SHOPEE_COOKIE from secrets.")
        return SHOPEE_COOKIE

    if not SHOPEE_USER or not SHOPEE_PASS:
        logger.warning("No cookies or credentials found. Running unauthenticated (high risk of block).")
        return ""

    logger.info(f"Attempting Selenium login for user: {SHOPEE_USER}")
    options = uc.ChromeOptions()
    if IS_CI:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

    # use_subprocess=True is critical for GHA to prevent process hanging
    driver = uc.Chrome(options=options, use_subprocess=True)
    try:
        driver.get("https://shopee.vn/buyer/login")
        time.sleep(3)
        
        # Fill credentials
        driver.find_element(By.NAME, "loginKey").send_keys(SHOPEE_USER)
        driver.find_element(By.NAME, "password").send_keys(SHOPEE_PASS)
        driver.find_element(By.XPATH, "//button[contains(text(), 'Đăng nhập') or contains(text(), 'Log In')]").click()
        
        # Wait for login success (redirect to home or profile)
        time.sleep(10) 
        
        # Capture cookies
        cookies = driver.get_cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        logger.success("Login successful, cookies captured.")
        return cookie_str
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return ""
    finally:
        driver.quit()

# ── Scraper ──────────────────────────────────────────────────────────────────

class ShopeeScraper:
    def __init__(self):
        self.output_dir = Path("data/raw/shopee")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cookie = get_session_cookies()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Cookie": self.cookie,
            "Referer": "https://shopee.vn/",
            "x-api-source": "pc"
        })

    def search_products(self, brand):
        url = "https://shopee.vn/api/v4/search/search_items"
        params = {"keyword": f"{brand} skincare", "limit": 20, "by": "relevancy", "order": "desc", "page_type": "search", "scenario": "PAGE_GLOBAL_SEARCH", "version": 2}
        try:
            # First try direct API
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 403:
                # Try HTML fallback if API is blocked
                logger.info(f"API 403 for {brand}, trying HTML fallback...")
                resp = self.session.get("https://shopee.vn/search", params={"keyword": f"{brand} skincare"}, timeout=15)
                ids = re.findall(r'"itemid":(\d+)', resp.text)
                sids = re.findall(r'"shopid":(\d+)', resp.text)
                return [{"item_id": i, "shop_id": s, "product_name": f"{brand} Product {i}"} for i, s in zip(ids, sids)]
            
            data = resp.json()
            items = data.get("data", {}).get("items", []) or data.get("items", [])
            return [{"item_id": str(i.get("item_basic", i).get("itemid")), "shop_id": str(i.get("item_basic", i).get("shopid")), "product_name": i.get("item_basic", i).get("name")} for i in items]
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def get_reviews(self, item_id, shop_id):
        url = "https://shopee.vn/api/v2/item/get_ratings"
        params = {"itemid": item_id, "shopid": shop_id, "limit": 50, "offset": 0, "filter": 0, "type": 0}
        try:
            resp = self.session.get(url, params=params, timeout=15)
            return resp.json().get("data", {}).get("ratings", [])
        except: return []

    def run(self):
        total = 0
        for brand in TARGET_BRANDS:
            logger.info(f"Processing Brand: {brand}")
            products = self.search_products(brand)
            for p in products[:PRODUCTS_PER_BRAND]:
                if _checkpoint_exists(p['item_id']): continue
                
                ratings = self.get_reviews(p['item_id'], p['shop_id'])
                reviews = []
                for r in ratings:
                    if not r.get("comment"): continue
                    reviews.append({
                        "platform": "shopee", "brand": brand, "item_id": int(p['item_id']),
                        "product_name": p['product_name'], "skin_type": "", "total_like": r.get("liked_count", 0),
                        "formula": "", "time_delivery": "", "price": "", "flash_sale": "No",
                        "rating": r.get("rating_star", 0), "comment": r.get("comment"),
                        "reviewer_name": r.get("author_username", "anon"),
                        "review_time": datetime.utcfromtimestamp(r.get("ctime", time.time()))
                    })
                
                if reviews:
                    out = self.output_dir / f"shopee_{p['item_id']}.parquet"
                    pq.write_table(pa.Table.from_pylist(reviews, schema=SHOPEE_SCHEMA), out)
                    _upload_to_s3(out)
                    _write_checkpoint(p['item_id'])
                    total += len(reviews)
            time.sleep(random.uniform(2, 5))
        logger.success(f"Shopee Done: {total} reviews")

if __name__ == "__main__":
    ShopeeScraper().run()
