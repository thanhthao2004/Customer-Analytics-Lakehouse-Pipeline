"""
Lazada VN review scraper.
Uses Selenium (undetected_chromedriver) to bypass captchas/Datadome.

Target: Skincare products on Lazada VN (Health & Beauty > Skincare)
URL pattern: lazada.vn/products/-i{item_id}.html
"""

import time
import random
import ssl
import re
from pathlib import Path
from datetime import datetime

# Bypass macOS SSL certificate verification for undetected_chromedriver downloads
ssl._create_default_https_context = ssl._create_unverified_context

import pyarrow as pa
import pyarrow.parquet as pq
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from loguru import logger

from config import settings


LAZADA_SCHEMA = pa.schema([
    pa.field("platform", pa.string()),
    pa.field("item_id", pa.string()),
    pa.field("product_name", pa.string()),
    pa.field("rating", pa.int32()),
    pa.field("comment", pa.string()),
    pa.field("reviewer_name", pa.string()),
    pa.field("helpful_count", pa.int32()),
    pa.field("review_time", pa.timestamp("ms")),
    pa.field("is_verified_purchase", pa.bool_()),
    pa.field("scraped_at", pa.timestamp("ms")),
])


class LazadaScraper:
    """Scrapes skincare product reviews from Lazada VN via Selenium."""

    def __init__(self, output_dir: str | None = None):
        self.output_dir = Path(output_dir or settings.RAW_DATA_DIR) / "lazada"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None

    def init_driver(self):
        options = uc.ChromeOptions()
        # options.add_argument("--headless") # Comment out to debug visually
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280,1024")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        # Force chrome driver version to match local OS installed Chrome 146
        self.driver = uc.Chrome(options=options, version_main=146)

    def force_close_modals(self):
        """Use Javascript to forcefully destroy Lazada's blocking popups & overlays."""
        try:
            self.driver.execute_script("""
                document.querySelectorAll('.lazada-popup, .lzd-modal, .lzd-site-nav-menu-modal').forEach(e => e.remove());
                let closeBtns = document.querySelectorAll('div[data-spm="close"], div[data-spm="btn-close"]');
                closeBtns.forEach(btn => btn.click());
            """)
        except Exception:
            pass

    def parse_brands_from_category(self) -> list[str]:
        cat_file = Path("data/html-scripts/category.txt")
        if not cat_file.exists():
            return ["CeraVe", "La Roche-Posay", "Innisfree", "The Ordinary", "Bioderma", "Hada Labo"]
        
        lines = cat_file.read_text(encoding="utf-8").strip().split('\n')
        # Lazada line says "Same brands", so extract from the Shopee line
        for line in lines:
            if line.startswith("Shopee"):
                brands_str = line.split("\t")[-1]
                return [b.strip() for b in brands_str.split(",")]
        return ["CeraVe"]

    def discover_products(self, target_count=100) -> list[dict]:
        """Dynamically search and identify products for target brands."""
        if not self.driver:
            self.init_driver()

        brands = self.parse_brands_from_category()
        products = []
        seen_ids = set()
        products_per_brand = max(4, target_count // len(brands) + 1)

        import urllib.parse
        for brand in brands:
            if len(products) >= target_count:
                break
                
            logger.info(f"[Lazada] Discovering top products for: '{brand}'...")
            url = f"https://www.lazada.vn/catalog/?q={urllib.parse.quote(brand + ' skincare')}"
            
            self.driver.get(url)
            time.sleep(4)
            
            if "login" in self.driver.current_url.lower() or "verify" in self.driver.current_url.lower():
                logger.warning("[Lazada] Login or verification required!")
                logger.warning("[Lazada] Please solve the captcha or login manually in the open browser window.")
                logger.warning("[Lazada] Pausing for 60 seconds...")
                time.sleep(60)
                self.driver.get(url)
                time.sleep(4)

            self.force_close_modals()

            # Scroll to load search results lazy items
            for _ in range(4):
                self.driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(1)

            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='-i']")
                brand_count = 0
                for item in items:
                    href = item.get_attribute("href")
                    if not href:
                        continue
                        
                    match = re.search(r'-i(\d+)(?:-s\d+)?\.html', href)
                    if match:
                        item_id = match.group(1)
                        if item_id in seen_ids:
                            continue
                            
                        # Extract product name robustly
                        slug_match = re.search(r'products/(.*?)-i\d+', href)
                        if slug_match:
                            product_name = urllib.parse.unquote(slug_match.group(1).replace('-', ' '))
                        else:
                            try:
                                product_name = item.text.strip()
                            except:
                                product_name = f"Lazada Product {item_id}"
                        
                        if not product_name: 
                            product_name = f"Lazada Product {item_id}"
                            
                        products.append({
                            "item_id": item_id,
                            "product_name": product_name
                        })
                        seen_ids.add(item_id)
                        brand_count += 1
                        
                        if brand_count >= products_per_brand or len(products) >= target_count:
                            break
            except Exception as e:
                logger.error(f"[Lazada] Error during product discovery for {brand}: {e}")

        logger.info(f"[Lazada] Identified {len(products)} high-volume products across {len(brands)} brands.")
        return products

    def scrape_item(
        self,
        item_id: str,
        product_name: str,
        max_reviews: int = 50,
    ) -> list[dict]:
        """Scrape reviews for a single product."""
        if not self.driver:
            self.init_driver()

        url = f"https://www.lazada.vn/products/-i{item_id}.html"
        self.driver.get(url)
        time.sleep(3)

        self.force_close_modals()

        # Scroll to reviews
        for i in range(6):
            self.driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(1)
            self.force_close_modals()

        product_reviews = []
        page_count = 0

        logger.info(f"[Lazada] Scraping item_id={item_id}, product='{product_name[:30]}...'")

        while len(product_reviews) < max_reviews:
            try:
                review_elements = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".item._h213"))
                )
            except TimeoutException:
                logger.debug(f"[Lazada] No reviews found for {product_name[:30]}...")
                break

            for el in review_elements:
                try:
                    stars = el.find_elements(By.CSS_SELECTOR, "img[src*='star-yellow']")
                    rating = len(stars)

                    comment_el = el.find_elements(By.CSS_SELECTOR, ".content")
                    comment = comment_el[0].text.strip() if comment_el else ""

                    if comment:
                         product_reviews.append({
                            "platform": "lazada",
                            "item_id": item_id,
                            "product_name": product_name,
                            "rating": rating,
                            "comment": comment,
                            "reviewer_name": "anonymous",
                            "helpful_count": 0,
                            "review_time": datetime.utcnow(),
                            "is_verified_purchase": False,
                            "scraped_at": datetime.utcnow()
                        })
                except Exception:
                    pass

            try:
                next_btn = self.driver.find_element(By.CSS_SELECTOR, "button.next-pagination-item.next")
                if next_btn.get_attribute("disabled"):
                    break
                self.driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(3)
                self.force_close_modals()
                
                page_count += 1
                if page_count > (max_reviews // 5):
                     break
            except NoSuchElementException:
                break

        logger.info(f"[Lazada] Collected {len(product_reviews)} reviews for '{product_name[:30]}...'")
        return product_reviews[:max_reviews]

    def save_to_parquet(self, reviews: list[dict], filename: str | None = None) -> Path | None:
        if not reviews:
            logger.warning("No Lazada reviews to save.")
            return None

        fname = filename or f"lazada_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
        out_path = self.output_dir / fname

        table = pa.Table.from_pylist(reviews, schema=LAZADA_SCHEMA)
        pq.write_table(table, out_path, compression="snappy")
        logger.info(f"[Lazada] Saved {len(reviews)} reviews → {out_path}")
        return out_path

    def run(
        self,
        products: list[dict] | None = None,
        max_reviews_per_product: int = 50,
    ) -> Path | None:
        """
        Scrape dynamic top products using Selenium, save to Parquet.
        """
        if products is None:
            # Parse configured target brands to fetch 100 total products
            products = self.discover_products(target_count=100)

        all_reviews = []
        try:
            for product in products:
                reviews = self.scrape_item(
                    item_id=product["item_id"],
                    product_name=product["product_name"],
                    max_reviews=max_reviews_per_product,
                )
                all_reviews.extend(reviews)
        finally:
            if self.driver:
                self.driver.quit()

        return self.save_to_parquet(all_reviews)


if __name__ == "__main__":
    scraper = LazadaScraper()
    output_path = scraper.run(max_reviews_per_product=50) # Reduced to 50 for Selenium
    print(f"Done! Saved to: {output_path}")
