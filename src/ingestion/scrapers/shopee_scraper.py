"""
Shopee review scraper using Selenium (undetected_chromedriver).
Bypasses Datadome anti-bot checks by using a real browser window.

Target: Skincare products on Shopee VN (Health & Beauty > Skincare)

Schema fields per review:
    platform, brand, item_id (auto-incremented), product_name,
    skin_type, total_like, formula, time_delivery,
    rating, comment, reviewer_name, review_time
"""

import time
import re
import json
import ssl
import urllib.parse
from pathlib import Path
from datetime import datetime
from itertools import count as auto_counter

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


# ── Schema ────────────────────────────────────────────────────────────────────
SHOPEE_SCHEMA = pa.schema([
    pa.field("platform",      pa.string()),
    pa.field("brand",         pa.string()),
    pa.field("item_id",       pa.int64()),       # auto-incremented surrogate key
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

# Global surrogate key generator (persists across scrape_item calls)
_ITEM_ID_GEN = auto_counter(start=1)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_total_like(text: str) -> int:
    """Convert Shopee like strings like 'Đã thích (12,7k)' → 12700."""
    m = re.search(r"\(([^)]+)\)", text)
    if not m:
        return 0
    raw = m.group(1).strip().replace("\u00a0", "").replace(" ", "")
    # Handle Vietnamese decimal comma: '12,7k' → 12700, '1.2k' → 1200
    raw = raw.replace(",", ".")          # normalise decimal separator
    if raw.lower().endswith("k"):
        try:
            return int(float(raw[:-1]) * 1000)
        except ValueError:
            pass
    try:
        return int(re.sub(r"[^0-9]", "", raw))
    except ValueError:
        return 0


def _get_spec_field(driver, label: str) -> str:
    """
    Grab the text value of a Shopee product spec block:
        <div class="ybxj32">
            <h3 class="VJOnTD">{label}</h3>
            <div>VALUE</div>   OR   <a ...>VALUE</a>
        </div>
    """
    xpath = (
        f"//div[contains(@class,'ybxj32')]"
        f"[.//h3[normalize-space()='{label}']]"
        f"/*[not(self::h3)][last()]"
    )
    try:
        el = driver.find_element(By.XPATH, xpath)
        return el.text.strip()
    except Exception:
        return ""


# ── Scraper ───────────────────────────────────────────────────────────────────

class ShopeeScraper:
    """Scrapes skincare product reviews from Shopee VN via Selenium."""

    BASE_URL = "https://shopee.vn"
    LOGIN_URL = "https://shopee.vn/buyer/login"

    def __init__(self, output_dir: str | None = None):
        self.output_dir = Path(output_dir or settings.RAW_DATA_DIR) / "shopee"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.driver = None
        self._logged_in = False   # track so we only pause once

    # ── Driver ────────────────────────────────────────────────────────────────

    def init_driver(self):
        options = uc.ChromeOptions()
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280,1024")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        self.driver = uc.Chrome(options=options, version_main=146)

    def _close_modals(self):
        """Forcefully remove Shopee blocking login popups/overlays via JS."""
        try:
            self.driver.execute_script(
                "document.querySelectorAll('.shopee-popup__close-btn').forEach(b=>b.click());"
                "document.querySelectorAll('[class*=\"shopee-popup\"],[class*=\"shopee-modal\"]')"
                ".forEach(e=>e.remove());"
            )
        except Exception:
            pass

    def _wait_for_login(self, next_url: str):
        """
        Shopee already shows the login / captcha page — just wait here.
        Do NOT navigate away; that causes a second redirect loop.
        """
        logger.warning("[Shopee] Login or captcha page detected.")
        logger.warning("[Shopee]   Please complete login in the browser window. Waiting 120 seconds…")
        time.sleep(120)
        self._logged_in = True
        logger.info("[Shopee] Resuming — navigating to target URL.")
        self.driver.get(next_url)
        time.sleep(4)
        self._close_modals()

    def _check_login_redirect(self, expected_url: str):
        cur = self.driver.current_url
        is_blocked = (
            "verify/traffic" in cur
            or "buyer/login" in cur
            or ("login" in cur and "shopee.vn" in cur)
        )
        if is_blocked and not self._logged_in:
            self._wait_for_login(expected_url)
        elif is_blocked and self._logged_in:
            # Already paused once — short retry in case page reload is needed
            logger.warning("[Shopee] Still on login page after first pause. Retrying navigate…")
            self.driver.get(expected_url)
            time.sleep(5)

    # ── Brand / Product Discovery ─────────────────────────────────────────────

    def _parse_brands(self) -> list[str]:
        """Read brand list from category.txt (tab-separated, Shopee line)."""
        cat = Path("data/html-scripts/category.txt")
        if not cat.exists():
            logger.warning("category.txt not found, using built-in fallback brands.")
            return [
                "CeraVe", "La Roche-Posay", "Innisfree", "The Ordinary",
                "Bioderma", "Hada Labo", "Klairs", "Anessa", "Senka", "Neutrogena",
            ]
        for line in cat.read_text(encoding="utf-8").splitlines():
            if line.strip().lower().startswith("shopee"):
                brands_str = line.split("\t", 1)[-1]
                return [b.strip() for b in brands_str.split(",") if b.strip()]
        return ["CeraVe"]

    def discover_products(self, target_count: int = 100) -> list[dict]:
        """
        For each configured brand, search Shopee and collect product metadata
        until `target_count` unique products are found.
        """
        if not self.driver:
            self.init_driver()

        # ── Open Shopee main page first so user can log in ────────────────────
        logger.info("[Shopee] Opening Shopee main page. You have 120 s to log in if needed.")
        self.driver.get(self.BASE_URL)
        time.sleep(5)
        self._check_login_redirect(self.BASE_URL)

        brands = self._parse_brands()
        products: list[dict] = []
        seen_ids: set[str] = set()
        per_brand = max(5, target_count // len(brands) + 2)

        for brand in brands:
            if len(products) >= target_count:
                break

            logger.info(f"[Shopee] Discovering top products for brand: '{brand}'…")
            search_url = (
                f"{self.BASE_URL}/search"
                f"?keyword={urllib.parse.quote(brand + ' skincare')}"
            )
            self.driver.get(search_url)
            time.sleep(4)
            self._check_login_redirect(search_url)
            self._close_modals()

            # Scroll to trigger lazy-loaded items
            for _ in range(4):
                self.driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(0.8)

            try:
                anchors = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="-i."]')
                brand_count = 0
                for a in anchors:
                    href = a.get_attribute("href") or ""
                    m = re.search(r'-i\.(\d+)\.(\d+)', href)
                    if not m:
                        continue
                    shop_id_raw, item_id_raw = m.group(1), m.group(2)
                    uid = f"{shop_id_raw}_{item_id_raw}"
                    if uid in seen_ids:
                        continue

                    # Extract a human-readable name from the URL slug
                    slug_m = re.search(r'shopee\.vn/(.*?)-i\.', href)
                    name = (
                        urllib.parse.unquote(slug_m.group(1).replace("-", " "))
                        if slug_m else f"Shopee Product {item_id_raw}"
                    )

                    products.append({
                        "brand":        brand,
                        "shop_id_raw":  shop_id_raw,   # kept internally for URL construction
                        "item_id_raw":  item_id_raw,
                        "product_name": name,
                        "url":          href,
                    })
                    seen_ids.add(uid)
                    brand_count += 1

                    if brand_count >= per_brand or len(products) >= target_count:
                        break

            except Exception as exc:
                logger.error(f"[Shopee] Discovery error for '{brand}': {exc}")

        logger.info(f"[Shopee] Discovered {len(products)} products across {len(brands)} brands.")
        return products

    # ── Per-Product Scraping ──────────────────────────────────────────────────

    def scrape_item(
        self,
        brand: str,
        shop_id_raw: str,
        item_id_raw: str,
        product_name: str,
        url: str,
        max_reviews: int = 50,
    ) -> list[dict]:
        """Scrape reviews (and product-level fields) for a single product page."""
        if not self.driver:
            self.init_driver()

        surrogate_id = next(_ITEM_ID_GEN)

        self.driver.get(url)
        time.sleep(3)
        self._check_login_redirect(url)
        self._close_modals()

        # Scroll down incrementally to ensure lazy-loaded sections (like reviews) trigger
        # We scroll by 500px steps until the bottom of the page is reached (or up to 25 times)
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        for _ in range(25):
            self.driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(0.5)
            self._close_modals()
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            # If we've reached near the bottom, scroll back up a bit and break? No, just keep going
            # to make sure all network requests fire.
            
        # Give it a couple extra seconds to render the reviews
        time.sleep(2)

        # ── Product-level fields (extracted once per page) ────────────────────
        # product_name from H1
        try:
            h1 = self.driver.find_element(By.CSS_SELECTOR, "h1.vR6K3w")
            product_name = h1.text.strip() or product_name
        except Exception:
            pass

        # brand from spec block
        page_brand = _get_spec_field(self.driver, "Thương hiệu") or brand

        # skin_type
        skin_type = _get_spec_field(self.driver, "Loại Da")

        # formula
        formula = _get_spec_field(self.driver, "Công Thức")

        # total_like  <div class="rhG6k7">Đã thích (12,7k)</div>
        total_like = 0
        try:
            like_el = self.driver.find_element(By.CSS_SELECTOR, ".rhG6k7")
            total_like = _parse_total_like(like_el.text)
        except Exception:
            pass

        # price and flash_sale
        price = ""
        try:
            # Main price element which almost always contains ₫
            price_el = self.driver.find_element(
                By.XPATH, "//div[contains(text(), '₫') and not(contains(text(), '-'))]"
            )
            price = price_el.text.strip()
        except NoSuchElementException:
            try:
                # Fallback user's class
                price_el = self.driver.find_element(By.CSS_SELECTOR, "div.IZPeQz")
                price = price_el.text.strip()
            except NoSuchElementException:
                pass
                
        flash_sale = "No"
        try:
            self.driver.find_element(
                By.CSS_SELECTOR, 
                "div.shopee-countdown-timer, img[alt='flash sale branding icon']"
            )
            flash_sale = "Yes"
        except NoSuchElementException:
            pass

        # time_delivery — first styled <span> inside the shipping section
        time_delivery = ""
        try:
            td_el = self.driver.find_element(
                By.XPATH,
                "//section[.//h2[normalize-space()='Vận chuyển']]"
                "//div[contains(@class,'O3NAB1')]/span",
            )
            time_delivery = td_el.text.strip()
        except Exception:
            pass

        # ── Review-level extraction ───────────────────────────────────────────
        reviews: list[dict] = []
        page_count = 0
        logger.info(f"[Shopee] Scraping '{product_name[:40]}' (id={surrogate_id})…")

        while len(reviews) < max_reviews:
            try:
                review_els = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "div[data-cmtid]")
                    )
                )
            except TimeoutException:
                logger.debug(f"[Shopee] No reviews visible for '{product_name[:30]}'.")
                break

            # Extract all review data quickly using JavaScript to avoid StaleElementReferenceException
            try:
                js_script = """
                return Array.from(document.querySelectorAll('div[data-cmtid]')).map(el => {
                    let rating = 0;
                    el.querySelectorAll('svg.shopee-svg-icon').forEach(s => {
                        if (s.classList.contains('icon-rating-solid')) rating++;
                        else if (s.classList.contains('icon-rating')) {
                            let poly = s.querySelector('polygon');
                            if (poly && poly.getAttribute('fill') !== 'none') rating++;
                        }
                    });
                    if (rating > 5) rating = 5;
                    
                    let comment = '';
                    let c_el = el.querySelector('.YNedDV');
                    if (c_el) comment = c_el.innerText.trim();
                    
                    let reviewer_name = 'anonymous';
                    let r_el = el.querySelector('.InK5kS');
                    if (r_el) reviewer_name = r_el.innerText.trim();
                    
                    let ts = '';
                    let t_el = el.querySelector('.XYk98l');
                    if (t_el) ts = t_el.innerText.trim();
                    
                    return {rating, comment, reviewer_name, ts};
                });
                """
                page_reviews = self.driver.execute_script(js_script)
                
                for r_data in page_reviews:
                    if not r_data.get('comment'):
                        continue
                        
                    review_time_val = datetime.utcnow()
                    ts = r_data.get('ts')
                    if ts:
                        try:
                            review_time_val = datetime.strptime(ts, "%Y-%m-%d %H:%M")
                        except Exception:
                            pass
                            
                    reviews.append({
                        "platform":      "shopee",
                        "brand":         page_brand,
                        "item_id":       surrogate_id,
                        "product_name":  product_name,
                        "skin_type":     skin_type,
                        "total_like":    total_like,
                        "formula":       formula,
                        "time_delivery": time_delivery,
                        "price":         price,
                        "flash_sale":    flash_sale,
                        "rating":        r_data.get('rating', 0),
                        "comment":       r_data.get('comment', ''),
                        "reviewer_name": r_data.get('reviewer_name', 'anonymous'),
                        "review_time":   review_time_val,
                    })
            except Exception as exc:
                logger.debug(f"[Shopee] Review JS execution error: {exc}")

            # Paginate reviews
            try:
                next_btn = self.driver.find_element(
                    By.CSS_SELECTOR, "button.shopee-icon-button--right"
                )
                if not next_btn.is_enabled():
                    break
                self.driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(2)
                self._close_modals()
                page_count += 1
                if page_count >= max(3, max_reviews // 5):
                    break
            except NoSuchElementException:
                break

        logger.info(
            f"[Shopee] Collected {len(reviews)} reviews for '{product_name[:40]}'."
        )
        return reviews[:max_reviews]

    # ── Parquet Persistence ───────────────────────────────────────────────────

    def save_to_parquet(self, reviews: list[dict], filename: str) -> Path | None:
        if not reviews:
            logger.warning("[Shopee] No reviews to save.")
            return None
        out = self.output_dir / filename
        table = pa.Table.from_pylist(reviews, schema=SHOPEE_SCHEMA)
        pq.write_table(table, out, compression="snappy")
        logger.info(f"[Shopee] Saved {len(reviews)} reviews → {out}")
        return out

    # ── Orchestration ─────────────────────────────────────────────────────────

    def run(
        self,
        products: list[dict] | None = None,
        max_reviews_per_product: int = 1000,
    ) -> Path | None:
        """Discover products → scrape reviews → save to local Parquet."""
        
        ckpt_path = self.output_dir / "scraped_urls.json"
        scraped_urls = set()
        if ckpt_path.exists():
            try:
                scraped_urls = set(json.loads(ckpt_path.read_text("utf-8")))
                logger.info(f"[Shopee] Loaded checkpoint: {len(scraped_urls)} products already scraped.")
            except Exception as e:
                logger.warning(f"[Shopee] Failed to load checkpoint: {e}")

        if products is None:
            products = self.discover_products(target_count=100)

        # Ensure output directory exists before writing per-product files
        self.output_dir.mkdir(parents=True, exist_ok=True)

        try:
            for p in products:
                url = p.get("url", "")
                if url in scraped_urls:
                    logger.info(f"[Shopee] Skipping already scraped product: {p.get('product_name', 'Unknown')[:30]}...")
                    continue

                item_id_raw = p.get("item_id_raw", "0")

                revs = self.scrape_item(
                    brand=p.get("brand", "Unknown"),
                    shop_id_raw=p.get("shop_id_raw", "0"),
                    item_id_raw=item_id_raw,
                    product_name=p.get("product_name", ""),
                    url=url,
                    max_reviews=max_reviews_per_product,
                )
                
                if revs:
                    fname = f"shopee_{item_id_raw}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
                    self.save_to_parquet(revs, fname)
                
                # Update checkpoint
                scraped_urls.add(url)
                ckpt_path.write_text(json.dumps(list(scraped_urls)), "utf-8")
                
        finally:
            if self.driver:
                self.driver.quit()

        return self.output_dir


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ShopeeScraper()
    output = scraper.run(max_reviews_per_product=1000)
    print(f"Done! Saved to: {output}")
