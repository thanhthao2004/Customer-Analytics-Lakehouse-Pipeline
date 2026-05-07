"""
Shopee review scraper using Selenium (undetected_chromedriver).
Bypasses Datadome anti-bot checks by using a real browser window.

Target: Skincare products on Shopee VN (Health & Beauty > Skincare)

Cloud/GHA mode:
  Set CI=true in environment to enable headless mode + GHA Chrome flags.
  Set PROXY_HOST/PROXY_PORT/PROXY_USER/PROXY_PASS for residential proxy.

Schema fields per review:
    platform, brand, item_id (auto-incremented), product_name,
    skin_type, total_like, formula, time_delivery,
    rating, comment, reviewer_name, review_time
"""

import os
import time
import re
import json
import ssl
import urllib.parse
from pathlib import Path
from datetime import datetime
from itertools import count as auto_counter

# On macOS (local dev) bypass SSL verification for uc driver downloads.
# On GHA (Linux) this is a no-op.
if ssl.OPENSSL_VERSION.startswith("OpenSSL") and os.name == "posix" and not os.environ.get("CI"):
    ssl._create_default_https_context = ssl._create_unverified_context

# ── Cloud / GHA detection ─────────────────────────────────────────────────────
IS_CI = os.environ.get("CI", "").lower() in ("true", "1", "yes")

# ── Proxy config (Webshare.io free tier or any SOCKS5/HTTP proxy) ─────────────
PROXY_HOST = os.environ.get("PROXY_HOST", "")
PROXY_PORT = os.environ.get("PROXY_PORT", "")
PROXY_USER = os.environ.get("PROXY_USER", "")
PROXY_PASS = os.environ.get("PROXY_PASS", "")

# ── S3 / R2 upload config ─────────────────────────────────────────────────────
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
AWS_ACCESS_KEY  = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION      = os.environ.get("AWS_REGION", "auto")

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

        # ── Mandatory GHA / headless flags ────────────────────────────────────
        if IS_CI:
            options.add_argument("--headless=new")       # Chrome 112+ headless
            options.add_argument("--no-sandbox")          # required in container
            options.add_argument("--disable-dev-shm-usage")  # avoids /dev/shm OOM
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1280,1024")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-setuid-sandbox")
            options.add_argument("--remote-debugging-port=9222")
        else:
            # Local dev — visible browser for debugging
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1280,1024")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

        # ── Proxy injection (Webshare.io / any HTTP proxy) ────────────────────
        if PROXY_HOST and PROXY_PORT:
            if PROXY_USER and PROXY_PASS:
                # Authenticated proxy — inject via Chrome extension approach
                proxy_arg = f"--proxy-server=http://{PROXY_HOST}:{PROXY_PORT}"
                options.add_argument(proxy_arg)
                # Selenium wire or Playwright needed for authenticated proxies;
                # for simplicity we use env-var credentials via CDP later.
                logger.info(f"[Shopee] Using proxy: {PROXY_HOST}:{PROXY_PORT}")
            else:
                options.add_argument(f"--proxy-server=http://{PROXY_HOST}:{PROXY_PORT}")
                logger.info(f"[Shopee] Using unauthenticated proxy: {PROXY_HOST}:{PROXY_PORT}")

        # version_main=None → auto-detect installed Chrome version on runner
        chrome_version = None if IS_CI else 146
        self.driver = uc.Chrome(options=options, version_main=chrome_version)

        # Authenticate proxy via CDP (needed for user:pass proxies in headless)
        if IS_CI and PROXY_HOST and PROXY_USER and PROXY_PASS:
            self.driver.execute_cdp_cmd(
                "Network.enable", {}
            )
            self.driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders",
                {"headers": {
                    "Proxy-Authorization": __import__('base64').b64encode(
                        f"{PROXY_USER}:{PROXY_PASS}".encode()
                    ).decode()
                }}
            )

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
        assert self.driver is not None

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
        assert self.driver is not None

        surrogate_id = next(_ITEM_ID_GEN)

        self.driver.get(url)
        time.sleep(3)
        self._check_login_redirect(url)
        self._close_modals()

        # ── Deep Scroll to trigger lazy-loaded review XHR ─────────────────────
        # Shopee reviews only load after the page bottom fires a network request.
        # We must progressively scroll past the product specs to trigger that XHR.
        logger.debug(f"[Shopee] Scrolling product page to trigger review loading...")
        for _ in range(15):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.6)
            self._close_modals()

        # Scroll back into the review section specifically
        try:
            review_section = self.driver.find_element(
                By.XPATH,
                "//div[contains(@class,'product-rating') or contains(@class,'pdp-block') or @id='product-rating']"
            )
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", review_section)
        except Exception:
            self.driver.execute_script("window.scrollBy(0, -800);")

        # Wait for the review XHR response to populate the DOM
        time.sleep(3)

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
            # ── Multi-selector fallback for review containers ─────────────────
            # Shopee uses different container structures across product types.
            # We try multiple selectors in priority order.
            review_container_css = None
            for selector in [
                "div[data-cmtid]",
                "div.shopee-product-rating",
                ".product-ratings__item",
                "div[class*='cmt-item']",
                "div[class*='review-item']",
            ]:
                try:
                    found = WebDriverWait(self.driver, 6).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                    if found:
                        review_container_css = selector
                        logger.debug(f"[Shopee] Review selector matched: '{selector}' ({len(found)} els)")
                        break
                except TimeoutException:
                    continue

            if not review_container_css:
                logger.warning(f"[Shopee] No review containers found for '{product_name[:30]}'. Skipping.")
                break

            # Extract all review data using structural/semantic selectors (not obfuscated class names)
            # This is robust to Shopee's weekly React class-name rotation.
            try:
                js_script = f"""
                var container_sel = {repr(review_container_css)};
                return Array.from(document.querySelectorAll(container_sel)).map(function(el) {{

                    // ── Rating: count filled star SVGs ──────────────────────────
                    var rating = 0;
                    var stars = el.querySelectorAll('svg');
                    stars.forEach(function(s) {{
                        var cls = s.className && s.className.baseVal ? s.className.baseVal : '';
                        if (cls.indexOf('icon-rating-solid') !== -1 || cls.indexOf('rating--on') !== -1) {{
                            rating++;
                        }} else if (cls.indexOf('icon-rating') !== -1 || cls.indexOf('rating--off') !== -1) {{
                            var poly = s.querySelector('polygon,path');
                            if (poly && poly.getAttribute('fill') && poly.getAttribute('fill') !== 'none' && poly.getAttribute('fill') !== '#fff') {{
                                rating++;
                            }}
                        }}
                    }});
                    if (rating === 0) {{
                        // Fallback: count aria-label stars or data-score
                        var scoreEl = el.querySelector('[data-score],[aria-label*="sao"],[aria-label*="star"]');
                        if (scoreEl) {{
                            var score = parseInt(scoreEl.getAttribute('data-score') || scoreEl.getAttribute('aria-label') || '0');
                            if (!isNaN(score)) rating = score;
                        }}
                    }}
                    if (rating > 5) rating = 5;

                    // ── Comment: use structural query, not obfuscated class ────
                    var comment = '';
                    // Try ordered list of stable structural selectors
                    var comment_selectors = [
                        '[class*="content"][class*="review"]',
                        '[class*="cmt"][class*="text"]',
                        '[class*="comment"] p',
                        '[class*="review"] p',
                        'p[class*="text"]'
                    ];
                    for (var i = 0; i < comment_selectors.length; i++) {{
                        var cel = el.querySelector(comment_selectors[i]);
                        if (cel && cel.innerText && cel.innerText.trim().length > 3) {{
                            comment = cel.innerText.trim();
                            break;
                        }}
                    }}
                    // Final fallback: grab the longest text block inside the container
                    if (!comment) {{
                        var all_text = Array.from(el.querySelectorAll('p,span,div'))
                            .filter(function(e) {{ return e.children.length === 0; }})
                            .map(function(e) {{ return e.innerText ? e.innerText.trim() : ''; }})
                            .filter(function(t) {{ return t.length > 10; }});
                        if (all_text.length > 0) {{
                            comment = all_text.reduce(function(a,b) {{ return a.length > b.length ? a : b; }}, '');
                        }}
                    }}

                    // ── Reviewer name ──────────────────────────────────────────
                    var reviewer_name = 'anonymous';
                    var reviewer_selectors = [
                        '[class*="name"]', '[class*="user"]', '[class*="author"]',
                        'a[href*="/profile/"]'
                    ];
                    for (var j = 0; j < reviewer_selectors.length; j++) {{
                        var rel = el.querySelector(reviewer_selectors[j]);
                        if (rel && rel.innerText && rel.innerText.trim().length > 0) {{
                            reviewer_name = rel.innerText.trim();
                            break;
                        }}
                    }}

                    // ── Timestamp ─────────────────────────────────────────────
                    var ts = '';
                    var time_selectors = [
                        'time', '[datetime]', '[class*="time"]', '[class*="date"]'
                    ];
                    for (var k = 0; k < time_selectors.length; k++) {{
                        var tel = el.querySelector(time_selectors[k]);
                        if (tel) {{
                            ts = tel.getAttribute('datetime') || tel.innerText.trim();
                            if (ts) break;
                        }}
                    }}

                    return {{rating: rating, comment: comment, reviewer_name: reviewer_name, ts: ts}};
                }});
                """
                page_reviews = self.driver.execute_script(js_script)
                logger.debug(f"[Shopee] JS extracted {len(page_reviews) if page_reviews else 0} raw review blocks.")

                for r_data in (page_reviews or []):
                    if not r_data.get('comment'):
                        continue

                    review_time_val = datetime.utcnow()
                    ts = r_data.get('ts', '')
                    if ts:
                        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
                            try:
                                review_time_val = datetime.strptime(ts[:16], fmt[:len(ts[:16])])
                                break
                            except Exception:
                                continue

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
                        "rating":        int(r_data.get('rating') or 0),
                        "comment":       r_data.get('comment', ''),
                        "reviewer_name": r_data.get('reviewer_name', 'anonymous'),
                        "review_time":   review_time_val,
                    })
            except Exception as exc:
                logger.warning(f"[Shopee] Review JS execution error: {exc}")

            # ── Paginate reviews ─────────────────────────────────────────────
            paginated = False
            for next_sel in [
                "button.shopee-icon-button--right",
                "button[aria-label='Next']",
                ".shopee-page-controller button:last-child",
                "li.ant-pagination-next button",
            ]:
                try:
                    next_btn = self.driver.find_element(By.CSS_SELECTOR, next_sel)
                    if not next_btn.is_enabled():
                        break
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(2.5)
                    self._close_modals()