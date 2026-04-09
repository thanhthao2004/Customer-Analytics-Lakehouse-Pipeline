import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

options = uc.ChromeOptions()
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,1024")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
# Remove headless to mimic real scraper

print("Initializing browser...")
driver = uc.Chrome(options=options, version_main=146)
print("Navigating...")
driver.get("https://shopee.vn/search?keyword=CeraVe")
time.sleep(5)
print("URL:", driver.current_url)
print("Title:", driver.title)
items = driver.find_elements(By.CSS_SELECTOR, 'a[href*="-i."]')
print(f"Found {len(items)} items using a[href*='-i.']")

links = driver.find_elements(By.CSS_SELECTOR, 'a')
filtered = [a.get_attribute('href') for a in links if a.get_attribute('href') and '-i.' in a.get_attribute('href')]
print("Sample links:", filtered[:10])

driver.save_screenshot("shopee_test.png")
print("Screenshot saved.")
driver.quit()
