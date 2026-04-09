import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

options = uc.ChromeOptions()
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1280,1024")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = uc.Chrome(options=options, version_main=146)
driver.get("https://www.lazada.vn/catalog/?q=CeraVe")
time.sleep(5)
print("URL:", driver.current_url)
print("Title:", driver.title)
items = driver.find_elements(By.CSS_SELECTOR, "a[href*='-i']")
print(f"Found {len(items)} items on Lazada")
driver.quit()
