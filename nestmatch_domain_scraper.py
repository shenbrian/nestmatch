"""
NestMatch — Domain.com.au Listing Scraper (Selenium version)
Session 14 · Pilot corpus builder

Usage:
    pip install selenium webdriver-manager
    python nestmatch_domain_scraper.py

Output:
    nestmatch_pilot_listings.csv
"""

import csv
import time
import random
import re
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# ── Suburb config ─────────────────────────────────────────────────────────────
SUBURBS = [
    {"suburb": "Newtown",        "postcode": "2042", "price_min": 900000,  "price_max": 2200000, "types": ["apartment", "townhouse", "house"]},
    {"suburb": "Mosman",         "postcode": "2088", "price_min": 2500000, "price_max": 7500000, "types": ["house", "townhouse", "apartment"]},
    {"suburb": "Chatswood",      "postcode": "2067", "price_min": 1200000, "price_max": 4500000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Marrickville",   "postcode": "2204", "price_min": 900000,  "price_max": 2500000, "types": ["house", "townhouse", "apartment"]},
    {"suburb": "Leichhardt",     "postcode": "2040", "price_min": 1000000, "price_max": 2800000, "types": ["house", "townhouse", "apartment"]},
    {"suburb": "Randwick",       "postcode": "2031", "price_min": 1200000, "price_max": 3200000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Maroubra",       "postcode": "2035", "price_min": 1000000, "price_max": 2800000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Paddington",     "postcode": "2021", "price_min": 1200000, "price_max": 3500000, "types": ["townhouse", "house", "apartment"]},
    {"suburb": "Redfern",        "postcode": "2016", "price_min": 700000,  "price_max": 2000000, "types": ["townhouse", "apartment", "house"]},
    {"suburb": "Rose Bay",       "postcode": "2029", "price_min": 2000000, "price_max": 6000000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Parramatta",     "postcode": "2150", "price_min": 600000,  "price_max": 1800000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Blacktown",      "postcode": "2148", "price_min": 700000,  "price_max": 1400000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Epping",         "postcode": "2121", "price_min": 1000000, "price_max": 2800000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Frenchs Forest", "postcode": "2086", "price_min": 1200000, "price_max": 2800000, "types": ["house", "townhouse", "apartment"]},
    {"suburb": "Ryde",           "postcode": "2112", "price_min": 900000,  "price_max": 2400000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "West Pymble",    "postcode": "2073", "price_min": 1800000, "price_max": 4000000, "types": ["house", "townhouse", "apartment"]},
    {"suburb": "Hornsby",        "postcode": "2077", "price_min": 800000,  "price_max": 2000000, "types": ["house", "apartment", "townhouse"]},
    {"suburb": "Strathfield",    "postcode": "2135", "price_min": 1000000, "price_max": 3000000, "types": ["house", "apartment", "townhouse"]},
]

TARGET_PER_SUBURB = 8
OUTPUT_FILE = "nestmatch_pilot_listings.csv"

# ── Browser setup ─────────────────────────────────────────────────────────────

def make_driver():
    options = Options()
    # Browser stays visible so you can see what's happening
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def build_url(suburb_cfg, ptype, page=1):
    suburb = suburb_cfg["suburb"].lower().replace(" ", "-")
    postcode = suburb_cfg["postcode"]
    price_min = suburb_cfg["price_min"]
    price_max = suburb_cfg["price_max"]
    slug = f"{suburb}-nsw-{postcode}"
    url = (
        f"https://www.domain.com.au/sale/{slug}/"
        f"?ptype={ptype}"
        f"&price={price_min}-{price_max}"
        f"&excludeunderoffer=1"
        f"&sort=dateupdated-desc"
    )
    if page > 1:
        url += f"&page={page}"
    return url


def parse_price(text):
    if not text:
        return None, None
    if re.search(r"contact|auction|enquire", text, re.I):
        return None, None
    nums = re.findall(r"[\d,]+", text)
    vals = []
    for n in nums:
        try:
            v = int(n.replace(",", ""))
            if v > 100000:
                vals.append(v)
        except:
            pass
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], vals[0]
    return vals[0], vals[-1]


def dismiss_overlays(driver):
    selectors = [
        "#onetrust-accept-btn-handler",
        "button[aria-label*='close']",
        "button[aria-label*='Close']",
        "button[aria-label*='dismiss']",
        "[data-testid*='modal'] button",
    ]
    for sel in selectors:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            btn.click()
            time.sleep(0.8)
        except:
            pass


def extract_listings(driver, suburb_cfg, ptype, needed):
    results = []
    page = 1

    while len(results) < needed and page <= 3:
        url = build_url(suburb_cfg, ptype, page)
        print(f"    Fetching: {url}")
        driver.get(url)

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "article, [data-testid*='listing']")
                )
            )
        except:
            print(f"    No listings loaded on page {page} — skipping")
            break

        time.sleep(random.uniform(2.0, 3.5))
        dismiss_overlays(driver)

        cards = driver.find_elements(
            By.CSS_SELECTOR,
            "article[data-testid*='listing'], div[data-testid*='listing-card']"
        )
        if not cards:
            cards = driver.find_elements(By.TAG_NAME, "article")

        print(f"    Found {len(cards)} cards on page {page}")
        if not cards:
            break

        for card in cards:
            if len(results) >= needed:
                break
            try:
                # Address
                address = ""
                for sel in ["[data-testid*='address']", "h2", "h3"]:
                    try:
                        address = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                        if address:
                            break
                    except:
                        pass
                if not address:
                    continue

                # Price
                price_raw = ""
                for sel in ["[data-testid*='price']", "[class*='price']"]:
                    try:
                        price_raw = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                        if price_raw:
                            break
                    except:
                        pass
                price_min_val, price_max_val = parse_price(price_raw)
                if not price_min_val:
                    continue

                # Beds / baths / parking
                beds = baths = parking = ""
                for el in card.find_elements(By.CSS_SELECTOR, "[data-testid]"):
                    testid = (el.get_attribute("data-testid") or "").lower()
                    val = el.text.strip()
                    try:
                        v = int(re.sub(r"[^\d]", "", val))
                    except:
                        continue
                    if "bed" in testid:
                        beds = v
                    elif "bath" in testid:
                        baths = v
                    elif "parking" in testid or "car" in testid:
                        parking = v

                # Listing URL
                listing_url = ""
                try:
                    href = card.find_element(
                        By.CSS_SELECTOR, "a[href*='property-']"
                    ).get_attribute("href") or ""
                    listing_url = href if href.startswith("http") else f"https://www.domain.com.au{href}"
                except:
                    pass

                # Agent / agency
                agent_name = agency_name = ""
                for sel in ["[data-testid*='agent-name']", "[class*='agent-name']"]:
                    try:
                        agent_name = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                        if agent_name:
                            break
                    except:
                        pass
                for sel in ["[data-testid*='agency']", "[class*='agency']", "[class*='brand']"]:
                    try:
                        agency_name = card.find_element(By.CSS_SELECTOR, sel).text.strip()
                        if agency_name:
                            break
                    except:
                        pass

                results.append({
                    "suburb": suburb_cfg["suburb"],
                    "property_type": ptype,
                    "street_address": address,
                    "price_min": price_min_val,
                    "price_max": price_max_val or price_min_val,
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "parking_spaces": parking,
                    "real_estate_agency": agency_name,
                    "sales_agent": agent_name,
                    "listing_url_domain": listing_url,
                    "listing_status": "for_sale",
                    "scraped_date": str(date.today()),
                    "title": "",
                    "land_size_sqm": "",
                    "internal_size_sqm": "",
                    "renovation_status": "",
                    "development_zone": "",
                    "agent_phone": "",
                    "listing_url_rea": "",
                })

            except Exception as e:
                print(f"    Card parse error: {e}")
                continue

        page += 1
        time.sleep(random.uniform(3.0, 5.0))

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Starting NestMatch Domain scraper (Selenium)...")
    print("An Edge window will open — don't close it.\n")

    driver = make_driver()
    all_listings = []

    try:
        # Visit homepage first to set cookies naturally
        print("Warming up — visiting Domain homepage...")
        driver.get("https://www.domain.com.au")
        time.sleep(random.uniform(4.0, 6.0))
        dismiss_overlays(driver)

        for suburb_cfg in SUBURBS:
            suburb_name = suburb_cfg["suburb"]
            types = suburb_cfg["types"]
            print(f"\n{'─'*60}")
            print(f"Suburb: {suburb_name}")

            suburb_listings = []
            remaining = TARGET_PER_SUBURB
            per_type = max(2, TARGET_PER_SUBURB // len(types))

            for ptype in types:
                if remaining <= 0:
                    break
                needed = min(per_type, remaining)
                print(f"  Type: {ptype} — targeting {needed} listings")
                results = extract_listings(driver, suburb_cfg, ptype, needed)
                print(f"  Got {len(results)} listings")
                suburb_listings.extend(results[:needed])
                remaining -= len(results[:needed])
                time.sleep(random.uniform(4.0, 7.0))

            print(f"  Total for {suburb_name}: {len(suburb_listings)}/{TARGET_PER_SUBURB}")
            all_listings.extend(suburb_listings)

    finally:
        driver.quit()

    # Write CSV
    fieldnames = [
        "suburb", "property_type", "street_address", "title",
        "price_min", "price_max", "bedrooms", "bathrooms", "parking_spaces",
        "land_size_sqm", "internal_size_sqm", "renovation_status",
        "development_zone", "real_estate_agency", "sales_agent",
        "agent_phone", "listing_url_domain", "listing_url_rea",
        "listing_status", "scraped_date",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_listings)

    print(f"\n{'='*60}")
    print(f"Done. {len(all_listings)} listings written to {OUTPUT_FILE}")
    print(f"Review CSV, fill blank columns, then ingest to Neon.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()