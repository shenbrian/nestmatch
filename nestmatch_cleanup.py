"""
NestMatch — Pilot Listings CSV Cleanup
Session 14

Fixes:
1. Splits concatenated beds/baths/parking/land_size column
2. Removes rows with junk prices (422973729 = DOM element ID)
3. Removes rows with no real street address (suburb-name only)
4. Flags Frenchs Forest rows with Dee Why addresses
5. Re-fetches Domain listing URLs using Selenium
6. Outputs clean CSV ready for Neon ingest

Usage (in VS Code terminal):
    python nestmatch_cleanup.py
"""

import csv
import re
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

INPUT_FILE  = "nestmatch_pilot_listings.csv"
OUTPUT_FILE = "nestmatch_pilot_listings_clean.csv"

JUNK_PRICE  = 422973729  # DOM element ID mistaken for price
SYDNEY_SUBURBS = {s.lower() for s in [
    "Newtown","Mosman","Chatswood","Marrickville","Leichhardt","Randwick",
    "Maroubra","Paddington","Redfern","Rose Bay","Parramatta","Blacktown",
    "Epping","Frenchs Forest","Ryde","West Pymble","Hornsby","Strathfield",
]}


# ── Parser ────────────────────────────────────────────────────────────────────

def parse_bbpl(val):
    """Split concatenated beds/baths/parking/land string into components."""
    if not val or not str(val).strip():
        return None, None, None, None
    s = str(val).strip()
    if len(s) < 2:
        return None, None, None, None

    beds  = int(s[0])
    baths = int(s[1])
    rest  = s[2:]

    if not rest:
        return beds, baths, None, None
    if len(rest) == 1:
        return beds, baths, int(rest), None
    if len(rest) == 2:
        return beds, baths, int(rest[0]), None
    if len(rest) == 3:
        return beds, baths, None, int(rest)
    # len >= 4: first digit = parking, remainder = land
    return beds, baths, int(rest[0]), int(rest[1:])


def is_real_address(address, suburb):
    """Return False if address is just a suburb name or project name only."""
    if not address:
        return False
    addr_lower = address.strip().lower()
    # Pure suburb name
    if addr_lower == suburb.lower():
        return False
    # Very short (< 6 chars) — likely junk
    if len(address.strip()) < 6:
        return False
    return True


# ── URL fetcher ───────────────────────────────────────────────────────────────

def make_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def fetch_listing_url(driver, address, suburb, ptype):
    """Search Domain for a specific address and return the listing URL."""
    query = f"{address} {suburb}"
    search_url = f"https://www.domain.com.au/sale/?q={query.replace(' ', '+')}"
    try:
        driver.get(search_url)
        WebDriverWait(driver, 12).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "article, [data-testid*='listing']")
            )
        )
        time.sleep(random.uniform(1.5, 2.5))

        # Find first listing link
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='property-']")
        for link in links:
            href = link.get_attribute("href") or ""
            if "domain.com.au" in href and "property-" in href:
                return href
    except Exception as e:
        print(f"    URL fetch error for '{address}': {e}")
    return ""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # ── Step 1: Load and clean CSV ────────────────────────────────────────────
    print("Loading CSV...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    print(f"  {len(raw_rows)} rows loaded")

    clean_rows = []
    skipped = []

    for row in raw_rows:
        suburb    = row["suburb"]
        ptype     = row["property_type"]
        address   = row["street_address"]
        price_min = row["price_min"]
        price_max = row["price_max"]
        bbpl_raw  = row["bedrooms"]  # concatenated field

        # Skip junk prices
        try:
            if int(float(price_min)) == JUNK_PRICE:
                skipped.append((address, "junk price"))
                continue
        except:
            pass

        # Skip non-addresses
        if not is_real_address(address, suburb):
            skipped.append((address, "no real address"))
            continue

        # Parse beds/baths/parking/land
        beds, baths, parking, land = parse_bbpl(bbpl_raw)

        # Flag suburb mismatch (e.g. Frenchs Forest showing Dee Why)
        notes = ""
        if suburb == "Frenchs Forest" and "dee why" in address.lower():
            notes = "ADDRESS IN DEE WHY — verify suburb"

        clean_rows.append({
            "suburb":            suburb,
            "property_type":     ptype,
            "street_address":    address,
            "title":             row.get("title", ""),
            "price_min":         price_min,
            "price_max":         price_max,
            "bedrooms":          beds if beds is not None else "",
            "bathrooms":         baths if baths is not None else "",
            "parking_spaces":    parking if parking is not None else "",
            "land_size_sqm":     land if land is not None else "",
            "internal_size_sqm": row.get("internal_size_sqm", ""),
            "renovation_status": row.get("renovation_status", ""),
            "development_zone":  row.get("development_zone", ""),
            "real_estate_agency":row.get("real_estate_agency", ""),
            "sales_agent":       row.get("sales_agent", ""),
            "agent_phone":       row.get("agent_phone", ""),
            "listing_url_domain":"",   # to be filled by Step 2
            "listing_url_rea":   "",
            "listing_status":    "for_sale",
            "scraped_date":      row.get("scraped_date", ""),
            "notes":             notes,
        })

    print(f"\nAfter cleaning:")
    print(f"  Kept:    {len(clean_rows)} rows")
    print(f"  Skipped: {len(skipped)} rows")
    for addr, reason in skipped:
        print(f"    - '{addr}' ({reason})")

    # ── Step 2: Fetch listing URLs ────────────────────────────────────────────
    print(f"\nFetching listing URLs for {len(clean_rows)} rows...")
    print("Chrome will open — don't close it.\n")

    driver = make_driver()
    try:
        # Warm up
        driver.get("https://www.domain.com.au")
        time.sleep(random.uniform(3.0, 4.0))

        for i, row in enumerate(clean_rows):
            address = row["street_address"]
            suburb  = row["suburb"]
            ptype   = row["property_type"]
            print(f"  [{i+1}/{len(clean_rows)}] {address}, {suburb}")

            url = fetch_listing_url(driver, address, suburb, ptype)
            row["listing_url_domain"] = url
            if url:
                print(f"    ✓ {url[:80]}...")
            else:
                print(f"    ✗ not found")

            time.sleep(random.uniform(2.0, 3.5))

    finally:
        driver.quit()

    # ── Step 3: Write clean CSV ───────────────────────────────────────────────
    fieldnames = [
        "suburb", "property_type", "street_address", "title",
        "price_min", "price_max", "bedrooms", "bathrooms", "parking_spaces",
        "land_size_sqm", "internal_size_sqm", "renovation_status",
        "development_zone", "real_estate_agency", "sales_agent",
        "agent_phone", "listing_url_domain", "listing_url_rea",
        "listing_status", "scraped_date", "notes",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)

    urls_found = sum(1 for r in clean_rows if r["listing_url_domain"])
    print(f"\n{'='*60}")
    print(f"Done. {len(clean_rows)} clean rows written to {OUTPUT_FILE}")
    print(f"URLs fetched: {urls_found}/{len(clean_rows)}")
    print(f"Review 'notes' column for any flagged rows.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
