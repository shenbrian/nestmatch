"""
bridge_agent_replies.py - Session 29
Reads agent_replies, upserts into properties with listing_status = 'agent_sourced'.
Idempotent: skips rows where street address already exists in properties.
"""
import asyncio
import asyncpg
import os
import re

DATABASE_URL = os.environ["DATABASE_URL"]

NESTER_PROFILES = {
    "N01": {"bedrooms": 2, "property_type": "apartment"},
    "N02": {"bedrooms": 4, "property_type": "house"},
    "N03": {"bedrooms": 3, "property_type": "apartment"},
    "N04": {"bedrooms": 4, "property_type": "house"},
    "N05": {"bedrooms": 4, "property_type": "house"},
    "N06": {"bedrooms": 3, "property_type": "house"},
    "N07": {"bedrooms": 2, "property_type": "apartment"},
    "N08": {"bedrooms": 3, "property_type": "house"},
    "N09": {"bedrooms": 3, "property_type": "house"},
}

INSERT_SQL = (
    "INSERT INTO properties "
    "(suburb, street_address, property_type, bedrooms, "
    " price_min, price_max, sales_agent, agent_phone, "
    " real_estate_agency, listing_status) "
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'agent_sourced')"
)


def parse_price(price_guide):
    if not price_guide:
        return None
    s = price_guide.upper().replace(",", "").replace(" ", "")
    m = re.search(r"([\d.]+)(M|K)?", s)
    if not m:
        return None
    val = float(m.group(1))
    suffix = m.group(2)
    if suffix == "M":
        val *= 1_000_000
    elif suffix == "K":
        val *= 1_000
    return int(val)


def extract_suburb(address, suburb_field):
    if suburb_field:
        s = re.sub(
            r"\s+(NSW|VIC|QLD|WA|SA|TAS|ACT|NT).*$",
            "",
            suburb_field,
            flags=re.IGNORECASE,
        ).strip()
        return s.title()
    parts = address.split(",")
    if len(parts) >= 2:
        return parts[1].strip().title()
    return ""


async def main():
    conn = await asyncpg.connect(DATABASE_URL)

    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (property_address)
            nester_id, agent_name, agent_phone,
            property_address, suburb, property_type, price_guide, agency
        FROM agent_replies
        WHERE property_address IS NOT NULL
          AND nester_id IS NOT NULL
        ORDER BY property_address, received_at DESC
        """
    )

    existing = await conn.fetch(
        "SELECT LOWER(SPLIT_PART(street_address, ',', 1)) AS addr "
        "FROM properties WHERE street_address IS NOT NULL"
    )
    existing_addrs = {r["addr"] for r in existing}

    inserted = 0
    skipped = 0

    for row in rows:
        addr = row["property_address"].strip()
        addr_key = addr.lower().split(",")[0].strip()

        if addr_key in existing_addrs:
            skipped += 1
            continue

        profile = NESTER_PROFILES.get(row["nester_id"] or "", {})
        bedrooms = profile.get("bedrooms", 2)
        prop_type = row["property_type"] or profile.get("property_type", "apartment")
        suburb = extract_suburb(addr, row["suburb"] or "")

        if not suburb:
            print("SKIPPED (no suburb):", addr)
            skipped += 1
            continue

        price = parse_price(row["price_guide"] or "")
        price_max = price or 0
        price_min = int(price_max * 0.9) if price_max else 0

        await conn.execute(
            INSERT_SQL,
            suburb,
            addr,
            prop_type,
            bedrooms,
            price_min,
            price_max,
            row["agent_name"],
            row["agent_phone"],
            row["agency"],
        )
        existing_addrs.add(addr_key)
        inserted += 1
        print("INSERTED:", addr, "|", suburb, str(bedrooms) + "br", prop_type)

    print("\nDone.", inserted, "inserted,", skipped, "skipped.")
    await conn.close()


asyncio.run(main())