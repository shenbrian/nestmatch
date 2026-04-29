"""
agency_targets.py
-----------------
Track B: Agency mailing list subscription targets.

This is the master list of agencies to subscribe to, organised by corridor.
Each entry defines: agency name, suburb/office, subscription URL or email,
and which nester(s) should subscribe.

Philosophy:
  - One nester subscribes to one agency office (not the whole network).
    Ray White Cremorne and Ray White Neutral Bay are treated as separate
    offices with different agents and listing feeds.
  - Each nester subscribes to 2-3 agencies max — enough to generate
    meaningful volume without looking like bulk registration.
  - Subscriptions are one-time manual actions (nester visits agency website,
    registers as a buyer, or emails the office to be added to their list).
  - The email poller then captures all inbound listing alerts automatically.

Status values:
  'pending'     — not yet subscribed
  'subscribed'  — nester has registered, receiving emails
  'confirmed'   — first listing alert received and ingested
  'paused'      — temporarily paused (e.g. nester refresh needed)

This file doubles as the operational checklist for Track B rollout.
"""

AGENCY_TARGETS = {

    # -----------------------------------------------------------------------
    # LOWER NORTH SHORE
    # Nesters: N01-N03 (assign per property_type: N01 apt, N02 house, N03 terrace)
    # -----------------------------------------------------------------------
    "lower_north_shore": [
        {
            "agency": "McGrath Estate Agents",
            "office": "Cremorne / Mosman",
            "subscription_url": "https://www.mcgrath.com.au/buy/register-your-interest",
            "subscription_email": "cremorne@mcgrath.com.au",
            "nester_id": "N01",
            "status": "pending",
            "notes": "Dominant lower NS agency. High volume of apartment listings.",
        },
        {
            "agency": "Belle Property",
            "office": "Neutral Bay",
            "subscription_url": "https://www.belleproperty.com/neutral-bay/contact",
            "subscription_email": None,
            "nester_id": "N02",
            "status": "pending",
            "notes": "Strong for houses. Register as active buyer.",
        },
        {
            "agency": "Ray White",
            "office": "Mosman",
            "subscription_url": "https://www.raywhite.com/offices/ray-white-mosman",
            "subscription_email": "mosman@raywhite.com.au",
            "nester_id": "N03",
            "status": "pending",
            "notes": "Large network, regular new listing alerts via email.",
        },
        {
            "agency": "Stone Real Estate",
            "office": "North Shore",
            "subscription_url": "https://www.stonerealestaste.com.au",
            "subscription_email": None,
            "nester_id": "N01",
            "status": "pending",
            "notes": "Growing presence in LNS. Good for apartments.",
        },
    ],

    # -----------------------------------------------------------------------
    # UPPER NORTH SHORE
    # Nesters: N04-N06
    # -----------------------------------------------------------------------
    "upper_north_shore": [
        {
            "agency": "Richardson & Wrench",
            "office": "Gordon / Killara",
            "subscription_url": "https://rw.com.au/gordon",
            "subscription_email": None,
            "nester_id": "N04",
            "status": "pending",
            "notes": "Long-established UNS agency. Houses dominate.",
        },
        {
            "agency": "LJ Hooker",
            "office": "Chatswood",
            "subscription_url": "https://www.ljhooker.com.au/chatswood",
            "subscription_email": "chatswood@ljhooker.com.au",
            "nester_id": "N05",
            "status": "pending",
            "notes": "Apartment focus. Chatswood has high turnover.",
        },
        {
            "agency": "McGrath Estate Agents",
            "office": "Gordon",
            "subscription_url": "https://www.mcgrath.com.au/buy/register-your-interest",
            "subscription_email": "gordon@mcgrath.com.au",
            "nester_id": "N06",
            "status": "pending",
            "notes": "Strong for family homes. Buyer registration form on website.",
        },
    ],

    # -----------------------------------------------------------------------
    # INNER WEST
    # Nesters: N07-N09
    # -----------------------------------------------------------------------
    "inner_west": [
        {
            "agency": "BresicWhitney",
            "office": "Balmain / Inner West",
            "subscription_url": "https://www.bresicwhitney.com.au/contact",
            "subscription_email": None,
            "nester_id": "N07",
            "status": "pending",
            "notes": "Inner West specialist. High volume, strong agent relationships.",
        },
        {
            "agency": "Ray White",
            "office": "Newtown",
            "subscription_url": "https://www.raywhite.com/offices/ray-white-newtown",
            "subscription_email": "newtown@raywhite.com.au",
            "nester_id": "N08",
            "status": "pending",
            "notes": "Good for terraces and semis. Email list well-maintained.",
        },
        {
            "agency": "Cobden & Hayson",
            "office": "Annandale / Leichhardt",
            "subscription_url": "https://www.cobdenandhayson.com.au",
            "subscription_email": None,
            "nester_id": "N09",
            "status": "pending",
            "notes": "Inner West boutique. Pre-market notifications common.",
        },
    ],

    # -----------------------------------------------------------------------
    # EASTERN SUBURBS
    # Nesters: N10-N12
    # -----------------------------------------------------------------------
    "eastern_suburbs": [
        {
            "agency": "McGrath Estate Agents",
            "office": "Edgecliff / Double Bay",
            "subscription_url": "https://www.mcgrath.com.au/buy/register-your-interest",
            "subscription_email": "doublebay@mcgrath.com.au",
            "nester_id": "N10",
            "status": "pending",
            "notes": "Premium eastern suburbs. Apartment and terrace focus.",
        },
        {
            "agency": "Ray White",
            "office": "Bondi",
            "subscription_url": "https://www.raywhite.com/offices/ray-white-bondi-junction",
            "subscription_email": None,
            "nester_id": "N11",
            "status": "pending",
            "notes": "High volume Bondi area. New listing alerts frequent.",
        },
        {
            "agency": "NG Farah",
            "office": "Randwick / Coogee",
            "subscription_url": "https://www.ngfarah.com.au",
            "subscription_email": "info@ngfarah.com.au",
            "nester_id": "N12",
            "status": "pending",
            "notes": "South-east specialist. FHB and investor mix.",
        },
    ],

    # -----------------------------------------------------------------------
    # PARRAMATTA / WEST
    # Nesters: N13-N15
    # -----------------------------------------------------------------------
    "parramatta_west": [
        {
            "agency": "Ray White",
            "office": "Parramatta",
            "subscription_url": "https://www.raywhite.com/offices/ray-white-parramatta",
            "subscription_email": "parramatta@raywhite.com.au",
            "nester_id": "N13",
            "status": "pending",
            "notes": "Largest Parramatta agency. Strong investor segment.",
        },
        {
            "agency": "LJ Hooker",
            "office": "Strathfield",
            "subscription_url": "https://www.ljhooker.com.au/strathfield",
            "subscription_email": None,
            "nester_id": "N14",
            "status": "pending",
            "notes": "Strathfield/Burwood corridor. Multicultural buyer segment.",
        },
        {
            "agency": "McGrath Estate Agents",
            "office": "Parramatta",
            "subscription_url": "https://www.mcgrath.com.au/buy/register-your-interest",
            "subscription_email": "parramatta@mcgrath.com.au",
            "nester_id": "N15",
            "status": "pending",
            "notes": "Growing McGrath presence west of CBD.",
        },
    ],

    # -----------------------------------------------------------------------
    # HILLS DISTRICT
    # Nesters: N16-N18
    # -----------------------------------------------------------------------
    "hills_district": [
        {
            "agency": "Professionals",
            "office": "Castle Hill",
            "subscription_url": "https://www.professionals.com.au/castle-hill",
            "subscription_email": None,
            "nester_id": "N16",
            "status": "pending",
            "notes": "Strong Hills agency. Houses dominate. Good family home stock.",
        },
        {
            "agency": "Ray White",
            "office": "Baulkham Hills",
            "subscription_url": "https://www.raywhite.com/offices/ray-white-baulkham-hills",
            "subscription_email": "baulkhamhills@raywhite.com.au",
            "nester_id": "N17",
            "status": "pending",
            "notes": "Large Hills office. Regular listing alert emails.",
        },
        {
            "agency": "Raine & Horne",
            "office": "Castle Hill / Norwest",
            "subscription_url": "https://www.raineandhorne.com.au/castle-hill",
            "subscription_email": None,
            "nester_id": "N18",
            "status": "pending",
            "notes": "Norwest corridor growing. Good for new builds and townhouses.",
        },
    ],

    # -----------------------------------------------------------------------
    # ST GEORGE / SOUTH
    # Nesters: N19-N21
    # -----------------------------------------------------------------------
    "st_george_south": [
        {
            "agency": "Ray White",
            "office": "Hurstville",
            "subscription_url": "https://www.raywhite.com/offices/ray-white-hurstville",
            "subscription_email": "hurstville@raywhite.com.au",
            "nester_id": "N19",
            "status": "pending",
            "notes": "St George dominant. High apartment turnover.",
        },
        {
            "agency": "McGrath Estate Agents",
            "office": "St George / Kogarah",
            "subscription_url": "https://www.mcgrath.com.au/buy/register-your-interest",
            "subscription_email": "kogarah@mcgrath.com.au",
            "nester_id": "N20",
            "status": "pending",
            "notes": "Growing McGrath presence in St George corridor.",
        },
        {
            "agency": "Trow Jones",
            "office": "Cronulla / Sutherland",
            "subscription_url": "https://www.trowjones.com.au",
            "subscription_email": "info@trowjones.com.au",
            "nester_id": "N21",
            "status": "pending",
            "notes": "Sutherland Shire specialist. Beachside suburb focus.",
        },
    ],

    # -----------------------------------------------------------------------
    # NORTHERN BEACHES
    # Nesters: N22-N24
    # -----------------------------------------------------------------------
    "northern_beaches": [
        {
            "agency": "LJ Hooker",
            "office": "Manly",
            "subscription_url": "https://www.ljhooker.com.au/manly",
            "subscription_email": "manly@ljhooker.com.au",
            "nester_id": "N22",
            "status": "pending",
            "notes": "Manly and surrounds. Strong apartment and house mix.",
        },
        {
            "agency": "Cunninghams Real Estate",
            "office": "Dee Why / Collaroy",
            "subscription_url": "https://www.cunninghams.com.au",
            "subscription_email": None,
            "nester_id": "N23",
            "status": "pending",
            "notes": "Northern Beaches specialist. Long-term agency with loyal agent base.",
        },
        {
            "agency": "Belle Property",
            "office": "Manly / Northern Beaches",
            "subscription_url": "https://www.belleproperty.com/northern-beaches/contact",
            "subscription_email": None,
            "nester_id": "N24",
            "status": "pending",
            "notes": "Premium northern beaches. Good for upsizers and downsizers.",
        },
    ],
}


def get_pending_subscriptions() -> list[dict]:
    """Return all agency targets not yet subscribed, with corridor context."""
    pending = []
    for corridor, agencies in AGENCY_TARGETS.items():
        for agency in agencies:
            if agency["status"] == "pending":
                pending.append({**agency, "corridor": corridor})
    return pending


def get_subscriptions_for_nester(nester_id: str) -> list[dict]:
    """Return all agency targets assigned to a given nester."""
    results = []
    for corridor, agencies in AGENCY_TARGETS.items():
        for agency in agencies:
            if agency["nester_id"] == nester_id:
                results.append({**agency, "corridor": corridor})
    return results


if __name__ == "__main__":
    pending = get_pending_subscriptions()
    print(f"\n--- Agency subscription targets ---")
    print(f"Total pending: {len(pending)}\n")
    for p in pending:
        url = p["subscription_url"] or f"email: {p['subscription_email']}"
        print(f"  [{p['nester_id']}] {p['agency']:30s} {p['office']:30s} → {url[:60]}")
    print()
