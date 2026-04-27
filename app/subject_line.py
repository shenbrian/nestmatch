"""
subject_line.py
---------------
Generates subject lines for nester enquiry emails that look like
genuine buyer enquiries — not portal forwards, not templates.

Design principles:
  1. Never use "Re:" — that implies a reply to something, which is odd for
     a cold enquiry. REA/Domain portal enquiries don't use Re:.
  2. Never use the full street number + street + suburb as a rigid template —
     it looks automated when every email follows the same pattern.
  3. Vary the phrasing naturally across a small set of real patterns that
     genuine buyers use when emailing agents directly.
  4. Keep it short — agents read subject lines in a mobile notification.
     Aim for under 60 characters.
  5. The property address must appear so the agent can locate it quickly.
     Suburb alone is not enough. Full address preferred, unit/street minimum.

Pattern bank (rotate randomly):
  A. "Enquiry – [address]"                         ← clean, direct
  B. "Question about [address]"                    ← slightly warmer
  C. "[address] – quick question"                  ← casual
  D. "Interested in [address]"                     ← buyer intent signal
  E. "[address]"                                   ← bare address, very natural
  F. "Following up on [address]"                   ← implies prior awareness

Pattern F ("following up") is used only on scheduled/warm sends — never
on first-contact demand sends — because it implies prior contact.

For agency subscription outreach (Track B), subject lines are different:
  "New to the area – keen to hear about upcoming listings"
  "Looking for [property_type] in [suburb/corridor]"
  "Buyer registration – [corridor] area"
"""

import random

# ---------------------------------------------------------------------------
# Pattern sets
# ---------------------------------------------------------------------------

# Cold / first-contact patterns (demand trigger, first send to this agent)
COLD_PATTERNS = [
    "Enquiry – {address}",
    "Question about {address}",
    "{address} – quick question",
    "Interested in {address}",
    "{address}",
]

# Warm patterns (scheduled trigger, agent already replied before)
WARM_PATTERNS = [
    "Following up on {address}",
    "Enquiry – {address}",
    "{address} – still interested",
    "Update on {address}?",
]

# Track B: agency subscription outreach subject lines
# Keyed by buyer_type for persona consistency
SUBSCRIPTION_SUBJECTS = {
    "fhb": [
        "First home buyer looking in {corridor}",
        "Keen to hear about listings in {area}",
        "Buyer registration – {area}",
        "Looking for my first home in {area}",
    ],
    "upsizer": [
        "Upsizing – interested in {area} listings",
        "Looking for larger home in {area}",
        "Buyer enquiry – {area}",
        "Keen to hear about new listings in {area}",
    ],
    "investor": [
        "Investment buyer – {area} properties",
        "Investor looking in {area}",
        "Buyer registration – {area}",
        "Interested in investment opportunities in {area}",
    ],
    "downsizer": [
        "Downsizing to {area}",
        "Looking for smaller property in {area}",
        "Buyer enquiry – {area}",
        "Keen to hear about listings in {area}",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trim_address(address: str) -> str:
    """
    Shorten a full address for use in a subject line.
    '16/74-76 Upper Pitt Street, Kirribilli NSW 2061' → '16/74-76 Upper Pitt St, Kirribilli'
    Removes state + postcode suffix, abbreviates Street/Road/Avenue.
    """
    # Strip state + postcode at end (', NSW 2XXX' or similar)
    import re
    address = re.sub(r",?\s+[A-Z]{2,3}\s+\d{4}\s*$", "", address.strip())
    # Abbreviate common street types
    replacements = {
        " Street": " St",
        " Avenue": " Ave",
        " Road": " Rd",
        " Drive": " Dr",
        " Place": " Pl",
        " Crescent": " Cres",
        " Boulevard": " Blvd",
        " Parade": " Pde",
        " Terrace": " Tce",
        " Highway": " Hwy",
    }
    for full, abbr in replacements.items():
        address = address.replace(full, abbr)
    return address


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_subject(
    property_address: str,
    agency_name: str | None = None,
    is_warm: bool = False,
) -> str:
    """
    Build a subject line for a portal enquiry email.

    Args:
        property_address: Full address of the property.
        agency_name: Optional. Not currently used in subject but reserved
                     for future personalisation (e.g. large agency flag).
        is_warm: True if this is a follow-up send to an agent who has
                 already replied. Enables WARM_PATTERNS.

    Returns:
        A subject line string, ready to use.
    """
    short_address = _trim_address(property_address)
    patterns = WARM_PATTERNS if is_warm else COLD_PATTERNS
    template = random.choice(patterns)
    return template.format(address=short_address)


def build_subscription_subject(
    buyer_type: str,
    area: str,
    corridor: str | None = None,
) -> str:
    """
    Build a subject line for Track B agency subscription outreach.

    Args:
        buyer_type: 'fhb' | 'upsizer' | 'investor' | 'downsizer'
        area: Suburb or loose area name (e.g. 'Cremorne', 'Lower North Shore')
        corridor: Optional corridor label for more specific subjects.

    Returns:
        A subject line string.
    """
    bt = buyer_type.lower().replace("-", "").replace(" ", "")
    # Normalise buyer_type keys
    if bt in ("firsthomebuyer", "firsthome", "fhb"):
        bt = "fhb"
    elif bt in ("upsizer", "upgrader"):
        bt = "upsizer"
    elif bt in ("investor", "investment"):
        bt = "investor"
    elif bt in ("downsizer",):
        bt = "downsizer"
    else:
        bt = "upsizer"  # safe default

    patterns = SUBSCRIPTION_SUBJECTS.get(bt, SUBSCRIPTION_SUBJECTS["upsizer"])
    template = random.choice(patterns)
    return template.format(area=area, corridor=corridor or area)


# ---------------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("\n--- subject_line demo ---\n")

    addresses = [
        "16/74-76 Upper Pitt Street, Kirribilli NSW 2061",
        "42 Cremorne Road, Cremorne NSW 2090",
        "3/18 Military Road, Neutral Bay NSW 2089",
        "88 Bourke Street, Surry Hills NSW 2010",
    ]

    print("Cold enquiry subjects:")
    for addr in addresses:
        print(f"  {build_subject(addr)}")

    print("\nWarm follow-up subjects:")
    for addr in addresses[:2]:
        print(f"  {build_subject(addr, is_warm=True)}")

    print("\nSubscription outreach subjects:")
    cases = [
        ("fhb", "Parramatta"),
        ("investor", "Lower North Shore"),
        ("downsizer", "Mosman"),
        ("upsizer", "Castle Hill"),
    ]
    for bt, area in cases:
        print(f"  [{bt}] {build_subscription_subject(bt, area)}")

    print()
