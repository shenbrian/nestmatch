"""
nester_router.py
----------------
Routes a property (identified by suburb) to the correct nester(s).

Two functions:
  get_nesters_for_suburb(suburb, property_type=None) -> list[dict]
      Returns all nesters whose corridor covers that suburb.
      Optionally filters by property_type match.

  pick_nester(suburb, property_type=None, exclude_ids=None) -> dict | None
      Returns a single nester for a send, rotating fairly across eligible
      nesters and respecting the exclusion list (e.g. nesters who already
      sent to this agent today).

Usage:
  from nester_router import pick_nester
  nester = pick_nester(suburb="Cremorne", property_type="apartment")
"""

import json
import os
import random
from pathlib import Path

# ---------------------------------------------------------------------------
# Load personas once at import time
# ---------------------------------------------------------------------------
_PERSONAS_PATH = Path(__file__).parent.parent / "personas.json"

def _load_personas() -> dict:
    with open(_PERSONAS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

PERSONAS: dict = _load_personas()

# ---------------------------------------------------------------------------
# Suburb → corridor map
# Each entry: suburb (lowercase) → corridor label (must match personas.json)
# Covers all 8 corridors. Extend this list as new suburbs enter the DB.
# ---------------------------------------------------------------------------
SUBURB_CORRIDOR_MAP: dict[str, str] = {
    # Lower North Shore
    "cremorne":         "lower north shore",
    "mosman":           "lower north shore",
    "neutral bay":      "lower north shore",
    "kirribilli":       "lower north shore",
    "mcmahons point":   "lower north shore",
    "lavender bay":     "lower north shore",
    "milsons point":    "lower north shore",
    "north sydney":     "lower north shore",
    "waverton":         "lower north shore",
    "crows nest":       "lower north shore",
    "st leonards":      "lower north shore",
    "wollstonecraft":   "lower north shore",

    # Upper North Shore
    "chatswood":        "upper north shore",
    "gordon":           "upper north shore",
    "killara":          "upper north shore",
    "lindfield":        "upper north shore",
    "pymble":           "upper north shore",
    "turramurra":       "upper north shore",
    "wahroonga":        "upper north shore",
    "hornsby":          "upper north shore",
    "west pennant hills":"upper north shore",
    "pennant hills":    "upper north shore",
    "beecroft":         "upper north shore",
    "cheltenham":       "upper north shore",

    # Inner West
    "balmain":          "inner west",
    "rozelle":          "inner west",
    "leichhardt":       "inner west",
    "annandale":        "inner west",
    "glebe":            "inner west",
    "newtown":          "inner west",
    "marrickville":     "inner west",
    "dulwich hill":     "inner west",
    "petersham":        "inner west",
    "stanmore":         "inner west",
    "enmore":           "inner west",
    "tempe":            "inner west",
    "sydenham":         "inner west",

    # Eastern Suburbs
    "bondi":            "eastern suburbs",
    "bondi beach":      "eastern suburbs",
    "bondi junction":   "eastern suburbs",
    "double bay":       "eastern suburbs",
    "edgecliff":        "eastern suburbs",
    "woollahra":        "eastern suburbs",
    "paddington":       "eastern suburbs",
    "surry hills":      "eastern suburbs",
    "darlinghurst":     "eastern suburbs",
    "potts point":      "eastern suburbs",
    "elizabeth bay":    "eastern suburbs",
    "coogee":           "eastern suburbs",
    "randwick":         "eastern suburbs",
    "kingsford":        "eastern suburbs",
    "maroubra":         "eastern suburbs",

    # Parramatta / West
    "parramatta":       "parramatta / west",
    "westmead":         "parramatta / west",
    "merrylands":       "parramatta / west",
    "granville":        "parramatta / west",
    "auburn":           "parramatta / west",
    "lidcombe":         "parramatta / west",
    "strathfield":      "parramatta / west",
    "homebush":         "parramatta / west",
    "burwood":          "parramatta / west",
    "concord":          "parramatta / west",
    "rhodes":           "parramatta / west",
    "meadowbank":       "parramatta / west",
    "ryde":             "parramatta / west",
    "ermington":        "parramatta / west",

    # Hills District
    "castle hill":      "hills district",
    "baulkham hills":   "hills district",
    "norwest":          "hills district",
    "kellyville":       "hills district",
    "rouse hill":       "hills district",
    "beaumont hills":   "hills district",
    "glenhaven":        "hills district",
    "dural":            "hills district",
    "cherrybrook":      "hills district",
    "west pennant hills":"hills district",  # also upper north shore — corridor overlap
    "north rocks":      "hills district",

    # St George / South
    "hurstville":       "st george / south",
    "kogarah":          "st george / south",
    "rockdale":         "st george / south",
    "brighton-le-sands":"st george / south",
    "sans souci":       "st george / south",
    "ramsgate":         "st george / south",
    "blakehurst":       "st george / south",
    "connells point":   "st george / south",
    "kyle bay":         "st george / south",
    "mortdale":         "st george / south",
    "penshurst":        "st george / south",
    "beverly hills":    "st george / south",
    "narwee":           "st george / south",
    "cronulla":         "st george / south",
    "caringbah":        "st george / south",
    "sutherland":       "st george / south",
    "miranda":          "st george / south",

    # Northern Beaches
    "manly":            "northern beaches",
    "dee why":          "northern beaches",
    "collaroy":         "northern beaches",
    "narrabeen":        "northern beaches",
    "mona vale":        "northern beaches",
    "newport":          "northern beaches",
    "avalon beach":     "northern beaches",
    "palm beach":       "northern beaches",
    "freshwater":       "northern beaches",
    "curl curl":        "northern beaches",
    "brookvale":        "northern beaches",
    "warriewood":       "northern beaches",
}


def get_corridor_for_suburb(suburb: str) -> str | None:
    """Return the corridor name for a suburb, or None if unmapped."""
    return SUBURB_CORRIDOR_MAP.get(suburb.strip().lower())


def get_nesters_for_suburb(
    suburb: str,
    property_type: str | None = None,
) -> list[dict]:
    """
    Return all nesters whose corridor covers the given suburb.
    If property_type is provided, prefer nesters whose property_type matches
    but still return all corridor matches (caller can filter further).
    Results are sorted: matching property_type first, then others.
    """
    corridor = get_corridor_for_suburb(suburb)
    if not corridor:
        return []

    matches = []
    for nester_id, nester in PERSONAS.items():
        if nester.get("corridor", "").lower() == corridor.lower():
            nester_with_id = {**nester, "nester_id": nester_id}
            matches.append(nester_with_id)

    if property_type and matches:
        pt = property_type.lower()
        primary = [n for n in matches if n.get("property_type", "").lower() == pt]
        secondary = [n for n in matches if n.get("property_type", "").lower() != pt]
        matches = primary + secondary

    return matches


def pick_nester(
    suburb: str,
    property_type: str | None = None,
    exclude_ids: list[str] | None = None,
) -> dict | None:
    """
    Pick a single nester for a send.

    Selection logic:
      1. Get all nesters for the suburb's corridor
      2. Filter out excluded IDs (e.g. already sent today, D74 same-agency rule)
      3. Prefer property_type match
      4. Among eligible, pick randomly (avoids mechanical patterns)

    Returns the nester dict (with nester_id key added), or None if no
    eligible nester exists.
    """
    exclude_ids = exclude_ids or []
    candidates = get_nesters_for_suburb(suburb, property_type)

    # Remove excluded
    candidates = [n for n in candidates if n["nester_id"] not in exclude_ids]

    if not candidates:
        return None

    # Prefer property_type match if any exist
    if property_type:
        pt = property_type.lower()
        preferred = [n for n in candidates if n.get("property_type", "").lower() == pt]
        pool = preferred if preferred else candidates
    else:
        pool = candidates

    return random.choice(pool)


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_cases = [
        ("Cremorne", "apartment"),
        ("Bondi Beach", "house"),
        ("Parramatta", "apartment"),
        ("Castle Hill", "house"),
        ("Newtown", "terrace"),
        ("Unknown Suburb", None),
    ]
    print("\n--- nester_router smoke test ---\n")
    for suburb, pt in test_cases:
        nester = pick_nester(suburb, pt)
        if nester:
            print(f"  {suburb:20s} ({pt or 'any':10s}) → {nester['nester_id']} {nester['full_name']} [{nester['corridor']}]")
        else:
            print(f"  {suburb:20s} ({pt or 'any':10s}) → NO MATCH")
    print()
