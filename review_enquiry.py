"""
NestMatch — Nester Enquiry Review Script
review_enquiry.py  (run from repo root: python review_enquiry.py)

Generates a draft enquiry email for a given property + nester,
displays it for review, and lets you approve or regenerate.

Usage:
  python review_enquiry.py

Requires ANTHROPIC_API_KEY in environment.
Windows PowerShell: $env:ANTHROPIC_API_KEY = 'your-key-here'
"""

import asyncio
import sys
import os

# Add app/ to path so we can import question_engine
sys.path.insert(0, os.path.dirname(__file__))
from app.question_engine import generate_enquiry, PERSONAS


def print_divider():
    print("\n" + "=" * 60 + "\n")


def print_result(result: dict):
    print_divider()
    print(f"NESTER:    {result['nester_id']} — {result['nester_name']}")
    print(f"EMAIL:     {result['nester_email']}")
    print(f"PROPERTY:  {result['property_address']}, {result['property_suburb']}")
    print(f"TERRITORIES TRIGGERED: {', '.join(result['territories_triggered'])}")
    print_divider()
    print("DRAFT EMAIL BODY:")
    print()
    print(result['email_body'])
    print_divider()


def get_nester_choice() -> str:
    print("Available nesters:")
    for nid, p in PERSONAS.items():
        print(f"  {nid}  {p['full_name']:<20}  {p['corridor']:<25}  {p['property_target']}")
    print()
    while True:
        choice = input("Enter nester ID (e.g. N01): ").strip().upper()
        if choice in PERSONAS:
            return choice
        print(f"  Not found. Choose from: {', '.join(PERSONAS.keys())}")


def get_property_data() -> dict:
    print("\nEnter property details (press Enter to skip optional fields):\n")

    address = input("  Address (e.g. 16/74-76 Upper Pitt Street): ").strip()
    suburb = input("  Suburb (e.g. Kirribilli): ").strip()
    property_type = input("  Property type (apartment / house / townhouse): ").strip() or "apartment"
    bedrooms_str = input("  Bedrooms (e.g. 2): ").strip()
    bedrooms = int(bedrooms_str) if bedrooms_str.isdigit() else None
    agent_name = input("  Agent name (optional): ").strip()
    agency_name = input("  Agency name (optional): ").strip()

    print()
    days_str = input("  Days on market (0 if unknown): ").strip()
    days_on_market = int(days_str) if days_str.isdigit() else 0

    building_age_str = input("  Building age in years (0 if unknown): ").strip()
    building_age = int(building_age_str) if building_age_str.isdigit() else 0

    auction_date = input("  Auction date YYYY-MM-DD (leave blank if none/unknown): ").strip() or None

    print()
    description = input("  Paste a snippet of listing description (optional, press Enter to skip):\n  ").strip()

    return {
        "address": address,
        "suburb": suburb,
        "property_type": property_type,
        "bedrooms": bedrooms,
        "agent_name": agent_name,
        "agency_name": agency_name,
        "days_on_market": days_on_market,
        "building_age_years": building_age,
        "auction_date": auction_date,
        "listing_description": description or None,
    }


async def main():
    print()
    print("=" * 60)
    print("  NestMatch — Nester Enquiry Review")
    print("=" * 60)

    # Check API key
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("\nERROR: ANTHROPIC_API_KEY not set.")
        print("In PowerShell: $env:ANTHROPIC_API_KEY = 'your-key-here'")
        sys.exit(1)

    nester_id = get_nester_choice()
    property_data = get_property_data()

    while True:
        print("\nGenerating enquiry...")
        try:
            result = await generate_enquiry(property_data, nester_id)
        except Exception as e:
            print(f"\nERROR generating enquiry: {e}")
            sys.exit(1)

        print_result(result)

        print("OPTIONS:")
        print("  y  — approve (copy this email and send it)")
        print("  r  — regenerate (get a different version)")
        print("  q  — quit without sending")
        print()
        choice = input("Your choice (y/r/q): ").strip().lower()

        if choice == "y":
            print()
            print("APPROVED. Copy and send the email above.")
            print()
            print(f"Send FROM: {result['nester_email']}")
            print(f"Subject suggestion: Re: [enquiry] {result['property_address']}")
            print()
            print("After sending, prepend [" + nester_id + "] to the subject")
            print("when you forward any agent reply to buyers@nestmatch.com.au")
            print()
            break
        elif choice == "r":
            print("\nRegenerating...")
            continue
        elif choice == "q":
            print("\nExited without sending.")
            break
        else:
            print("Please enter y, r, or q.")


if __name__ == "__main__":
    asyncio.run(main())


# ─── TEST DATA ─────────────────────────────────────────────────────────────
# To test without interactive prompts, comment out asyncio.run(main()) above
# and uncomment the block below:
#
# async def test():
#     property_data = {
#         "address": "16/74-76 Upper Pitt Street",
#         "suburb": "Kirribilli",
#         "property_type": "apartment",
#         "bedrooms": 2,
#         "agent_name": "Jonathon De Brennan",
#         "agency_name": "De Brennan",
#         "days_on_market": 0,
#         "building_age_years": 0,
#         "auction_date": None,
#         "listing_description": "Stunning Harbour Views. Few experiences compare to the privilege of arriving home to a panorama of uninterrupted splendour.",
#     }
#     result = await generate_enquiry(property_data, "N01")
#     print(result["email_body"])
#
# asyncio.run(test())
