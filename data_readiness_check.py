"""
NestMatch — Data Readiness Check
Session 12 · Agentic-ready

Queryable gate function for any downstream agent (e.g. PropTrack outreach agent).
Returns a structured readiness report: which pipelines are complete, what coverage
looks like, and whether the outreach trigger condition from D50 is met.

Usage:
    python data_readiness_check.py

    # Or import in another agent:
    from data_readiness_check import check_readiness
    result = check_readiness()
    if result["outreach_ready"]:
        fire_proptrack_outreach()
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set.")
    sys.exit(1)

# D50: outreach fires when GTFS + VG are both confirmed in production
OUTREACH_TRIGGER_PIPELINES = {"gtfs", "valuer_general"}


def check_readiness() -> dict:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    report = {
        "checked_at": datetime.now().isoformat(),
        "pipelines": {},
        "property_coverage": {},
        "outreach_ready": False,
        "outreach_blockers": [],
    }

    # 1. Pipeline run status
    cur.execute("""
        SELECT DISTINCT ON (pipeline)
            pipeline, status, rows_affected, notes, run_at
        FROM data_pipeline_runs
        ORDER BY pipeline, run_at DESC
    """)
    for row in cur.fetchall():
        report["pipelines"][row["pipeline"]] = {
            "status": row["status"],
            "rows_affected": row["rows_affected"],
            "notes": row["notes"],
            "run_at": row["run_at"].isoformat() if row["run_at"] else None,
        }

    # 2. Property coverage stats
    cur.execute("""
        SELECT
            COUNT(*)                                        AS total,
            COUNT(*) FILTER (WHERE commute_source = 'gtfs_auto') AS gtfs_auto,
            COUNT(*) FILTER (WHERE commute_source = 'manual')    AS commute_manual,
            COUNT(*) FILTER (WHERE land_to_asset_ratio IS NOT NULL) AS has_land_ratio,
            COUNT(*) FILTER (WHERE suburb_trajectory IS NOT NULL)   AS has_trajectory,
            COUNT(*) FILTER (WHERE school_rating IS NOT NULL)       AS has_school,
            COUNT(*) FILTER (WHERE commute_rating IS NOT NULL)      AS has_commute
        FROM properties
    """)
    stats = cur.fetchone()
    report["property_coverage"] = dict(stats)

    cur.close()
    conn.close()

    # 3. Evaluate outreach trigger condition (D50)
    completed_pipelines = {
        k for k, v in report["pipelines"].items()
        if v["status"] == "completed"
    }
    missing = OUTREACH_TRIGGER_PIPELINES - completed_pipelines
    if missing:
        report["outreach_blockers"] = [
            f"Pipeline not completed: {p}" for p in sorted(missing)
        ]
    
    total = report["property_coverage"].get("total", 0)
    gtfs_pct = report["property_coverage"].get("gtfs_auto", 0) / max(total, 1)
    if gtfs_pct < 0.8:
        report["outreach_blockers"].append(
            f"GTFS coverage only {gtfs_pct:.0%} — target ≥ 80%"
        )

    report["outreach_ready"] = len(report["outreach_blockers"]) == 0
    return report


def print_report(r: dict):
    print("\n=== NestMatch Data Readiness Report ===")
    print(f"Checked: {r['checked_at']}\n")

    print("Pipeline status:")
    for name, info in r["pipelines"].items():
        symbol = "✓" if info["status"] == "completed" else "✗"
        print(f"  {symbol} {name:<20} {info['status']:<12} rows={info['rows_affected']}  {info['run_at']}")

    if not r["pipelines"]:
        print("  (no pipeline runs recorded)")

    print("\nProperty coverage:")
    cov = r["property_coverage"]
    total = cov.get("total", 0)
    for key, val in cov.items():
        if key == "total":
            continue
        pct = f"{val/max(total,1):.0%}" if isinstance(val, int) else ""
        print(f"  {key:<30} {val:>4} / {total}  {pct}")

    print(f"\nPropTrack outreach trigger (D50):")
    if r["outreach_ready"]:
        print("  ✓ READY — all trigger conditions met. Fire outreach.")
    else:
        print("  ✗ NOT READY — blockers:")
        for b in r["outreach_blockers"]:
            print(f"    · {b}")


if __name__ == "__main__":
    report = check_readiness()
    print_report(report)
    sys.exit(0 if report["outreach_ready"] else 1)
