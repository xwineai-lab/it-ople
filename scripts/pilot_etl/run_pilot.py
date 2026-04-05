"""
Pilot ETL orchestrator
----------------------
Runs all 3 fetchers for a target ingredient (default: Vitamin D) and produces
a single unified JSON record conforming to the catalog_v2 ingredient master
schema.

Usage:
  python3 run_pilot.py                   # defaults to vitamin_d
  python3 run_pilot.py vitamin_c 비타민C
  python3 run_pilot.py omega_3 오메가3
"""
import sys
import os
import json
import time
from pathlib import Path

# Make local imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nih_ods_fetcher import fetch_nutrient
from openfoodfacts_fetcher import search_products as off_search
from mfds_fetcher import search_products as mfds_search
from unifier import unify


# Ingredient → (english key for NIH, OFF search term, Korean term for MFDS)
INGREDIENT_CATALOG = {
    "vitamin_d": ("vitamin d", "비타민D"),
    "vitamin_c": ("vitamin c", "비타민C"),
    "vitamin_b12": ("vitamin b12", "비타민B12"),
    "vitamin_a": ("vitamin a", "비타민A"),
    "vitamin_e": ("vitamin e", "비타민E"),
    "omega_3": ("omega 3", "오메가3"),
    "calcium": ("calcium", "칼슘"),
    "iron": ("iron", "철"),
    "magnesium": ("magnesium", "마그네슘"),
    "zinc": ("zinc", "아연"),
    "probiotics": ("probiotics", "프로바이오틱스"),
}


def run(ingredient_key: str = "vitamin_d", korean_override: str = None) -> dict:
    meta = INGREDIENT_CATALOG.get(ingredient_key)
    if not meta:
        raise ValueError(f"unknown ingredient: {ingredient_key}")
    off_query, kr_query = meta
    if korean_override:
        kr_query = korean_override

    print(f"━━━ Pilot ETL: {ingredient_key} ({kr_query}) ━━━")
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    print("[1/3] NIH ODS...")
    nih = fetch_nutrient(ingredient_key)

    print("[2/3] Open Food Facts...")
    off = off_search(off_query, page_size=5, category="dietary-supplements")

    print("[3/3] MFDS (sample key)...")
    mfds = mfds_search(kr_query, end=10)

    print("[unify] merging sources...")
    unified = unify(ingredient_key, kr_query, nih, off, mfds)

    unified["meta"] = {
        "pilot_run_at": started_at,
        "pilot_version": "1.0",
    }
    return unified


def main():
    key = sys.argv[1] if len(sys.argv) > 1 else "vitamin_d"
    kr_override = sys.argv[2] if len(sys.argv) > 2 else None

    unified = run(key, kr_override)

    # Output paths: always under workspace outputs
    out_dir = Path("/sessions/elegant-zealous-rubin/mnt/outputs/pilot_etl")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{key}_unified.json"
    out_path.write_text(
        json.dumps(unified, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Also save raw source dumps
    print(f"\n✅ unified record saved: {out_path}")
    print(f"   coverage: {unified['coverage_score']}")
    print(f"   name: {unified['name_i18n']}")
    print(f"   dosage RDA: {unified['dosage'].get('rda', 'n/a')}")
    print(f"   dosage UL : {unified['dosage'].get('upper_limit', 'n/a')}")
    print(f"   KR registered products: {unified['kr_regulatory']['registered_product_count']}")
    print(f"   market samples: {len(unified['market_samples'])}")


if __name__ == "__main__":
    main()
