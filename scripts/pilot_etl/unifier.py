"""
Unifier: merges 3 data sources into a single enriched ingredient record
matching the normalized catalog_v2 schema proposed in the flow doc.

Inputs:
  - nih_data: dict from nih_ods_fetcher.fetch_nutrient()
  - off_data: dict from openfoodfacts_fetcher.search_products()
  - mfds_data: dict from mfds_fetcher.search_products()

Output: a single IngredientMaster-shaped dict with i18n name, dosage,
        safety, allergens, Korean regulatory status, and market-product samples.
"""
from typing import Dict, List, Optional


def unify(ingredient_key: str, korean_keyword: str,
          nih: Dict, off: Dict, mfds: Dict) -> Dict:
    """Build a unified ingredient master record."""

    # --- Name i18n ---
    name_i18n = {
        "ko": korean_keyword,
        "en": ingredient_key.replace("_", " ").title(),
    }

    # --- Dosage (from NIH) ---
    dosage = {}
    if nih and not nih.get("error"):
        dosage = {
            "rda": nih.get("dosage", {}).get("rda"),
            "ai": nih.get("dosage", {}).get("ai"),
            "upper_limit": nih.get("dosage", {}).get("ul"),
            "reference_authority": "NIH ODS (U.S.)",
            "reference_url": nih.get("source_url"),
        }

    # --- Safety & health effects (from NIH) ---
    safety_notes = {}
    if nih and not nih.get("error"):
        safety_notes = {
            "description_en": nih.get("introduction", "")[:400],
            "deficiency": (nih.get("deficiency") or "")[:300],
            "health_effects": (nih.get("health_effects") or "")[:300],
            "drug_interactions": (nih.get("interactions") or "")[:300],
            "toxicity": (nih.get("safety") or "")[:300],
        }

    # --- Cross-check: Korean registered products (MFDS) ---
    korean_registered = []
    if mfds and not mfds.get("error"):
        for p in mfds.get("products", [])[:5]:
            korean_registered.append({
                "name_ko": p.get("product_name_ko"),
                "company_ko": p.get("company_name"),
                "form": p.get("product_form"),
                "functionality_claim_ko": (p.get("functionality") or "")[:250],
                "intake_method_ko": p.get("intake_method"),
                "warnings_ko": (p.get("warnings") or "")[:250],
                "raw_materials_ko": (p.get("raw_materials") or "")[:250],
                "mfds_report_no": p.get("report_number"),
            })

    # --- Market product samples (OFF, if available) ---
    market_samples = []
    if off and not off.get("error"):
        for p in off.get("products", [])[:5]:
            market_samples.append({
                "barcode": p.get("barcode"),
                "name": p.get("name"),
                "brand": p.get("brand"),
                "ingredients_text": (p.get("ingredients_text") or "")[:200],
                "allergens": p.get("allergens", []),
                "image_url": p.get("image_url"),
            })

    # --- Allergen aggregation (from OFF products) ---
    all_allergens = set()
    if off and not off.get("error"):
        for p in off.get("products", []):
            all_allergens.update(p.get("allergens", []))

    # --- Korean regulatory summary ---
    kr_regulatory = {
        "is_registered_in_kr": bool(korean_registered),
        "registered_product_count": mfds.get("total_count", 0) if mfds else 0,
        "sample_functionality_claims": [
            p["functionality_claim_ko"] for p in korean_registered[:3]
            if p.get("functionality_claim_ko")
        ],
    }

    # --- Sources audit trail ---
    sources_used = []
    if nih and not nih.get("error"):
        sources_used.append({
            "source": "NIH_ODS",
            "url": nih.get("source_url"),
            "fetched_at": nih.get("fetched_at"),
        })
    if off and not off.get("error"):
        sources_used.append({
            "source": "OpenFoodFacts",
            "url": off.get("source_url"),
            "fetched_at": off.get("fetched_at"),
        })
    elif off:
        sources_used.append({
            "source": "OpenFoodFacts",
            "status": "unavailable",
            "note": off.get("note"),
        })
    if mfds and not mfds.get("error"):
        sources_used.append({
            "source": "MFDS_foodsafetykorea",
            "service_id": mfds.get("service_id"),
            "fetched_at": mfds.get("fetched_at"),
        })

    return {
        "ingredient_key": ingredient_key,
        "name_i18n": name_i18n,
        "dosage": dosage,
        "safety_notes": safety_notes,
        "allergens": sorted(all_allergens),
        "kr_regulatory": kr_regulatory,
        "korean_registered_products": korean_registered,
        "market_samples": market_samples,
        "sources_used": sources_used,
        "coverage_score": _coverage(nih, off, mfds),
    }


def _coverage(nih: Dict, off: Dict, mfds: Dict) -> Dict:
    """How many of 3 sources returned data."""
    score = 0
    detail = {}
    for name, data in [("nih", nih), ("off", off), ("mfds", mfds)]:
        ok = bool(data) and not data.get("error")
        detail[name] = "ok" if ok else "missing"
        if ok:
            score += 1
    return {"score": f"{score}/3", "detail": detail}
