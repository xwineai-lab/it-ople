"""
iHerb Product Mapper
────────────────────
OPLE 상품 ↔ iHerb 상품 매핑 파이프라인.
3단계: UPC 정확매칭 → 퍼지매칭 → AI 검증
"""

import json
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from rapidfuzz import fuzz, process

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("iherb_mapper")


@dataclass
class MappingResult:
    ople_id: str
    ople_name: str
    ople_brand: str
    ople_price_usd: float
    iherb_id: Optional[str]
    iherb_name: Optional[str]
    iherb_brand: Optional[str]
    iherb_price_usd: Optional[float]
    match_method: str  # "upc", "fuzzy", "ai", "manual", "none"
    match_score: float  # 0-100
    price_diff: Optional[float]  # negative = OPLE cheaper
    price_diff_pct: Optional[float]


def normalize_product_name(name: str) -> str:
    """Normalize product name for comparison."""
    name = name.lower().strip()
    # Remove common noise
    name = re.sub(r"[,\-–—·]", " ", name)
    name = re.sub(r"\s+", " ", name)
    # Standardize units
    name = re.sub(r"(\d+)\s*mg", r"\1mg", name)
    name = re.sub(r"(\d+)\s*mcg", r"\1mcg", name)
    name = re.sub(r"(\d+)\s*iu", r"\1iu", name)
    name = re.sub(r"(\d+)\s*(정|캡슐|tabs|capsules|softgels|tablets|vcaps)", r"\1\2", name)
    return name


def extract_brand(name: str) -> str:
    """Extract brand name from product name."""
    known_brands = [
        "now foods", "solgar", "jarrow formulas", "doctor's best",
        "nature's way", "swanson", "double wood", "absonutrix",
        "life extension", "garden of life", "nordic naturals",
        "country life", "natrol", "source naturals", "21st century",
        "california gold nutrition", "lake avenue nutrition",
        "sports research", "thorne", "pure encapsulations",
    ]
    name_lower = name.lower()
    for brand in known_brands:
        if brand in name_lower:
            return brand
    return ""


def extract_quantity(name: str) -> str:
    """Extract quantity/count from product name."""
    match = re.search(r"(\d+)\s*(정|캡슐|tabs?|capsules?|softgels?|tablets?|vcaps?|ct|count)", name, re.I)
    if match:
        return f"{match.group(1)}{match.group(2).lower()}"
    return ""


def extract_dosage(name: str) -> str:
    """Extract dosage from product name."""
    match = re.search(r"(\d+)\s*(mg|mcg|iu|g|ml)", name, re.I)
    if match:
        return f"{match.group(1)}{match.group(2).lower()}"
    return ""


# ── Mapping Pipeline ─────────────────────────────────────

class IHerbMapper:
    def __init__(self, iherb_products: list[dict]):
        """
        Initialize with iHerb product data.
        Expected format: [{"id": "...", "name": "...", "brand": "...", "price": 0.0, "upc": "..."}, ...]
        """
        self.iherb_products = iherb_products
        self.iherb_by_upc = {}
        self.iherb_names = []
        self.iherb_name_map = {}

        for p in iherb_products:
            # UPC index
            upc = p.get("upc", "").strip()
            if upc:
                self.iherb_by_upc[upc] = p

            # Name index for fuzzy matching
            normalized = normalize_product_name(p.get("name", ""))
            brand = p.get("brand", "").lower()
            key = f"{brand} {normalized}"
            self.iherb_names.append(key)
            self.iherb_name_map[key] = p

        logger.info(f"Loaded {len(iherb_products)} iHerb products, {len(self.iherb_by_upc)} with UPC")

    def match_by_upc(self, upc: str) -> Optional[tuple[dict, float]]:
        """Step 1: Exact UPC match."""
        if upc and upc in self.iherb_by_upc:
            return self.iherb_by_upc[upc], 99.0
        return None

    def match_by_fuzzy(self, ople_product: dict, threshold: int = 70) -> Optional[tuple[dict, float]]:
        """Step 2: Fuzzy name matching with brand + dosage + quantity."""
        brand = extract_brand(ople_product.get("name_ko", "") + " " + ople_product.get("name_en", ""))
        name = normalize_product_name(ople_product.get("name_en", "") or ople_product.get("name_ko", ""))
        dosage = extract_dosage(name)
        quantity = extract_quantity(name)

        query = f"{brand} {name}"

        # Use rapidfuzz for fast fuzzy matching
        results = process.extract(
            query,
            self.iherb_names,
            scorer=fuzz.token_sort_ratio,
            limit=5,
        )

        if not results:
            return None

        best_match_name, best_score, _ = results[0]

        if best_score < threshold:
            return None

        matched_product = self.iherb_name_map.get(best_match_name)
        if not matched_product:
            return None

        # Bonus scoring for matching dosage and quantity
        matched_name = normalize_product_name(matched_product.get("name", ""))
        matched_dosage = extract_dosage(matched_name)
        matched_quantity = extract_quantity(matched_name)

        bonus = 0
        if dosage and matched_dosage and dosage == matched_dosage:
            bonus += 5
        if quantity and matched_quantity and quantity == matched_quantity:
            bonus += 5

        final_score = min(best_score + bonus, 99)

        return matched_product, final_score

    def map_product(self, ople_product: dict) -> MappingResult:
        """Map a single OPLE product to iHerb."""
        ople_id = ople_product.get("it_id", "")
        ople_name = ople_product.get("name_ko", "")
        ople_brand = ople_product.get("brand", "")
        ople_price = ople_product.get("price_usd", 0.0)

        # Step 1: UPC match
        upc = ople_product.get("upc", "")
        upc_result = self.match_by_upc(upc)
        if upc_result:
            iherb_prod, score = upc_result
            iherb_price = iherb_prod.get("price", 0.0)
            price_diff = ople_price - iherb_price if (ople_price and iherb_price) else None
            price_diff_pct = (price_diff / iherb_price * 100) if (price_diff is not None and iherb_price) else None

            return MappingResult(
                ople_id=ople_id, ople_name=ople_name, ople_brand=ople_brand,
                ople_price_usd=ople_price,
                iherb_id=iherb_prod.get("id"),
                iherb_name=iherb_prod.get("name"),
                iherb_brand=iherb_prod.get("brand"),
                iherb_price_usd=iherb_price,
                match_method="upc", match_score=score,
                price_diff=price_diff, price_diff_pct=price_diff_pct,
            )

        # Step 2: Fuzzy match
        fuzzy_result = self.match_by_fuzzy(ople_product)
        if fuzzy_result:
            iherb_prod, score = fuzzy_result
            iherb_price = iherb_prod.get("price", 0.0)
            price_diff = ople_price - iherb_price if (ople_price and iherb_price) else None
            price_diff_pct = (price_diff / iherb_price * 100) if (price_diff is not None and iherb_price) else None

            return MappingResult(
                ople_id=ople_id, ople_name=ople_name, ople_brand=ople_brand,
                ople_price_usd=ople_price,
                iherb_id=iherb_prod.get("id"),
                iherb_name=iherb_prod.get("name"),
                iherb_brand=iherb_prod.get("brand"),
                iherb_price_usd=iherb_price,
                match_method="fuzzy", match_score=score,
                price_diff=price_diff, price_diff_pct=price_diff_pct,
            )

        # No match
        return MappingResult(
            ople_id=ople_id, ople_name=ople_name, ople_brand=ople_brand,
            ople_price_usd=ople_price,
            iherb_id=None, iherb_name=None, iherb_brand=None, iherb_price_usd=None,
            match_method="none", match_score=0,
            price_diff=None, price_diff_pct=None,
        )

    def map_all(self, ople_products: list[dict]) -> list[MappingResult]:
        """Map all OPLE products to iHerb."""
        results = []
        for i, prod in enumerate(ople_products):
            result = self.map_product(prod)
            results.append(result)

            if (i + 1) % 100 == 0:
                matched = sum(1 for r in results if r.match_method != "none")
                logger.info(f"Progress: {i + 1}/{len(ople_products)}, matched: {matched}")

        # Summary
        total = len(results)
        matched = sum(1 for r in results if r.match_method != "none")
        upc_matches = sum(1 for r in results if r.match_method == "upc")
        fuzzy_matches = sum(1 for r in results if r.match_method == "fuzzy")

        logger.info(f"═══ Mapping Complete ═══")
        logger.info(f"Total: {total}, Matched: {matched} ({matched/total*100:.1f}%)")
        logger.info(f"  UPC: {upc_matches}, Fuzzy: {fuzzy_matches}")

        # Price analysis
        price_diffs = [r.price_diff for r in results if r.price_diff is not None]
        if price_diffs:
            avg_diff = sum(price_diffs) / len(price_diffs)
            ople_cheaper = sum(1 for d in price_diffs if d < 0)
            logger.info(f"Price: avg diff ${avg_diff:.2f}, OPLE cheaper {ople_cheaper}/{len(price_diffs)}")

        return results


def save_mapping_results(results: list[MappingResult], output_path: str):
    """Save mapping results to JSON."""
    data = []
    for r in results:
        data.append({
            "ople_id": r.ople_id,
            "ople_name": r.ople_name,
            "ople_brand": r.ople_brand,
            "ople_price_usd": r.ople_price_usd,
            "iherb_id": r.iherb_id,
            "iherb_name": r.iherb_name,
            "iherb_brand": r.iherb_brand,
            "iherb_price_usd": r.iherb_price_usd,
            "match_method": r.match_method,
            "match_score": r.match_score,
            "price_diff": r.price_diff,
            "price_diff_pct": r.price_diff_pct,
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(data)} mapping results to {output_path}")


if __name__ == "__main__":
    # Demo with sample data
    sample_iherb = [
        {"id": "1", "name": "Now Foods Ultra Omega-3 180 Softgels", "brand": "Now Foods", "price": 28.49, "upc": "733739016522"},
        {"id": "2", "name": "Solgar Collagen Hyaluronic Acid Complex 30 Tablets", "brand": "Solgar", "price": 16.99},
        {"id": "3", "name": "Doctor's Best Lutein with FloraGlo 20mg 180 Softgels", "brand": "Doctor's Best", "price": 29.99},
    ]

    sample_ople = [
        {"it_id": "1319032894", "name_ko": "Now Foods 울트라 오메가-3, 180캡슐", "name_en": "Now Foods Ultra Omega-3 180 Softgels", "brand": "Now Foods", "price_usd": 25.99},
        {"it_id": "1505216341", "name_ko": "Solgar 콜라겐 히알루론산, 30정", "name_en": "Solgar Collagen HA Complex", "brand": "Solgar", "price_usd": 14.99},
    ]

    mapper = IHerbMapper(sample_iherb)
    results = mapper.map_all(sample_ople)

    for r in results:
        status = "MATCHED" if r.match_method != "none" else "NO MATCH"
        price_info = f" | diff: ${r.price_diff:+.2f}" if r.price_diff is not None else ""
        print(f"[{status}] {r.ople_name} → {r.iherb_name} ({r.match_method}: {r.match_score}%){price_info}")
