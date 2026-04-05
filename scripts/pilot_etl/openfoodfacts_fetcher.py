"""
Open Food Facts API Fetcher
----------------------------
Queries OFF for supplement products to cross-check ingredients, allergens,
and nutrition facts.

Docs: https://openfoodfacts.github.io/openfoodfacts-server/api/

Endpoints tried (in order of preference):
  1. https://world.openfoodfacts.org/api/v2/search
  2. https://world.openfoodfacts.org/cgi/search.pl (legacy)
  3. Country mirrors (fr, us, kr)

Returns a list of matching products with normalized fields.
"""
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Dict, List, Optional

USER_AGENT = "OPLE-ETL/1.0 (contact: admin@ople.com)"

OFF_ENDPOINTS = [
    "https://world.openfoodfacts.org/api/v2/search",
    "https://us.openfoodfacts.org/api/v2/search",
    "https://fr.openfoodfacts.org/api/v2/search",
]

FIELDS = ",".join([
    "code", "product_name", "product_name_en", "brands",
    "categories_tags", "ingredients_text", "ingredients_text_en",
    "allergens_tags", "labels_tags", "countries_tags",
    "nutriments", "image_front_url", "nutriscore_grade",
])


def _http_get(url: str, timeout: int = 20) -> Optional[str]:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"  [OFF] request failed: {e}")
        return None


def search_products(query: str, page_size: int = 10, category: Optional[str] = None) -> Dict:
    """
    Search OFF for products matching `query`.
    `category` can be an OFF category tag like 'vitamins' or 'dietary-supplements'.
    """
    params = {
        "search_terms": query,
        "fields": FIELDS,
        "page_size": str(page_size),
        "sort_by": "popularity_key",
    }
    if category:
        params["categories_tags_en"] = category

    qs = urllib.parse.urlencode(params)
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    for base in OFF_ENDPOINTS:
        url = f"{base}?{qs}"
        print(f"  [OFF] trying {base.split('//')[1].split('/')[0]}...")
        body = _http_get(url)
        if not body:
            continue
        # Check if we got HTML (error page) vs JSON
        if body.lstrip().startswith("<"):
            print(f"  [OFF] got HTML (service unavailable), trying next endpoint")
            continue
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            continue

        products = data.get("products", [])
        return {
            "source": "OpenFoodFacts",
            "source_url": url,
            "query": query,
            "category": category,
            "fetched_at": fetched_at,
            "endpoint": base,
            "total_count": data.get("count", len(products)),
            "returned": len(products),
            "products": [_normalize_product(p) for p in products],
        }

    return {
        "source": "OpenFoodFacts",
        "query": query,
        "category": category,
        "fetched_at": fetched_at,
        "error": "all_endpoints_failed",
        "note": "OFF API temporarily unavailable. Retry later or use local dump: "
                "https://world.openfoodfacts.org/data",
        "products": [],
    }


def _normalize_product(p: Dict) -> Dict:
    """Normalize OFF product to OPLE-friendly schema."""
    nutri = p.get("nutriments", {}) or {}
    return {
        "barcode": p.get("code"),
        "name": p.get("product_name_en") or p.get("product_name") or "",
        "brand": (p.get("brands") or "").split(",")[0].strip(),
        "ingredients_text": p.get("ingredients_text_en") or p.get("ingredients_text") or "",
        "allergens": [t.replace("en:", "") for t in (p.get("allergens_tags") or [])],
        "labels": [t.replace("en:", "") for t in (p.get("labels_tags") or [])],
        "countries": [t.replace("en:", "") for t in (p.get("countries_tags") or [])],
        "categories": [t.replace("en:", "") for t in (p.get("categories_tags") or [])],
        "image_url": p.get("image_front_url"),
        "nutriscore": p.get("nutriscore_grade"),
        "nutriments_summary": {
            k: nutri.get(k) for k in [
                "energy-kcal_100g", "proteins_100g", "carbohydrates_100g",
                "fat_100g", "sugars_100g", "salt_100g",
                "vitamin-d_100g", "vitamin-c_100g", "calcium_100g", "iron_100g",
            ] if k in nutri
        },
    }


def fetch_by_barcode(barcode: str) -> Optional[Dict]:
    """Direct product lookup by UPC/EAN barcode (most reliable)."""
    url = f"https://world.openfoodfacts.org/api/v2/product/{barcode}?fields={FIELDS}"
    body = _http_get(url)
    if not body or body.lstrip().startswith("<"):
        return None
    try:
        data = json.loads(body)
        if data.get("status") == 1:
            return _normalize_product(data.get("product", {}))
    except json.JSONDecodeError:
        pass
    return None


if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "vitamin d"
    cat = sys.argv[2] if len(sys.argv) > 2 else "dietary-supplements"
    result = search_products(q, page_size=5, category=cat)
    print(json.dumps(result, indent=2, ensure_ascii=False)[:3000])
