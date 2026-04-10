"""
IT.OPLE — Shopify Metafield Mapper
===================================
OPLE 상품 데이터 → Shopify custom.* 메타필드 값 매핑.

Core responsibilities:
  1. OPLE 상품 JSON (static/data/wms_active.json) → SKU lookup
  2. OPLE 상품 설명 HTML (static/data/wms_desc.json) → description_html
  3. OPLE 카테고리 (OpleCategory 테이블) → category_name / parent_category / category_id
  4. ShopifyProduct override 필드 → custom_title/description/price/tags 적용
  5. 22개 메타필드 전체 키에 대해 "채워졌는지" readiness 평가

Integration hints:
  - `load_ople_catalog()`: SKU → product dict, 6,155 active products
  - `build_metafields(it_id, sp_row, db)`: dict {key: value or None, ...}
  - `assess_readiness(mf)`: {ready: bool, missing_required: [...], missing_optional: [...]}
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

# ── Runtime caches ─────────────────────────────────────────
_catalog_cache: Optional[dict] = None   # {parent_sku: product_dict}
_desc_cache: Optional[dict] = None      # {parent_sku: html_string}

_DATA_DIR = Path(__file__).resolve().parent.parent / "static" / "data"

# Public dashboard base — used to build ople_url metafield.
# Can be overridden via env var OPLE_DASHBOARD_URL at runtime.
_DEFAULT_DASHBOARD_URL = "https://it-ople.onrender.com"


def _data_path(name: str) -> Path:
    return _DATA_DIR / name


def load_ople_catalog() -> dict:
    """Lazy-load wms_active.json → {parent_sku: product}."""
    global _catalog_cache
    if _catalog_cache is not None:
        return _catalog_cache

    path = _data_path("wms_active.json")
    if not path.exists():
        _catalog_cache = {}
        return _catalog_cache

    with open(path, "r", encoding="utf-8") as f:
        products = json.load(f)

    catalog = {}
    for p in products:
        sku = p.get("sku")
        if sku:
            catalog[sku] = p
    _catalog_cache = catalog
    return catalog


def load_ople_desc() -> dict:
    """Lazy-load wms_desc.json → {parent_sku: html_string}."""
    global _desc_cache
    if _desc_cache is not None:
        return _desc_cache

    path = _data_path("wms_desc.json")
    if not path.exists():
        _desc_cache = {}
        return _desc_cache

    with open(path, "r", encoding="utf-8") as f:
        _desc_cache = json.load(f)
    return _desc_cache


def reset_caches():
    """Clear caches so next call reloads from disk. Used by hot reload tools."""
    global _catalog_cache, _desc_cache
    _catalog_cache = None
    _desc_cache = None


# ── Category lookup ────────────────────────────────────────

def lookup_categories_for_parent(parent_sku: str, product: dict, db) -> list:
    """Given a WMS parent SKU and product dict, return the list of
    OpleCategory rows it belongs to.

    Linkage: ProductCategory.it_id stores the 10-digit OPLE public it_id,
    which appears inside the WMS product's child SKU list (p.ch).
    """
    # Avoid circular import; imported lazily.
    from database import Category, ProductCategory

    child_skus = []
    for c in (product.get("ch") or []):
        if isinstance(c, dict):
            child_sku = c.get("sku")
        else:
            child_sku = str(c)
        if child_sku:
            child_skus.append(str(child_sku))

    if not child_skus:
        return []

    pc_rows = (
        db.query(ProductCategory.category_id)
        .filter(ProductCategory.it_id.in_(child_skus))
        .distinct()
        .all()
    )
    cat_ids = [row[0] for row in pc_rows]
    if not cat_ids:
        return []

    return db.query(Category).filter(Category.category_id.in_(cat_ids)).all()


def _format_category_name(cats: list) -> Optional[str]:
    if not cats:
        return None
    # Take the deepest path available — prefer level3 if set
    primary = cats[0]
    parts = [primary.level1, primary.level2, primary.level3]
    return " > ".join(p for p in parts if p)


# ── Image URL resolver ─────────────────────────────────────

def resolve_image_url(product: dict) -> Optional[str]:
    """Resolve the canonical OPLE product image URL.

    Verified pattern from live OPLE product pages:
        https://img.ople.com/ople/item_img/{UPC}_R.jpg
    (The `_R` suffix is the large / representative hero image.)

    Previous patterns tried (all 404):
        https://www.ople.com/data/item/{UPC}/{UPC}.jpg
        https://img.ople.com/data/item/{prefix}/{it_id}.jpg
    """
    upc = (product.get("upc") or "").strip()
    if not upc:
        return None
    return f"https://img.ople.com/ople/item_img/{upc}_R.jpg"


# ── Public URL builder ─────────────────────────────────────

def build_ople_url(parent_sku: str) -> str:
    base = os.getenv("OPLE_DASHBOARD_URL", _DEFAULT_DASHBOARD_URL).rstrip("/")
    return f"{base}/#/products/{parent_sku}"


# ── Main mapper ────────────────────────────────────────────

# Which metafield keys are required for sync vs. optional
REQUIRED_KEYS = {
    "ople_sku",
    "upc",
    "brand_name_ko",
    "name_ko",
    "name_en",
    "price_usd",
    "sales_status",
    "stock_qty",
    "description_html",
    "image_url",
}

OPTIONAL_KEYS = {
    "brand_code",
    "ople_id",
    "ople_url",
    "price_krw",
    "box_count",
    "child_count",
    "category_name",
    "parent_category",
    "category_id",
    "child_products",
    "ople_mapped",
}

# Keys explicitly not populated by automation (user confirmed)
SKIPPED_KEYS = {"reserve_flag"}


def _default_fx_rate() -> float:
    """Resolve the live USD→KRW rate via fx_service, with safe fallback."""
    try:
        from fx_service import get_usd_krw_rate
        return get_usd_krw_rate()
    except Exception:
        return 1350.0


def build_metafields(
    parent_sku: str,
    sp_row=None,
    db=None,
    *,
    fx_rate_usd_to_krw: Optional[float] = None,
) -> dict:
    """Build a dict of Shopify metafield values for a given OPLE product.

    Parameters
    ----------
    parent_sku : str
        WMS parent SKU, e.g. "3M-P022334" (this is ShopifyProduct.it_id)
    sp_row : ShopifyProduct, optional
        If provided, override fields (custom_title / custom_description /
        custom_price_usd / custom_tags) take precedence over OPLE values.
    db : Session, optional
        SQLAlchemy session — required for category lookup.
    fx_rate_usd_to_krw : float, optional
        USD → KRW conversion rate for price_krw. If not provided, the live
        rate from fx_service is used (falls back to 1350 if all providers fail).

    Returns
    -------
    dict with keys matching 22 Shopify metafield definitions + a `_meta`
    section listing mapping details.
    """
    if fx_rate_usd_to_krw is None:
        fx_rate_usd_to_krw = _default_fx_rate()
    catalog = load_ople_catalog()
    desc_map = load_ople_desc()

    product = catalog.get(parent_sku)
    if not product:
        return {
            "_meta": {
                "parent_sku": parent_sku,
                "found_in_catalog": False,
                "error": f"SKU not found in wms_active.json: {parent_sku}",
            }
        }

    # Apply ShopifyProduct overrides if any
    override_title = getattr(sp_row, "custom_title", None) if sp_row else None
    override_desc = getattr(sp_row, "custom_description", None) if sp_row else None
    override_price = getattr(sp_row, "custom_price_usd", None) if sp_row else None
    override_tags_raw = getattr(sp_row, "custom_tags", None) if sp_row else None

    override_tags: list = []
    if override_tags_raw:
        try:
            override_tags = json.loads(override_tags_raw) if isinstance(override_tags_raw, str) else list(override_tags_raw)
        except Exception:
            override_tags = []

    # Base values from OPLE
    name_ko = override_title or product.get("kn") or None
    name_en = product.get("en") or None
    description_html = override_desc or desc_map.get(parent_sku)
    price_usd = override_price if override_price is not None else product.get("pr")
    if price_usd == 0:
        price_usd = None

    price_krw = None
    if price_usd:
        price_krw = int(round(float(price_usd) * float(fx_rate_usd_to_krw)))

    stock_qty = product.get("qt")
    if stock_qty is None:
        stock_qty = 0

    # Category lookup
    cats = []
    if db is not None:
        try:
            cats = lookup_categories_for_parent(parent_sku, product, db)
        except Exception:
            cats = []

    primary_cat = cats[0] if cats else None
    category_name = _format_category_name(cats)
    parent_category = primary_cat.level1 if primary_cat else None
    category_id = primary_cat.category_id if primary_cat else None

    # Child products JSON
    child_products_json = None
    ch = product.get("ch") or []
    if ch:
        child_products_json = json.dumps(ch, ensure_ascii=False)

    # Compose final map
    metafields = {
        # ━━━ OPLE 핵심 정보 ━━━
        "ople_sku": parent_sku,
        "upc": product.get("upc") or None,
        "brand_code": product.get("bc") or None,
        "brand_name_ko": product.get("bn") or None,
        "name_ko": name_ko,
        "name_en": name_en,
        "description_html": description_html,
        "ople_id": str(product.get("id")) if product.get("id") is not None else None,
        "ople_url": build_ople_url(parent_sku),

        # ━━━ 가격 & 재고 ━━━
        "price_usd": float(price_usd) if price_usd else None,
        "price_krw": price_krw,
        "stock_qty": int(stock_qty) if stock_qty is not None else None,
        "box_count": product.get("bx") if product.get("bx") else None,
        "child_count": product.get("cc") if product.get("cc") else None,

        # ━━━ 상태 & 분류 ━━━
        "sales_status": product.get("st") or product.get("rn") or None,
        "reserve_flag": None,  # 사용자 확정: 무시
        "category_name": category_name,
        "parent_category": parent_category,
        "category_id": category_id,

        # ━━━ 구성품 & 이미지 & 매핑 ━━━
        "child_products": child_products_json,
        "image_url": resolve_image_url(product),
        "ople_mapped": True,

        # ━━━ Debug / 운영 정보 ━━━
        "_meta": {
            "parent_sku": parent_sku,
            "found_in_catalog": True,
            "has_override_title": bool(override_title),
            "has_override_description": bool(override_desc),
            "has_override_price": override_price is not None,
            "override_tags": override_tags,
            "category_count": len(cats),
            "fx_rate_usd_to_krw": fx_rate_usd_to_krw,
            "skipped_keys": sorted(SKIPPED_KEYS),
        },
    }
    return metafields


# ── Readiness assessment ───────────────────────────────────

def assess_readiness(metafields: dict) -> dict:
    """Evaluate which required / optional metafields are actually populated.

    Returns a readiness report that can drive the Shopify sync gate
    (sync button should only activate when `ready` is True).
    """
    if not metafields.get("_meta", {}).get("found_in_catalog", False):
        return {
            "ready": False,
            "missing_required": sorted(REQUIRED_KEYS),
            "missing_optional": sorted(OPTIONAL_KEYS),
            "filled_count": 0,
            "required_count": len(REQUIRED_KEYS),
            "optional_count": len(OPTIONAL_KEYS),
            "total_keys": len(REQUIRED_KEYS) + len(OPTIONAL_KEYS),
            "error": metafields.get("_meta", {}).get("error", "Product not found"),
        }

    def _is_empty(val) -> bool:
        if val is None:
            return True
        if isinstance(val, str) and not val.strip():
            return True
        if isinstance(val, (list, dict)) and len(val) == 0:
            return True
        return False

    missing_required = sorted(k for k in REQUIRED_KEYS if _is_empty(metafields.get(k)))
    missing_optional = sorted(k for k in OPTIONAL_KEYS if _is_empty(metafields.get(k)))

    filled = [k for k in (REQUIRED_KEYS | OPTIONAL_KEYS) if not _is_empty(metafields.get(k))]

    return {
        "ready": len(missing_required) == 0,
        "missing_required": missing_required,
        "missing_optional": missing_optional,
        "filled_count": len(filled),
        "required_count": len(REQUIRED_KEYS),
        "optional_count": len(OPTIONAL_KEYS),
        "total_keys": len(REQUIRED_KEYS) + len(OPTIONAL_KEYS),
    }
