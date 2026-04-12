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
import re
from pathlib import Path
from typing import Any, Optional

# ── Runtime caches ─────────────────────────────────────────
_catalog_cache: Optional[dict] = None   # {parent_sku: product_dict}
_desc_cache: Optional[dict] = None      # {parent_sku: html_string}
_child_to_parent_cache: Optional[dict] = None  # {child_sku: parent_sku}

_DATA_DIR = Path(__file__).resolve().parent.parent / "static" / "data"

# Public dashboard base — used to build ople_url metafield.
# Can be overridden via env var OPLE_DASHBOARD_URL at runtime.
_DEFAULT_DASHBOARD_URL = "https://it-ople.onrender.com"


def _data_path(name: str) -> Path:
    return _DATA_DIR / name


# ── Source-data sanitization ───────────────────────────────
#
# wms_active.json was produced by an upstream Excel → JSON ETL that
# preserved the Office Open XML `_x000D_` escape sequence (= U+000D carriage
# return) as a *literal* 7-char string inside the product names. Roughly 68%
# of rows carry this artifact on the `kn` (Korean name) field. The _xHHHH_
# form is standardized in ECMA-376 / ISO 29500 for any character that can't
# appear directly inside XML text, so we defensively strip the whole family
# of escapes, not just `_x000D_`.
#
# Reference: Excel writes `_x{hex}_` for any control character it needs to
# round-trip; well-behaved readers should decode them back. Some exporters
# (notably older openpyxl paths and hand-rolled XML-to-CSV scripts) skip the
# decode step and the literal escape leaks into downstream data.

# Matches the whole _xHHHH_ escape family (case-insensitive hex).
_XML_ESCAPE_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")


def _sanitize_str(value: str) -> str:
    """Strip Excel-XML `_xHHHH_` artifacts + stray control characters.

    What this cleans:
      - `_x000D_` / `_x000A_` / `_x0009_` etc.  → removed (these are the
        Excel-XML escapes for CR / LF / TAB that leaked through as literal
        text because the upstream exporter did not decode them)
      - Real `\r` characters                    → removed
      - Leading / trailing whitespace            → stripped

    What this *preserves*:
      - Real `\n` line breaks inside the string  (intentional formatting)
      - Real `\t` tabs                            (intentional formatting)
      - Everything else

    Safe to call on non-string inputs — they are returned unchanged.
    """
    if not isinstance(value, str):
        return value
    # Drop every _xHHHH_ Excel escape — not just _x000D_ — so we catch any
    # future control-char leak without another round of patches.
    cleaned = _XML_ESCAPE_RE.sub("", value)
    # Real CRs that sneaked in for other reasons.
    cleaned = cleaned.replace("\r", "")
    return cleaned.strip()


def _sanitize_deep(obj: Any) -> Any:
    """Recursively apply `_sanitize_str` to every string inside a dict/list."""
    if isinstance(obj, str):
        return _sanitize_str(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_deep(v) for v in obj]
    return obj


def load_ople_catalog() -> dict:
    """Lazy-load wms_active.json → {parent_sku: product}.

    Applies `_sanitize_deep` to every product on load so that downstream code
    (metafield builder, Shopify push, dashboard UI, …) only ever sees clean
    strings. One source-of-truth sanitization beats plastering `.replace()`
    calls across every consumer.
    """
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
            # Sanitize after lookup so the SKU key itself isn't touched —
            # SKUs are already known clean and we don't want to risk any
            # whitespace-strip edge case breaking keyed lookups.
            catalog[sku] = _sanitize_deep(p)
    _catalog_cache = catalog
    return catalog


def _build_child_to_parent_map(catalog: dict) -> dict:
    """Build reverse lookup: child_sku (10-digit OPLE IT ID) → parent_sku.

    ShopifyProduct.it_id stores the 10-digit OPLE child ID (e.g. "1417406120"),
    but the catalog is keyed by parent SKU (e.g. "SOL-P003579"). The child IDs
    live inside each parent's `ch` array. This map lets us resolve any child ID
    back to the parent product that contains the actual name, brand, price, etc.
    """
    global _child_to_parent_cache
    if _child_to_parent_cache is not None:
        return _child_to_parent_cache

    child_map = {}
    for parent_sku, product in catalog.items():
        for c in (product.get("ch") or []):
            if isinstance(c, dict):
                child_sku = c.get("sku")
            else:
                child_sku = str(c)
            if child_sku:
                child_map[str(child_sku)] = parent_sku
    _child_to_parent_cache = child_map
    return child_map


def resolve_parent_sku(sku: str) -> tuple[str, bool]:
    """Given either a parent SKU or child SKU, return (parent_sku, was_child).

    If `sku` is already a parent key in the catalog, returns (sku, False).
    If `sku` is a child ID found in the child→parent map, returns (parent_sku, True).
    If not found at all, returns (sku, False) so the caller can handle the miss.
    """
    catalog = load_ople_catalog()
    if sku in catalog:
        return (sku, False)
    child_map = _build_child_to_parent_map(catalog)
    parent = child_map.get(sku)
    if parent:
        return (parent, True)
    return (sku, False)


def load_ople_desc() -> dict:
    """Lazy-load wms_desc.json → {parent_sku: html_string}.

    Also sanitized, though wms_desc.json has not been observed to contain
    `_x000D_` — applying the same cleaner defensively in case the exporter
    changes or a future reload introduces it.
    """
    global _desc_cache
    if _desc_cache is not None:
        return _desc_cache

    path = _data_path("wms_desc.json")
    if not path.exists():
        _desc_cache = {}
        return _desc_cache

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    _desc_cache = {k: _sanitize_str(v) if isinstance(v, str) else v for k, v in raw.items()}
    return _desc_cache


def reset_caches():
    """Clear caches so next call reloads from disk. Used by hot reload tools."""
    global _catalog_cache, _desc_cache, _child_to_parent_cache
    _catalog_cache = None
    _desc_cache = None
    _child_to_parent_cache = None


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
    """Resolve the primary (front) OPLE product image URL.

    OPLE hosts up to two variants per UPC:
        https://img.ople.com/ople/item_img/{UPC}.jpg     → 정면 (Front / main)
        https://img.ople.com/ople/item_img/{UPC}_R.jpg   → 후면 (Rear)

    The `_R` suffix is the back of the package — it's a *secondary* image,
    not the representative hero. The hero / 메인 image is the suffix-less URL.

    Returned value populates the `image_url` metafield (single string).
    Use `resolve_image_urls()` for the full [front, rear] list when pushing
    media to Shopify.
    """
    upc = (product.get("upc") or "").strip()
    if not upc:
        return None
    return f"https://img.ople.com/ople/item_img/{upc}.jpg"


def resolve_image_urls(product: dict) -> list[str]:
    """Return the ordered list of OPLE product image URLs.

    Order matters: the first URL becomes the Shopify main product image,
    subsequent URLs become secondary gallery images.

      [0] {UPC}.jpg   — 정면 (Front)
      [1] {UPC}_R.jpg — 후면 (Rear)

    NOTE: This function does not HEAD-check the URLs — Shopify will silently
    skip any that 404 at fetch time. If a product has only one variant the
    other will just fail gracefully on Shopify's side.
    """
    upc = (product.get("upc") or "").strip()
    if not upc:
        return []
    return [
        f"https://img.ople.com/ople/item_img/{upc}.jpg",
        f"https://img.ople.com/ople/item_img/{upc}_R.jpg",
    ]


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

    # Resolve child SKU → parent SKU if needed.
    # ShopifyProduct.it_id may be a 10-digit child OPLE ID (e.g. "1417406120")
    # while the catalog is keyed by parent SKU (e.g. "SOL-P003579").
    original_sku = parent_sku
    resolved_parent, was_child = resolve_parent_sku(parent_sku)
    if was_child:
        parent_sku = resolved_parent

    product = catalog.get(parent_sku)
    if not product:
        return {
            "_meta": {
                "parent_sku": parent_sku,
                "original_sku": original_sku,
                "found_in_catalog": False,
                "error": f"SKU not found in wms_active.json: {parent_sku} (original: {original_sku})",
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
            "original_sku": original_sku,
            "resolved_from_child": was_child,
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
