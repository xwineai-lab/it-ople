"""
IT.OPLE — FastAPI Backend
OPLE 상품/리뷰 분석 & iHerb 매핑 인트라넷
"""
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, case
from database import init_db, get_db, Product, Review, IHerbMapping, IHerbProduct, ScrapeJob

# ── App Setup ────────────────────────────────────────────
app = FastAPI(title="IT.OPLE", version="1.0.0", description="OPLE 상품 분석 & iHerb 매핑 인트라넷")

# CORS middleware - allow iHerb scraping tabs to send data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize DB on startup
@app.on_event("startup")
def startup():
    init_db()
    db = next(get_db())
    if db.query(Product).count() == 0:
        seed_demo_data(db)
    db.close()

# ── Static Files ─────────────────────────────────────────
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = static_dir / "index.html"
    if index_path.exists():
        return index_path.read_text(encoding="utf-8")
    return "<h1>IT.OPLE API</h1><p>Frontend not found. Place index.html in /static/</p>"

# ── Dashboard APIs ───────────────────────────────────────
@app.get("/api/dashboard/stats")
def get_dashboard_stats(db: Session = Depends(get_db)):
    total_products = db.query(Product).count()
    total_reviews = db.query(Review).count()
    total_mapped = db.query(IHerbMapping).filter(IHerbMapping.match_method != "none").count()
    avg_price = db.query(func.avg(Product.price_usd)).scalar() or 0
    mapping_rate = (total_mapped / total_products * 100) if total_products > 0 else 0
    cheaper_count = db.query(IHerbMapping).filter(IHerbMapping.price_diff < 0).count()
    price_advantage = (cheaper_count / total_mapped * 100) if total_mapped > 0 else 0
    avg_price_diff = db.query(func.avg(IHerbMapping.price_diff_pct)).filter(
        IHerbMapping.price_diff_pct.isnot(None)
    ).scalar() or 0
    top_brands = db.query(
        Product.brand,
        func.count(Product.id).label("count"),
        func.avg(Product.price_usd).label("avg_price"),
        func.sum(Product.review_count).label("total_reviews"),
    ).group_by(Product.brand).order_by(desc("total_reviews")).limit(10).all()
    return {
        "kpi": {
            "total_products": total_products,
            "total_reviews": total_reviews,
            "total_mapped": total_mapped,
            "mapping_rate": round(mapping_rate, 1),
            "avg_price_usd": round(avg_price, 2),
            "price_advantage_pct": round(price_advantage, 1),
            "avg_price_diff_pct": round(avg_price_diff, 1),
        },
        "top_brands": [
            {"brand": b.brand or "Unknown", "count": b.count, "avg_price": round(b.avg_price or 0, 2), "total_reviews": b.total_reviews or 0}
            for b in top_brands
        ],
    }

@app.get("/api/dashboard/category-stats")
def get_category_stats(db: Session = Depends(get_db)):
    cats = db.query(
        Product.parent_category, func.count(Product.id).label("count"),
        func.avg(Product.price_usd).label("avg_price"),
        func.sum(Product.review_count).label("total_reviews"),
    ).group_by(Product.parent_category).order_by(desc("count")).all()
    return [{"category": c.parent_category or "기타", "count": c.count, "avg_price": round(c.avg_price or 0, 2), "total_reviews": c.total_reviews or 0} for c in cats]

@app.get("/api/dashboard/price-distribution")
def get_price_distribution(db: Session = Depends(get_db)):
    ranges = [("$0-10", 0, 10), ("$10-20", 10, 20), ("$20-30", 20, 30), ("$30-50", 30, 50), ("$50+", 50, 9999)]
    return [{"range": label, "count": db.query(Product).filter(Product.price_usd >= lo, Product.price_usd < hi).count()} for label, lo, hi in ranges]

# ── Products API ─────────────────────────────────────────
@app.get("/api/products")
def get_products(
    page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100),
    brand: Optional[str] = None, category: Optional[str] = None,
    search: Optional[str] = None, sort: str = "review_count", order: str = "desc",
    db: Session = Depends(get_db),
):
    query = db.query(Product)
    if brand: query = query.filter(Product.brand.ilike(f"%{brand}%"))
    if category: query = query.filter(Product.parent_category == category)
    if search: query = query.filter((Product.name_ko.ilike(f"%{search}%")) | (Product.name_en.ilike(f"%{search}%")) | (Product.brand.ilike(f"%{search}%")))
    sort_col = getattr(Product, sort, Product.review_count)
    if order == "desc": query = query.order_by(desc(sort_col))
    else: query = query.order_by(sort_col)
    total = query.count()
    products = query.offset((page - 1) * per_page).limit(per_page).all()
    return {"total": total, "page": page, "per_page": per_page, "pages": (total + per_page - 1) // per_page,
        "items": [{"it_id": p.it_id, "name_ko": p.name_ko, "name_en": p.name_en, "brand": p.brand, "price_usd": p.price_usd, "price_krw": p.price_krw, "category": p.parent_category, "sub_category": p.category_name, "review_count": p.review_count, "image_url": p.image_url, "url": p.url, "has_mapping": p.mapping is not None} for p in products]}

@app.get("/api/products/{it_id}")
def get_product_detail(it_id: str, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.it_id == it_id).first()
    if not product: raise HTTPException(404, "Product not found")
    reviews = db.query(Review).filter(Review.product_id == it_id).limit(20).all()
    mapping = None
    if product.mapping:
        m = product.mapping
        mapping = {"iherb_id": m.iherb_id, "iherb_name": m.iherb_name, "iherb_brand": m.iherb_brand, "iherb_price_usd": m.iherb_price_usd, "match_method": m.match_method, "match_score": m.match_score, "price_diff": m.price_diff, "price_diff_pct": m.price_diff_pct, "verified": m.verified}
    return {"product": {"it_id": product.it_id, "name_ko": product.name_ko, "name_en": product.name_en, "brand": product.brand, "price_usd": product.price_usd, "price_krw": product.price_krw, "category": product.parent_category, "sub_category": product.category_name, "review_count": product.review_count, "image_url": product.image_url, "description": product.description, "url": product.url},
        "reviews": [{"reviewer": r.reviewer, "rating": r.rating, "text": r.text, "date": r.date} for r in reviews], "iherb_mapping": mapping}

# ── iHerb Mapping API ────────────────────────────────────
@app.get("/api/mapping")
def get_mappings(page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100), method: Optional[str] = None, verified: Optional[bool] = None, db: Session = Depends(get_db)):
    query = db.query(IHerbMapping).join(Product, Product.it_id == IHerbMapping.ople_id)
    if method: query = query.filter(IHerbMapping.match_method == method)
    if verified is not None: query = query.filter(IHerbMapping.verified == verified)
    total = query.count()
    mappings = query.order_by(desc(IHerbMapping.match_score)).offset((page - 1) * per_page).limit(per_page).all()
    return {"total": total, "page": page, "items": [{"ople_id": m.ople_id, "ople_name": m.product.name_ko if m.product else "", "ople_price": m.product.price_usd if m.product else 0, "iherb_id": m.iherb_id, "iherb_name": m.iherb_name, "iherb_price": m.iherb_price_usd, "match_method": m.match_method, "match_score": m.match_score, "price_diff": m.price_diff, "price_diff_pct": m.price_diff_pct, "verified": m.verified} for m in mappings]}

@app.get("/api/mapping/stats")
def get_mapping_stats(db: Session = Depends(get_db)):
    total = db.query(IHerbMapping).count()
    by_method = db.query(IHerbMapping.match_method, func.count(IHerbMapping.id).label("count")).group_by(IHerbMapping.match_method).all()
    verified_count = db.query(IHerbMapping).filter(IHerbMapping.verified == True).count()
    avg_diff = db.query(func.avg(IHerbMapping.price_diff)).filter(IHerbMapping.price_diff.isnot(None)).scalar() or 0
    ople_cheaper = db.query(IHerbMapping).filter(IHerbMapping.price_diff < 0).count()
    return {"total": total, "verified": verified_count, "by_method": {m.match_method: m.count for m in by_method}, "avg_price_diff": round(avg_diff, 2), "ople_cheaper_count": ople_cheaper, "ople_cheaper_pct": round(ople_cheaper / total * 100, 1) if total > 0 else 0}

@app.put("/api/mapping/{ople_id}/verify")
def verify_mapping(ople_id: str, db: Session = Depends(get_db)):
    mapping = db.query(IHerbMapping).filter(IHerbMapping.ople_id == ople_id).first()
    if not mapping: raise HTTPException(404, "Mapping not found")
    mapping.verified = True
    mapping.updated_at = datetime.utcnow()
    db.commit()
    return {"status": "verified"}

# ── Scrape Jobs API ──────────────────────────────────────
@app.get("/api/jobs")
def get_jobs(db: Session = Depends(get_db)):
    jobs = db.query(ScrapeJob).order_by(desc(ScrapeJob.created_at)).limit(20).all()
    return [{"id": j.id, "type": j.job_type, "status": j.status, "total": j.total_items, "processed": j.processed_items, "error": j.error_message, "started": j.started_at.isoformat() if j.started_at else None, "completed": j.completed_at.isoformat() if j.completed_at else None} for j in jobs]

@app.post("/api/jobs/scrape-ople")
async def start_ople_scrape(background_tasks: BackgroundTasks, max_products: int = 50, db: Session = Depends(get_db)):
    job = ScrapeJob(job_type="ople_products", status="pending", started_at=datetime.utcnow())
    db.add(job)
    db.commit()
    db.refresh(job)
    return {"job_id": job.id, "status": "queued", "message": f"Scraping up to {max_products} products"}

@app.post("/api/jobs/scrape-iherb")
async def start_iherb_scrape(background_tasks: BackgroundTasks, categories: Optional[str] = None, max_products: int = 50, max_pages: int = 3, db: Session = Depends(get_db)):
    cat_list = categories.split(",") if categories else None
    job = ScrapeJob(job_type="iherb_full", status="running", started_at=datetime.utcnow(), config={"categories": cat_list, "max_products": max_products, "max_pages": max_pages})
    db.add(job)
    db.commit()
    db.refresh(job)
    background_tasks.add_task(run_iherb_scrape_job, job.id, cat_list, max_products, max_pages)
    return {"job_id": job.id, "status": "running", "message": f"iHerb scraping started"}

async def run_iherb_scrape_job(job_id, categories, max_products, max_pages):
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))
    db = next(get_db())
    try:
        from iherb_scraper import run_iherb_scrape
        async def progress_cb(processed, total, message):
            job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
            if job:
                job.processed_items = processed
                job.total_items = total
                db.commit()
        result = await run_iherb_scrape(output_dir="data", categories=categories, max_products_per_category=max_products, max_pages_per_category=max_pages, scrape_details=True, progress_callback=progress_cb)
        data_file = Path("data/iherb_products.json")
        if data_file.exists():
            with open(data_file, encoding="utf-8") as f:
                products_data = json.load(f)
            saved = 0
            for p in products_data:
                pid = p.get("product_id") or p.get("iherb_id", "")
                if not pid: continue
                existing = db.query(IHerbProduct).filter((IHerbProduct.product_id == pid) | (IHerbProduct.iherb_id == p.get("iherb_id", ""))).first()
                if existing:
                    for key, val in p.items():
                        if val and hasattr(existing, key): setattr(existing, key, val)
                    existing.updated_at = datetime.utcnow()
                else:
                    product = IHerbProduct(iherb_id=p.get("iherb_id", pid), product_id=pid, url=p.get("url", ""), name=p.get("name", ""), brand=p.get("brand", ""), price_usd=p.get("price_usd", 0), price_original=p.get("price_original", 0), discount_pct=p.get("discount_pct", 0), rating=p.get("rating", 0), review_count=p.get("review_count", 0), category=p.get("category", ""), sub_category=p.get("sub_category", ""), image_url=p.get("image_url", ""), description=p.get("description", ""), ingredients=p.get("ingredients", ""), supplement_facts=p.get("supplement_facts"), suggested_use=p.get("suggested_use", ""), warnings=p.get("warnings", ""), other_ingredients=p.get("other_ingredients", ""), allergen_info=p.get("allergen_info", ""), badges=p.get("badges"), in_stock=p.get("in_stock", True))
                    db.add(product)
                    saved += 1
            db.commit()
        job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
        if job:
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            db.commit()
    except Exception as e:
        job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
        if job:
            job.status = "failed"
            job.error_message = str(e)[:500]
            job.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()

# ── iHerb Products API ───────────────────────────────────
from pydantic import BaseModel
from typing import List

class IHerbProductIn(BaseModel):
    iherb_id: str
    name: str = ""
    name_ko: str = ""
    brand: str = ""
    url: str = ""
    image_url: str = ""
    price_usd: float = 0
    price_krw: float = 0
    price_original: float = 0
    discount_pct: float = 0
    rating: float = 0
    review_count: int = 0
    category: str = ""
    sub_category: str = ""
    category_path: str = ""
    description: str = ""
    suggested_use: str = ""
    other_ingredients: str = ""
    warnings: str = ""
    supplement_facts: str = ""
    ingredients: str = ""
    allergen_info: str = ""
    serving_size: str = ""
    servings_per_container: str = ""
    product_form: str = ""
    count: str = ""
    weight: str = ""
    dimensions: str = ""
    badges: str = "[]"
    in_stock: bool = True
    # Detail fields from individual product page scraping
    top_positive_review: str = ""
    top_critical_review: str = ""
    best_by_date: str = ""
    shipping_weight: str = ""
    upc_barcode: str = ""
    review_tags: str = "[]"
    meta_description: str = ""  # Used for AI review summary

class BulkProductsIn(BaseModel):
    products: List[IHerbProductIn]

# All fields that can be updated in bulk save
_BULK_UPDATE_FIELDS = [
    'name', 'name_ko', 'brand', 'url', 'image_url', 'price_usd', 'price_krw',
    'price_original', 'discount_pct', 'rating', 'review_count', 'category',
    'sub_category', 'category_path', 'description', 'suggested_use',
    'other_ingredients', 'warnings', 'supplement_facts', 'ingredients',
    'allergen_info', 'serving_size', 'servings_per_container', 'product_form',
    'count', 'weight', 'dimensions', 'badges', 'in_stock',
    'top_positive_review', 'top_critical_review', 'best_by_date',
    'shipping_weight', 'upc_barcode', 'review_tags', 'meta_description',
]

@app.post("/api/iherb/products/bulk")
def bulk_save_iherb_products(data: BulkProductsIn, db: Session = Depends(get_db)):
    """Bulk save/update iHerb products from Chrome scraper."""
    saved = 0
    updated = 0
    for p in data.products:
        existing = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == p.iherb_id).first()
        if existing:
            for field in _BULK_UPDATE_FIELDS:
                val = getattr(p, field, None)
                if val is not None and val != "" and val != 0 and val != "[]" and hasattr(existing, field):
                    setattr(existing, field, val)
            existing.updated_at = datetime.utcnow()
            updated += 1
        else:
            new_product = IHerbProduct(
                iherb_id=p.iherb_id, product_id=f"iherb_{p.iherb_id}",
                name=p.name, name_ko=p.name_ko, brand=p.brand, url=p.url,
                image_url=p.image_url, price_usd=p.price_usd, price_krw=p.price_krw,
                price_original=p.price_original, discount_pct=p.discount_pct,
                rating=p.rating, review_count=p.review_count, category=p.category,
                sub_category=p.sub_category, category_path=p.category_path,
                description=p.description, suggested_use=p.suggested_use,
                other_ingredients=p.other_ingredients, warnings=p.warnings,
                supplement_facts=p.supplement_facts, ingredients=p.ingredients,
                allergen_info=p.allergen_info, serving_size=p.serving_size,
                servings_per_container=p.servings_per_container,
                product_form=p.product_form, count=p.count, weight=p.weight,
                dimensions=p.dimensions, badges=p.badges, in_stock=p.in_stock,
                top_positive_review=p.top_positive_review,
                top_critical_review=p.top_critical_review,
                best_by_date=p.best_by_date, shipping_weight=p.shipping_weight,
                upc_barcode=p.upc_barcode, review_tags=p.review_tags if hasattr(IHerbProduct, 'review_tags') else None,
                meta_description=p.meta_description,
                scraped_at=datetime.utcnow(), updated_at=datetime.utcnow(),
            )
            db.add(new_product)
            saved += 1
    db.commit()
    return {"status": "ok", "saved": saved, "updated": updated, "total": saved + updated}

# ── NEW: Product Detail Update Endpoints ─────────────────

class IHerbProductDetailIn(BaseModel):
    """Detail data scraped from individual product pages."""
    description: str = ""
    suggested_use: str = ""
    ingredients: str = ""
    other_ingredients: str = ""
    allergen_info: str = ""
    warnings: str = ""
    supplement_facts: str = ""
    serving_size: str = ""
    servings_per_container: str = ""
    meta_description: str = ""  # AI review summary
    review_tags: str = "[]"
    top_positive_review: str = ""
    top_critical_review: str = ""
    best_by_date: str = ""
    product_form: str = ""
    count: str = ""
    weight: str = ""
    dimensions: str = ""
    shipping_weight: str = ""
    upc_barcode: str = ""

@app.put("/api/iherb/products/{iherb_id}/details")
def update_iherb_product_details(iherb_id: str, data: IHerbProductDetailIn, db: Session = Depends(get_db)):
    """Update product details from individual page scraping."""
    product = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == iherb_id).first()
    if not product:
        raise HTTPException(404, f"Product {iherb_id} not found")
    updated_fields = 0
    for field, value in data.dict().items():
        if value and value != "" and value != "[]":
            if hasattr(product, field):
                setattr(product, field, value)
                updated_fields += 1
    product.updated_at = datetime.utcnow()
    db.commit()
    return {"status": "ok", "iherb_id": iherb_id, "updated_fields": updated_fields}

class BulkDetailIn(BaseModel):
    details: List[dict]

@app.post("/api/iherb/products/bulk-details")
def bulk_update_details(data: BulkDetailIn, db: Session = Depends(get_db)):
    """Bulk update product details from scraping."""
    updated = 0
    not_found = 0
    for item in data.details:
        iherb_id = item.get("iherb_id", "")
        if not iherb_id: continue
        product = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == iherb_id).first()
        if not product:
            not_found += 1
            continue
        for field, value in item.items():
            if field == "iherb_id": continue
            if value and value != "" and value != "[]" and hasattr(product, field):
                setattr(product, field, value)
        product.updated_at = datetime.utcnow()
        updated += 1
    db.commit()
    return {"status": "ok", "updated": updated, "not_found": not_found}

@app.post("/api/iherb/products/bulk-text")
async def bulk_save_text(request: Request, db: Session = Depends(get_db)):
    """Accept product data as text/plain (for sendBeacon CORS workaround)."""
    body = await request.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON in request body")
    products = data.get("products", [])
    saved = 0
    updated = 0
    for p in products:
        iherb_id = p.get("iherb_id", "")
        if not iherb_id: continue
        existing = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == iherb_id).first()
        if existing:
            for field in _BULK_UPDATE_FIELDS:
                val = p.get(field)
                if val is not None and val != "" and val != 0 and val != "[]" and hasattr(existing, field):
                    setattr(existing, field, val)
            existing.updated_at = datetime.utcnow()
            updated += 1
        else:
            new_product = IHerbProduct(
                iherb_id=iherb_id, product_id=p.get("product_id", f"iherb_{iherb_id}"),
                name=p.get("name", ""), brand=p.get("brand", ""), url=p.get("url", ""),
                image_url=p.get("image_url", ""), price_usd=p.get("price_usd", 0),
                price_original=p.get("price_original", 0), discount_pct=p.get("discount_pct", 0),
                rating=p.get("rating", 0), review_count=p.get("review_count", 0),
                category=p.get("category", ""), sub_category=p.get("sub_category", ""),
                badges=p.get("badges", "[]"), in_stock=p.get("in_stock", True),
                scraped_at=datetime.utcnow(), updated_at=datetime.utcnow(),
            )
            db.add(new_product)
            saved += 1
    db.commit()
    return {"status": "ok", "saved": saved, "updated": updated, "total": saved + updated}

# ── iHerb Products Read API ──────────────────────────────

@app.get("/api/iherb/products")
def get_iherb_products(
    page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100),
    brand: Optional[str] = None, category: Optional[str] = None,
    search: Optional[str] = None, in_stock: Optional[bool] = None,
    min_rating: Optional[float] = None, has_details: Optional[bool] = None,
    sort: str = "review_count", order: str = "desc",
    db: Session = Depends(get_db),
):
    """iHerb product list with filtering and pagination."""
    query = db.query(IHerbProduct)
    if brand: query = query.filter(IHerbProduct.brand.ilike(f"%{brand}%"))
    if category: query = query.filter((IHerbProduct.category.ilike(f"%{category}%")) | (IHerbProduct.category_path.ilike(f"%{category}%")))
    if search: query = query.filter((IHerbProduct.name.ilike(f"%{search}%")) | (IHerbProduct.brand.ilike(f"%{search}%")) | (IHerbProduct.iherb_id.ilike(f"%{search}%")))
    if in_stock is not None: query = query.filter(IHerbProduct.in_stock == in_stock)
    if min_rating: query = query.filter(IHerbProduct.rating >= min_rating)
    if has_details is True: query = query.filter(IHerbProduct.description != "", IHerbProduct.description.isnot(None))
    if has_details is False: query = query.filter((IHerbProduct.description == "") | (IHerbProduct.description.is_(None)))
    sort_col = getattr(IHerbProduct, sort, IHerbProduct.review_count)
    if order == "desc": query = query.order_by(desc(sort_col))
    else: query = query.order_by(sort_col)
    total = query.count()
    products = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "total": total, "page": page, "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "products": [{
            "iherb_id": p.iherb_id, "product_id": p.product_id, "name": p.name,
            "brand": p.brand, "price_usd": p.price_usd, "price_original": p.price_original,
            "discount_pct": p.discount_pct, "rating": p.rating, "review_count": p.review_count,
            "image_url": p.image_url, "category": p.category, "sub_category": p.sub_category,
            "in_stock": p.in_stock, "product_form": p.product_form, "count": p.count,
            "has_details": bool(p.description),
            "badges": json.loads(p.badges) if isinstance(p.badges, str) else (p.badges or []),
            "url": p.url,
        } for p in products],
    }

@app.get("/api/iherb/products/{product_id}")
def get_iherb_product_detail(product_id: str, db: Session = Depends(get_db)):
    """Full iHerb product detail with ALL collected information."""
    product = db.query(IHerbProduct).filter((IHerbProduct.product_id == product_id) | (IHerbProduct.iherb_id == product_id)).first()
    if not product: raise HTTPException(404, "iHerb product not found")
    return {
        "basic": {"iherb_id": product.iherb_id, "product_id": product.product_id, "name": product.name, "name_ko": getattr(product, 'name_ko', ''), "subtitle": getattr(product, 'subtitle', ''), "brand": product.brand, "brand_url": getattr(product, 'brand_url', ''), "url": product.url},
        "pricing": {"price_usd": product.price_usd, "price_original": product.price_original, "discount_pct": product.discount_pct, "price_per_unit": getattr(product, 'price_per_unit', ''), "in_stock": product.in_stock, "stock_status": getattr(product, 'stock_status', '')},
        "images": {"main": product.image_url, "thumbnail": getattr(product, 'thumbnail_url', ''), "all": getattr(product, 'image_urls', None) or []},
        "rating": {"average": product.rating, "count": product.review_count, "distribution": getattr(product, 'rating_distribution', None), "top_positive": getattr(product, 'top_positive_review', ''), "top_critical": getattr(product, 'top_critical_review', '')},
        "description": {"text": product.description, "features": getattr(product, 'features', None), "suggested_use": product.suggested_use, "warnings": product.warnings, "storage": getattr(product, 'storage_info', '')},
        "nutrition": {"supplement_facts": product.supplement_facts, "ingredients": product.ingredients, "ingredients_list": getattr(product, 'ingredients_list', None), "other_ingredients": product.other_ingredients, "allergen_info": product.allergen_info, "serving_size": product.serving_size, "servings_per_container": product.servings_per_container},
        "specs": {"product_form": product.product_form, "count": product.count, "weight": product.weight, "dimensions": product.dimensions, "upc_barcode": product.upc_barcode, "sku": getattr(product, 'sku', ''), "shipping_weight": product.shipping_weight},
        "certifications": {"badges": json.loads(product.badges) if isinstance(product.badges, str) else (product.badges or []), "certifications": getattr(product, 'certifications', None), "best_by_date": product.best_by_date},
        "category": {"category": product.category, "sub_category": product.sub_category, "path": product.category_path},
        "reviews": {"summary": getattr(product, 'meta_description', ''), "tags": json.loads(getattr(product, 'review_tags', '[]') or '[]') if isinstance(getattr(product, 'review_tags', '[]'), str) else (getattr(product, 'review_tags', None) or []), "data": getattr(product, 'reviews_data', None)},
        "meta": {"scraped_at": product.scraped_at.isoformat() if product.scraped_at else None, "updated_at": product.updated_at.isoformat() if product.updated_at else None},
    }

# ── iHerb Stats API ──────────────────────────────────────
@app.get("/api/iherb/stats")
def get_iherb_stats(db: Session = Depends(get_db)):
    total = db.query(IHerbProduct).count()
    in_stock = db.query(IHerbProduct).filter(IHerbProduct.in_stock == True).count()
    with_details = db.query(IHerbProduct).filter(IHerbProduct.description != "", IHerbProduct.description.isnot(None)).count()
    by_category = db.query(IHerbProduct.category, func.count(IHerbProduct.id).label("count"), func.avg(IHerbProduct.price_usd).label("avg_price"), func.avg(IHerbProduct.rating).label("avg_rating")).group_by(IHerbProduct.category).order_by(desc("count")).limit(20).all()
    by_brand = db.query(IHerbProduct.brand, func.count(IHerbProduct.id).label("count"), func.avg(IHerbProduct.price_usd).label("avg_price"), func.avg(IHerbProduct.rating).label("avg_rating")).group_by(IHerbProduct.brand).order_by(desc("count")).limit(20).all()
    top_rated = db.query(IHerbProduct).filter(IHerbProduct.review_count >= 100, IHerbProduct.rating > 0).order_by(desc(IHerbProduct.rating)).limit(10).all()
    return {
        "total_products": total, "in_stock": in_stock, "with_details": with_details,
        "by_category": [{"category": c.category or "기타", "count": c.count, "avg_price": round(c.avg_price or 0, 2), "avg_rating": round(c.avg_rating or 0, 1)} for c in by_category],
        "by_brand": [{"brand": b.brand or "Unknown", "count": b.count, "avg_price": round(b.avg_price or 0, 2), "avg_rating": round(b.avg_rating or 0, 1)} for b in by_brand],
        "top_rated": [{"name": p.name, "brand": p.brand, "rating": p.rating, "review_count": p.review_count, "price_usd": p.price_usd, "image_url": p.image_url} for p in top_rated],
    }

# ── Analytics API ─────────────────────────────────────────
@app.get("/api/analytics/brand-comparison")
def brand_comparison(db: Session = Depends(get_db)):
    results = db.query(Product.brand, func.count(IHerbMapping.id).label("mapped_count"), func.avg(Product.price_usd).label("avg_ople_price"), func.avg(IHerbMapping.iherb_price_usd).label("avg_iherb_price"), func.avg(IHerbMapping.price_diff_pct).label("avg_diff_pct")).join(IHerbMapping, Product.it_id == IHerbMapping.ople_id).group_by(Product.brand).having(func.count(IHerbMapping.id) >= 3).order_by(desc("mapped_count")).limit(15).all()
    return [{"brand": r.brand or "Unknown", "mapped_count": r.mapped_count, "avg_ople_price": round(r.avg_ople_price or 0, 2), "avg_iherb_price": round(r.avg_iherb_price or 0, 2), "avg_diff_pct": round(r.avg_diff_pct or 0, 1)} for r in results]

@app.get("/api/analytics/review-keywords")
def review_keywords(db: Session = Depends(get_db)):
    reviews = db.query(Review.keywords).filter(Review.keywords.isnot(None)).limit(1000).all()
    keyword_counts = {}
    for r in reviews:
        if r.keywords:
            for kw in r.keywords: keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
    sorted_kw = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    return [{"keyword": k, "count": v} for k, v in sorted_kw]

# ── Demo Data Seeding ────────────────────────────────────
def seed_demo_data(db: Session):
    demo_products = [
        {"it_id": "1319032894", "name_ko": "Now Foods 울트라 오메가-3, 180캡슐", "name_en": "Now Foods Ultra Omega-3 180 Softgels", "brand": "Now Foods", "price_usd": 25.99, "price_krw": 39115, "review_count": 1473, "parent_category": "건강식품", "category_name": "오메가3/피쉬오일"},
        {"it_id": "1505216341", "name_ko": "Solgar 콜라겐 히알루론산, 30정", "name_en": "Solgar Collagen Hyaluronic Acid Complex", "brand": "Solgar", "price_usd": 14.99, "price_krw": 22560, "review_count": 607, "parent_category": "뷰티용품", "category_name": "콜라겐"},
        {"it_id": "1407165807", "name_ko": "Doctor's Best 루테인+루트맥스 20mg, 180캡슐", "name_en": "Doctor's Best Lutein with FloraGlo 20mg", "brand": "Doctor's Best", "price_usd": 26.99, "price_krw": 40620, "review_count": 954, "parent_category": "건강식품", "category_name": "눈건강"},
        {"it_id": "1511431863", "name_ko": "Jarrow Formulas 비건 MSM 1000mg, 200캡슐", "name_en": "Jarrow Formulas Vegan MSM 1000mg", "brand": "Jarrow Formulas", "price_usd": 25.99, "price_krw": 39115, "review_count": 1106, "parent_category": "건강식품", "category_name": "관절건강"},
        {"it_id": "1417406111", "name_ko": "Solgar 글루코사민+콘드로이친+MSM, 120정", "name_en": "Solgar Glucosamine Chondroitin MSM", "brand": "Solgar", "price_usd": 28.99, "price_krw": 43630, "review_count": 156, "parent_category": "건강식품", "category_name": "관절건강"},
        {"it_id": "1511224023", "name_ko": "Solgar 마그네슘+비타민B6, 250정", "name_en": "Solgar Magnesium with Vitamin B6", "brand": "Solgar", "price_usd": 11.99, "price_krw": 18045, "review_count": 204, "parent_category": "건강식품", "category_name": "미네랄"},
        {"it_id": "1510428215", "name_ko": "Solgar 에스터-C 비타민C 1000mg, 180정", "name_en": "Solgar Ester-C Plus Vitamin C 1000mg", "brand": "Solgar", "price_usd": 21.59, "price_krw": 32493, "review_count": 445, "parent_category": "건강식품", "category_name": "비타민C"},
        {"it_id": "1511818877", "name_ko": "Doctor's Best 비타민 D3 2000IU, 180캡슐", "name_en": "Doctor's Best Vitamin D3 2000IU", "brand": "Doctor's Best", "price_usd": 7.99, "price_krw": 12025, "review_count": 110, "parent_category": "건강식품", "category_name": "비타민D"},
        {"it_id": "1505100130", "name_ko": "Now Foods L-아르기닌 1000mg, 120정", "name_en": "Now Foods L-Arginine 1000mg", "brand": "Now Foods", "price_usd": 13.99, "price_krw": 21055, "review_count": 440, "parent_category": "건강식품", "category_name": "아미노산"},
        {"it_id": "1417406120", "name_ko": "Solgar 비오틴 10000mcg, 60캡슐", "name_en": "Solgar Biotin 10000mcg", "brand": "Solgar", "price_usd": 13.99, "price_krw": 21055, "review_count": 70, "parent_category": "뷰티용품", "category_name": "비오틴"},
        {"it_id": "1672906957", "name_ko": "Jarrow Formulas 비건 펨 도피러스 유산균, 30캡슐", "name_en": "Jarrow Formulas Fem Dophilus", "brand": "Jarrow Formulas", "price_usd": 22.99, "price_krw": 34600, "review_count": 145, "parent_category": "건강식품", "category_name": "유산균"},
        {"it_id": "1513534579", "name_ko": "Double Wood 포스파티딜세린 300mg, 120캡슐", "name_en": "Double Wood Phosphatidylserine 300mg", "brand": "Double Wood", "price_usd": 14.29, "price_krw": 21507, "review_count": 230, "parent_category": "건강식품", "category_name": "두뇌건강"},
        {"it_id": "1672905857", "name_ko": "Swanson 유산균 가세리 30억, 60캡슐", "name_en": "Swanson L. Gasseri 3 Billion", "brand": "Swanson", "price_usd": 12.99, "price_krw": 19550, "review_count": 187, "parent_category": "헬스/다이어트", "category_name": "다이어트"},
        {"it_id": "1511560477", "name_ko": "Absonutrix 시서스 1600mg, 120캡슐", "name_en": "Absonutrix Cissus 1600mg", "brand": "Absonutrix", "price_usd": 19.99, "price_krw": 30085, "review_count": 268, "parent_category": "헬스/다이어트", "category_name": "다이어트"},
        {"it_id": "1510480064", "name_ko": "Jarrow Formulas 비건 MSM 파우더, 1kg", "name_en": "Jarrow Formulas MSM Powder 1kg", "brand": "Jarrow Formulas", "price_usd": 38.99, "price_krw": 58679, "review_count": 604, "parent_category": "건강식품", "category_name": "관절건강"},
        {"it_id": "1268694786", "name_ko": "Solgar 루테인 20mg, 60캡슐", "name_en": "Solgar Lutein 20mg", "brand": "Solgar", "price_usd": 15.59, "price_krw": 23463, "review_count": 79, "parent_category": "건강식품", "category_name": "눈건강"},
        {"it_id": "1505230641", "name_ko": "Solgar 칼슘+마그네슘+아연, 250정", "name_en": "Solgar Calcium Magnesium Zinc", "brand": "Solgar", "price_usd": 13.99, "price_krw": 21055, "review_count": 300, "parent_category": "건강식품", "category_name": "미네랄"},
        {"it_id": "1334058959", "name_ko": "Nature's Way 얼라이브 맨즈 50+ 멀티비타민, 60정", "name_en": "Nature's Way Alive Men's 50+ Multi", "brand": "Nature's Way", "price_usd": 17.99, "price_krw": 27075, "review_count": 480, "parent_category": "건강식품", "category_name": "종합비타민"},
    ]
    for p in demo_products:
        db.add(Product(it_id=p["it_id"], name_ko=p["name_ko"], name_en=p["name_en"], brand=p["brand"], price_usd=p["price_usd"], price_krw=p["price_krw"], review_count=p["review_count"], parent_category=p["parent_category"], category_name=p["category_name"], url=f"https://www.ople.com/mall5/shop/item.php?it_id={p['it_id']}", image_url=f"https://img.ople.com/data/item/{p['it_id'][:4]}/{p['it_id']}.jpg"))

    demo_mappings = [
        {"ople_id": "1319032894", "iherb_id": "NOW-01652", "iherb_name": "Now Foods Ultra Omega-3 180 Softgels", "iherb_brand": "Now Foods", "iherb_price_usd": 28.49, "match_method": "upc", "match_score": 99.0, "price_diff": -2.50, "price_diff_pct": -8.8},
        {"ople_id": "1505216341", "iherb_id": "SOL-01736", "iherb_name": "Solgar Collagen Hyaluronic Acid Complex", "iherb_brand": "Solgar", "iherb_price_usd": 16.99, "match_method": "fuzzy", "match_score": 92.0, "price_diff": -2.00, "price_diff_pct": -11.8},
        {"ople_id": "1407165807", "iherb_id": "DRB-00369", "iherb_name": "Doctor's Best Lutein 20mg 180 Softgels", "iherb_brand": "Doctor's Best", "iherb_price_usd": 29.99, "match_method": "fuzzy", "match_score": 88.0, "price_diff": -3.00, "price_diff_pct": -10.0},
        {"ople_id": "1511431863", "iherb_id": "JRW-18005", "iherb_name": "Jarrow Formulas MSM 1000mg 200 Capsules", "iherb_brand": "Jarrow Formulas", "iherb_price_usd": 27.99, "match_method": "upc", "match_score": 99.0, "price_diff": -2.00, "price_diff_pct": -7.1},
        {"ople_id": "1510428215", "iherb_id": "SOL-01050", "iherb_name": "Solgar Ester-C Plus 1000mg Vitamin C", "iherb_brand": "Solgar", "iherb_price_usd": 24.29, "match_method": "fuzzy", "match_score": 85.0, "price_diff": -2.70, "price_diff_pct": -11.1},
        {"ople_id": "1505100130", "iherb_id": "NOW-00033", "iherb_name": "Now Foods L-Arginine 1000mg 120 Tablets", "iherb_brand": "Now Foods", "iherb_price_usd": 15.99, "match_method": "upc", "match_score": 99.0, "price_diff": -2.00, "price_diff_pct": -12.5},
        {"ople_id": "1334058959", "iherb_id": "NWY-15691", "iherb_name": "Nature's Way Alive Men's 50+ Multi", "iherb_brand": "Nature's Way", "iherb_price_usd": 19.99, "match_method": "fuzzy", "match_score": 91.0, "price_diff": -2.00, "price_diff_pct": -10.0},
        {"ople_id": "1511224023", "iherb_id": "SOL-01731", "iherb_name": "Solgar Magnesium with Vitamin B6", "iherb_brand": "Solgar", "iherb_price_usd": 13.99, "match_method": "fuzzy", "match_score": 87.0, "price_diff": -2.00, "price_diff_pct": -14.3},
    ]
    for m in demo_mappings:
        db.add(IHerbMapping(ople_id=m["ople_id"], iherb_id=m["iherb_id"], iherb_name=m["iherb_name"], iherb_brand=m["iherb_brand"], iherb_price_usd=m["iherb_price_usd"], match_method=m["match_method"], match_score=m["match_score"], price_diff=m["price_diff"], price_diff_pct=m["price_diff_pct"], verified=m["match_method"] == "upc"))

    demo_reviews = [
        {"product_id": "1319032894", "reviewer": "건강맘***", "rating": 5, "text": "오메가3 먹고 나서 피로가 확 줄었어요.", "date": "2024-12-15", "keywords": ["피로", "눈건강", "오메가3"]},
        {"product_id": "1319032894", "reviewer": "운동매***", "rating": 5, "text": "운동 후 회복이 빨라진 것 같아요.", "date": "2024-11-20", "keywords": ["운동", "회복", "관절"]},
        {"product_id": "1505216341", "reviewer": "뷰티러***", "rating": 4, "text": "콜라겐 한달 먹었는데 피부 탄력이 좋아진 느낌이에요", "date": "2024-12-01", "keywords": ["콜라겐", "피부", "탄력"]},
        {"product_id": "1407165807", "reviewer": "직장인***", "rating": 5, "text": "루테인 먹고 나서 눈 피로가 확실히 줄었습니다.", "date": "2025-01-10", "keywords": ["루테인", "눈피로", "직장인"]},
        {"product_id": "1511431863", "reviewer": "등산러***", "rating": 5, "text": "MSM 관절에 정말 좋아요.", "date": "2024-10-25", "keywords": ["MSM", "관절", "무릎", "통증"]},
        {"product_id": "1510428215", "reviewer": "면역전***", "rating": 5, "text": "비타민C 에스터C가 위에 부담 없어서 좋아요.", "date": "2024-11-15", "keywords": ["비타민C", "면역", "감기", "위장"]},
    ]
    for r in demo_reviews:
        db.add(Review(product_id=r["product_id"], reviewer=r["reviewer"], rating=r["rating"], text=r["text"], date=r["date"], keywords=r["keywords"]))

    db.commit()
    print(f"Seeded {len(demo_products)} products, {len(demo_mappings)} mappings, {len(demo_reviews)} reviews")

# ── Run ──────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
