"""
IT.OPLE — FastAPI Backend
OPLE 상품/리뷰 분석 & iHerb 매핑 인트라넷
"""
import os
import sys
import json
import asyncio
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, case
import httpx
from jose import jwt, JWTError

from database import init_db, get_db, Product, Review, IHerbMapping, IHerbProduct, ScrapeJob, User, SessionLocal

# Add scraper module to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scraper"))
from iherb_scraper import run_iherb_scrape, SUPPLEMENT_CATEGORIES

# ── Auth Configuration ───────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "ople-dev-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24 * 7  # 7 days
ADMIN_EMAILS = os.getenv("ADMIN_EMAILS", "xwine.ai@gmail.com").split(",")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "3242")

def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    """Dependency to get current user from JWT token."""
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        scheme, token = auth_header.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid auth scheme")

        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id: int = int(payload.get("sub")) if payload.get("sub") is not None else None
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        if not user.is_active:
            raise HTTPException(status_code=403, detail="User is inactive")

        return user
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ── App Setup ────────────────────────────────────────────
app = FastAPI(title="IT.OPLE", version="1.1.0", description="OPLE 상품 분석 & iHerb 매핑 인트라넷")

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


@app.get("/spec", response_class=HTMLResponse)
async def spec():
    spec_path = static_dir / "spec.html"
    if spec_path.exists():
        return spec_path.read_text(encoding="utf-8")
    return "<h1>Spec not found</h1>"


@app.get("/ople-spec", response_class=HTMLResponse)
async def ople_spec():
    spec_path = static_dir / "ople_spec.html"
    if spec_path.exists():
        return spec_path.read_text(encoding="utf-8")
    return "<h1>OPLE Spec not found</h1>"


@app.get("/iherb-comparison", response_class=HTMLResponse)
async def iherb_comparison():
    spec_path = static_dir / "iherb_comparison.html"
    if spec_path.exists():
        return spec_path.read_text(encoding="utf-8")
    return "<h1>Comparison not found</h1>"


@app.get("/iherb-shopify-flow", response_class=HTMLResponse)
async def iherb_shopify_flow():
    spec_path = static_dir / "iherb_shopify_flow.html"
    if spec_path.exists():
        return spec_path.read_text(encoding="utf-8")
    return "<h1>Flow not found</h1>"


@app.get("/ingredients", response_class=HTMLResponse)
async def ingredients_page():
    spec_path = static_dir / "ingredients.html"
    if spec_path.exists():
        return spec_path.read_text(encoding="utf-8")
    return "<h1>Ingredients viewer not found</h1>"


@app.get("/ops-dashboard")
async def ops_dashboard():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/", status_code=302)


@app.get("/api/ingredients/{key}")
async def get_ingredient(key: str):
    """Serve unified ingredient JSON from pilot ETL output."""
    import json
    from fastapi import HTTPException
    data_path = static_dir / "data" / "pilot_etl" / f"{key}_unified.json"
    if not data_path.exists():
        raise HTTPException(status_code=404, detail=f"ingredient '{key}' not found")
    return json.loads(data_path.read_text(encoding="utf-8"))


@app.get("/api/ingredients")
async def list_ingredients():
    """List available ingredients."""
    data_dir = static_dir / "data" / "pilot_etl"
    if not data_dir.exists():
        return {"ingredients": []}
    keys = sorted([
        p.stem.replace("_unified", "")
        for p in data_dir.glob("*_unified.json")
    ])
    return {"ingredients": keys, "count": len(keys)}

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
        Product.brand, func.count(Product.id).label("count"),
        func.avg(Product.price_usd).label("avg_price"),
        func.sum(Product.review_count).label("total_reviews"),
    ).group_by(Product.brand).order_by(desc("total_reviews")).limit(10).all()
    return {
        "kpi": {
            "total_products": total_products, "total_reviews": total_reviews,
            "total_mapped": total_mapped, "mapping_rate": round(mapping_rate, 1),
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
    return [
        {"category": c.parent_category or "기타", "count": c.count, "avg_price": round(c.avg_price or 0, 2), "total_reviews": c.total_reviews or 0}
        for c in cats
    ]

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
    if search:
        query = query.filter(
            (Product.name_ko.ilike(f"%{search}%")) | (Product.name_en.ilike(f"%{search}%")) | (Product.brand.ilike(f"%{search}%"))
        )
    sort_col = getattr(Product, sort, Product.review_count)
    if order == "desc": query = query.order_by(desc(sort_col))
    else: query = query.order_by(sort_col)
    total = query.count()
    products = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "total": total, "page": page, "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "items": [
            {"it_id": p.it_id, "name_ko": p.name_ko, "name_en": p.name_en, "brand": p.brand,
             "price_usd": p.price_usd, "price_krw": p.price_krw, "category": p.parent_category,
             "sub_category": p.category_name, "review_count": p.review_count, "image_url": p.image_url,
             "url": p.url, "has_mapping": p.mapping is not None}
            for p in products
        ],
    }

@app.get("/api/products/{it_id}")
def get_product_detail(it_id: str, db: Session = Depends(get_db)):
    product = db.query(Product).filter(Product.it_id == it_id).first()
    if not product: raise HTTPException(404, "Product not found")
    reviews = db.query(Review).filter(Review.product_id == it_id).limit(20).all()
    mapping = None
    if product.mapping:
        m = product.mapping
        mapping = {"iherb_id": m.iherb_id, "iherb_name": m.iherb_name, "iherb_brand": m.iherb_brand,
                   "iherb_price_usd": m.iherb_price_usd, "match_method": m.match_method,
                   "match_score": m.match_score, "price_diff": m.price_diff,
                   "price_diff_pct": m.price_diff_pct, "verified": m.verified}
    return {
        "product": {"it_id": product.it_id, "name_ko": product.name_ko, "name_en": product.name_en,
                    "brand": product.brand, "price_usd": product.price_usd, "price_krw": product.price_krw,
                    "category": product.parent_category, "sub_category": product.category_name,
                    "review_count": product.review_count, "image_url": product.image_url,
                    "description": product.description, "url": product.url},
        "reviews": [{"reviewer": r.reviewer, "rating": r.rating, "text": r.text, "date": r.date} for r in reviews],
        "iherb_mapping": mapping,
    }

# ── iHerb Mapping API ────────────────────────────────────
@app.get("/api/mapping")
def get_mappings(
    page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100),
    method: Optional[str] = None, verified: Optional[bool] = None,
    db: Session = Depends(get_db),
):
    query = db.query(IHerbMapping).join(Product, Product.it_id == IHerbMapping.ople_id)
    if method: query = query.filter(IHerbMapping.match_method == method)
    if verified is not None: query = query.filter(IHerbMapping.verified == verified)
    total = query.count()
    mappings = query.order_by(desc(IHerbMapping.match_score)).offset((page - 1) * per_page).limit(per_page).all()
    return {
        "total": total, "page": page,
        "items": [
            {"ople_id": m.ople_id, "ople_name": m.product.name_ko if m.product else "",
             "ople_price": m.product.price_usd if m.product else 0,
             "iherb_id": m.iherb_id, "iherb_name": m.iherb_name, "iherb_price": m.iherb_price_usd,
             "match_method": m.match_method, "match_score": m.match_score,
             "price_diff": m.price_diff, "price_diff_pct": m.price_diff_pct, "verified": m.verified}
            for m in mappings
        ],
    }

@app.get("/api/mapping/stats")
def get_mapping_stats(db: Session = Depends(get_db)):
    total = db.query(IHerbMapping).count()
    by_method = db.query(IHerbMapping.match_method, func.count(IHerbMapping.id).label("count")).group_by(IHerbMapping.match_method).all()
    verified_count = db.query(IHerbMapping).filter(IHerbMapping.verified == True).count()
    avg_diff = db.query(func.avg(IHerbMapping.price_diff)).filter(IHerbMapping.price_diff.isnot(None)).scalar() or 0
    ople_cheaper = db.query(IHerbMapping).filter(IHerbMapping.price_diff < 0).count()
    return {
        "total": total, "verified": verified_count,
        "by_method": {m.match_method: m.count for m in by_method},
        "avg_price_diff": round(avg_diff, 2),
        "ople_cheaper_count": ople_cheaper,
        "ople_cheaper_pct": round(ople_cheaper / total * 100, 1) if total > 0 else 0,
    }

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
    return [
        {"id": j.id, "type": j.job_type, "status": j.status, "total": j.total_items,
         "processed": j.processed_items, "error": j.error_message,
         "started": j.started_at.isoformat() if j.started_at else None,
         "completed": j.completed_at.isoformat() if j.completed_at else None}
        for j in jobs
    ]

@app.get("/api/jobs/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job.id, "type": job.job_type, "status": job.status,
        "total_items": job.total_items, "processed_items": job.processed_items,
        "error_message": job.error_message,
        "started": job.started_at.isoformat() if job.started_at else None,
        "completed": job.completed_at.isoformat() if job.completed_at else None,
    }

@app.post("/api/jobs/scrape-ople")
async def start_ople_scrape(background_tasks: BackgroundTasks, max_products: int = 50, db: Session = Depends(get_db)):
    job = ScrapeJob(job_type="ople_products", status="pending", started_at=datetime.utcnow())
    db.add(job)
    db.commit()
    db.refresh(job)
    return {"job_id": job.id, "status": "queued", "message": f"Scraping up to {max_products} products"}

# ── iHerb Scraping Background Task ─────────────────────
async def _run_iherb_scrape_task(job_id: int, categories: list, max_products: int):
    """Background task that runs the iHerb scraper and saves results to DB."""
    db = SessionLocal()
    try:
        job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
        job.status = "running"
        job.started_at = datetime.utcnow()
        db.commit()

        async def progress_cb(processed, total, message):
            nonlocal db, job
            try:
                db.refresh(job)
                job.processed_items = processed
                if total > 0:
                    job.total_items = total
                db.commit()
            except Exception:
                pass

        # Map frontend category keys to scraper keys
        cat_map = {
            "vitamins": "vitamins", "minerals": "minerals",
            "omega_fish_oil": "fish-oil-omegas", "probiotics": "probiotics",
            "protein": "protein", "amino_acids": "amino-acids",
            "herbs": "herbs-homeopathy", "antioxidants": "antioxidants",
            "joint_bone": "bone-joint", "digestive": "digestive-support",
            "immune": "immune-support", "sleep": "sleep",
            "energy": "energy", "weight_management": "weight-management",
            "beauty": "collagen", "mens_health": "mens-health",
            "womens_health": "womens-health", "children": "childrens-health",
            "greens_superfoods": "superfoods", "sports_nutrition": "sports-nutrition",
        }
        scraper_cats = None
        if categories:
            scraper_cats = [cat_map.get(c, c) for c in categories if cat_map.get(c, c) in SUPPLEMENT_CATEGORIES]
            if not scraper_cats:
                scraper_cats = None

        max_per_cat = max_products if max_products > 0 else None

        await run_iherb_scrape(
            output_dir="data",
            categories=scraper_cats,
            max_products_per_category=max_per_cat,
            max_pages_per_category=5 if max_per_cat and max_per_cat <= 100 else 10,
            scrape_details=True,
            scrape_korean=True,
            progress_callback=progress_cb,
        )

        # Load results and save to database
        results_file = Path("data/iherb_products.json")
        if results_file.exists():
            with open(results_file, encoding="utf-8") as f:
                products = json.load(f)

            saved_count = 0
            for p in products:
                pid = p.get("product_id", "")
                if not pid:
                    continue

                existing = db.query(IHerbProduct).filter(IHerbProduct.product_id == str(pid)).first()
                if existing:
                    # Update existing
                    for key in ["name", "name_ko", "brand", "price_usd", "price_krw", "rating",
                                "review_count", "image_url", "description", "suggested_use",
                                "other_ingredients", "warnings", "badges", "category", "sub_category"]:
                        val = p.get(key)
                        if val is not None and val != "":
                            setattr(existing, key, val)
                    saved_count += 1
                else:
                    iherb_prod = IHerbProduct(
                        iherb_id=p.get("iherb_id", ""),
                        product_id=str(pid),
                        name=p.get("name", ""),
                        name_ko=p.get("name_ko", ""),
                        brand=p.get("brand", ""),
                        price_usd=p.get("price_usd"),
                        price_krw=p.get("price_krw"),
                        rating=p.get("rating"),
                        review_count=p.get("review_count"),
                        category=p.get("category", ""),
                        sub_category=p.get("sub_category", ""),
                        product_form=p.get("product_form", ""),
                        count=p.get("count", ""),
                        in_stock=p.get("in_stock", True),
                        url=p.get("url", ""),
                        image_url=p.get("image_url", ""),
                        description=p.get("description", ""),
                        suggested_use=p.get("suggested_use", ""),
                        other_ingredients=p.get("other_ingredients", ""),
                        warnings=p.get("warnings", ""),
                        badges=p.get("badges", []),
                    )
                    db.add(iherb_prod)
                    saved_count += 1

            db.commit()

            job.processed_items = saved_count
            job.total_items = len(products)
            job.status = "completed"
            job.completed_at = datetime.utcnow()
            db.commit()
            print(f"[Scraper] Completed: {saved_count} products saved to DB")
        else:
            job.status = "failed"
            job.error_message = "No results file generated"
            job.completed_at = datetime.utcnow()
            db.commit()

    except Exception as e:
        traceback.print_exc()
        try:
            job = db.query(ScrapeJob).filter(ScrapeJob.id == job_id).first()
            if job:
                job.status = "failed"
                job.error_message = str(e)[:500]
                job.completed_at = datetime.utcnow()
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


@app.post("/api/jobs/scrape-iherb")
async def start_iherb_scrape(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    max_products = body.get("max_products", 50)
    categories = body.get("categories", [])

    job = ScrapeJob(
        job_type="iherb_full",
        status="pending",
        config={"max_products": max_products, "categories": categories},
        started_at=datetime.utcnow(),
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Launch scraper in background (use asyncio.create_task for async coroutine)
    asyncio.create_task(_run_iherb_scrape_task(job.id, categories, max_products))

    return {"job_id": job.id, "status": "queued", "message": f"iHerb scraping started (max {max_products} products)"}

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
    product_form: str = ""
    count: str = ""
    weight: str = ""
    dimensions: str = ""
    badges: str = "[]"
    in_stock: bool = True

class BulkProductsIn(BaseModel):
    products: List[IHerbProductIn]

class IHerbDetailIn(BaseModel):
    iherb_id: str
    description: str = ""
    suggested_use: str = ""
    ingredients: str = ""
    other_ingredients: str = ""
    allergen_info: str = ""
    warnings: str = ""
    supplement_facts: str = ""
    serving_size: str = ""
    servings_per_container: str = ""
    meta_description: str = ""
    review_tags: str = ""
    top_positive_review: str = ""
    top_critical_review: str = ""
    best_by_date: str = ""
    shipping_weight: str = ""
    upc_barcode: str = ""
    product_form: str = ""
    count: str = ""
    weight: str = ""
    dimensions: str = ""

class BulkDetailsIn(BaseModel):
    details: List[IHerbDetailIn]

@app.post("/api/iherb/products/bulk")
def bulk_save_iherb_products(data: BulkProductsIn, db: Session = Depends(get_db)):
    """Bulk save/update iHerb products from Chrome scraper."""
    saved = 0
    updated = 0
    for p in data.products:
        existing = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == p.iherb_id).first()
        if existing:
            for field in ['name', 'name_ko', 'brand', 'url', 'image_url', 'price_usd', 'price_krw',
                         'price_original', 'discount_pct', 'rating', 'review_count', 'category',
                         'sub_category', 'category_path', 'description', 'suggested_use',
                         'other_ingredients', 'warnings', 'supplement_facts', 'product_form',
                         'count', 'weight', 'dimensions', 'badges', 'in_stock']:
                val = getattr(p, field)
                if val and val != "" and val != 0 and val != "[]":
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
                supplement_facts=p.supplement_facts, product_form=p.product_form,
                count=p.count, weight=p.weight, dimensions=p.dimensions,
                badges=p.badges, in_stock=p.in_stock,
                scraped_at=datetime.utcnow(), updated_at=datetime.utcnow(),
            )
            db.add(new_product)
            saved += 1
    db.commit()
    return {"status": "ok", "saved": saved, "updated": updated, "total": saved + updated}

@app.post("/api/iherb/products/bulk-text")
async def bulk_save_text(request: Request, db: Session = Depends(get_db)):
    """Bulk save via text/plain body (for sendBeacon CORS workaround)."""
    body = await request.body()
    try:
        products = json.loads(body.decode("utf-8"))
    except Exception:
        return {"status": "error", "message": "Invalid JSON"}
    if not isinstance(products, list):
        products = [products]
    saved = 0
    updated = 0
    for p in products:
        iherb_id = str(p.get("iherb_id", ""))
        if not iherb_id:
            continue
        existing = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == iherb_id).first()
        if existing:
            for key, val in p.items():
                if key == "iherb_id":
                    continue
                if val and val != "" and val != 0 and hasattr(existing, key):
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val, ensure_ascii=False)
                    setattr(existing, key, val)
            existing.updated_at = datetime.utcnow()
            updated += 1
        else:
            new_product = IHerbProduct(
                iherb_id=iherb_id,
                product_id=p.get("product_id", f"iherb_{iherb_id}"),
                name=p.get("name", ""),
                name_ko=p.get("name_ko", ""),
                brand=p.get("brand", ""),
                url=p.get("url", p.get("product_url", "")),
                image_url=p.get("image_url", ""),
                price_usd=float(p.get("price_usd", 0)),
                price_krw=int(p.get("price_krw", 0)),
                price_original=float(p.get("price_original", 0)),
                discount_pct=float(p.get("discount_pct", 0)),
                rating=float(p.get("rating", 0)),
                review_count=int(p.get("review_count", 0)),
                category=p.get("category", ""),
                sub_category=p.get("sub_category", ""),
                in_stock=p.get("in_stock", True),
                scraped_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(new_product)
            saved += 1
    db.commit()
    return {"status": "ok", "saved": saved, "updated": updated, "total": saved + updated}

@app.post("/api/iherb/products/bulk-details")
def bulk_update_details(data: BulkDetailsIn, db: Session = Depends(get_db)):
    """Bulk update product detail fields (description, ingredients, supplement facts, etc.)."""
    updated = 0
    not_found = 0
    for d in data.details:
        product = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == d.iherb_id).first()
        if not product:
            not_found += 1
            continue
        detail_fields = [
            'description', 'suggested_use', 'ingredients', 'other_ingredients',
            'allergen_info', 'warnings', 'supplement_facts', 'serving_size',
            'servings_per_container', 'meta_description', 'top_positive_review',
            'top_critical_review', 'best_by_date', 'shipping_weight', 'upc_barcode',
            'product_form', 'count', 'weight', 'dimensions'
        ]
        for field in detail_fields:
            val = getattr(d, field, None)
            if val and val != "":
                setattr(product, field, val)
        if d.review_tags:
            product.tags = d.review_tags
        product.updated_at = datetime.utcnow()
        updated += 1
    db.commit()
    return {"status": "ok", "updated": updated, "not_found": not_found}

@app.put("/api/iherb/products/{iherb_id}/details")
def update_product_details(iherb_id: str, data: IHerbDetailIn, db: Session = Depends(get_db)):
    """Update individual product details."""
    product = db.query(IHerbProduct).filter(IHerbProduct.iherb_id == iherb_id).first()
    if not product:
        raise HTTPException(404, "Product not found")
    detail_fields = [
        'description', 'suggested_use', 'ingredients', 'other_ingredients',
        'allergen_info', 'warnings', 'supplement_facts', 'serving_size',
        'servings_per_container', 'meta_description', 'top_positive_review',
        'top_critical_review', 'best_by_date', 'shipping_weight', 'upc_barcode',
        'product_form', 'count', 'weight', 'dimensions'
    ]
    for field in detail_fields:
        val = getattr(data, field, None)
        if val and val != "":
            setattr(product, field, val)
    if data.review_tags:
        product.tags = data.review_tags
    product.updated_at = datetime.utcnow()
    db.commit()
    return {"status": "ok", "iherb_id": iherb_id}

@app.get("/api/iherb/products")
def get_iherb_products(
    page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100),
    brand: Optional[str] = None, category: Optional[str] = None,
    search: Optional[str] = None, in_stock: Optional[bool] = None,
    min_rating: Optional[float] = None,
    sort: str = "review_count", order: str = "desc",
    db: Session = Depends(get_db),
):
    """iHerb product list with filtering, pagination, and detail fields."""
    query = db.query(IHerbProduct)
    if brand: query = query.filter(IHerbProduct.brand.ilike(f"%{brand}%"))
    if category:
        query = query.filter(
            (IHerbProduct.category.ilike(f"%{category}%")) | (IHerbProduct.category_path.ilike(f"%{category}%"))
        )
    if search:
        query = query.filter(
            (IHerbProduct.name.ilike(f"%{search}%")) | (IHerbProduct.brand.ilike(f"%{search}%")) | (IHerbProduct.iherb_id.ilike(f"%{search}%"))
        )
    if in_stock is not None: query = query.filter(IHerbProduct.in_stock == in_stock)
    if min_rating: query = query.filter(IHerbProduct.rating >= min_rating)

    sort_col = getattr(IHerbProduct, sort, IHerbProduct.review_count)
    if order == "desc": query = query.order_by(desc(sort_col))
    else: query = query.order_by(sort_col)

    total = query.count()
    products = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "total": total, "page": page, "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "products": [
            {
                "iherb_id": p.iherb_id,
                "product_id": p.product_id,
                "name": p.name,
                "brand": p.brand,
                "price_usd": p.price_usd,
                "price_original": p.price_original,
                "discount_pct": p.discount_pct,
                "price_krw": p.price_krw,
                "rating": p.rating,
                "review_count": p.review_count,
                "image_url": p.image_url,
                "category": p.category,
                "sub_category": p.sub_category,
                "in_stock": p.in_stock,
                "product_form": p.product_form,
                "count": p.count,
                "badges": json.loads(p.badges) if isinstance(p.badges, str) and p.badges.startswith("[") else (p.badges or []),
                "url": p.url,
                "description": (p.description or "")[:200],
                "suggested_use": (p.suggested_use or "")[:200],
                "warnings": (p.warnings or "")[:200],
                "other_ingredients": (p.other_ingredients or "")[:200],
                "supplement_facts": p.supplement_facts or "",
                "serving_size": p.serving_size or "",
                "meta_description": (p.meta_description or "")[:200],
                "best_by_date": p.best_by_date or "",
                "upc_barcode": p.upc_barcode or "",
                "shipping_weight": p.shipping_weight or "",
                "has_details": bool(p.description or p.supplement_facts or p.suggested_use),
            }
            for p in products
        ],
    }

@app.get("/api/iherb/products/{product_id}")
def get_iherb_product_detail(product_id: str, db: Session = Depends(get_db)):
    """Full iHerb product detail with ALL collected information."""
    product = db.query(IHerbProduct).filter(
        (IHerbProduct.product_id == product_id) | (IHerbProduct.iherb_id == product_id)
    ).first()
    if not product: raise HTTPException(404, "iHerb product not found")
    return {
        "basic": {"iherb_id": product.iherb_id, "product_id": product.product_id,
                  "name": product.name, "name_ko": product.name_ko, "subtitle": product.subtitle,
                  "brand": product.brand, "brand_ko": getattr(product, 'brand_ko', None),
                  "brand_url": product.brand_url, "url": product.url, "image_url": product.image_url},
        "pricing": {"price_usd": product.price_usd, "price_original": product.price_original,
                    "discount_pct": product.discount_pct, "price_per_unit": product.price_per_unit,
                    "price_krw": product.price_krw, "in_stock": product.in_stock, "stock_status": product.stock_status},
        "images": {"main": product.image_url, "thumbnail": product.thumbnail_url, "all": product.image_urls or []},
        "rating": {"rating": product.rating, "count": product.review_count,
                   "distribution": product.rating_distribution,
                   "top_positive": product.top_positive_review, "top_critical": product.top_critical_review},
        "description": {"description": product.description, "description_ko": getattr(product, 'description_ko', None),
                       "features": product.features, "features_ko": getattr(product, 'features_ko', None),
                       "suggested_use": product.suggested_use, "suggested_use_ko": getattr(product, 'suggested_use_ko', None),
                       "warnings": product.warnings, "warnings_ko": getattr(product, 'warnings_ko', None),
                       "storage": product.storage_info, "storage_ko": getattr(product, 'storage_info_ko', None)},
        "nutrition": {"supplement_facts": product.supplement_facts, "ingredients": product.ingredients,
                     "ingredients_ko": getattr(product, 'ingredients_ko', None),
                     "ingredients_list": product.ingredients_list, "other_ingredients": product.other_ingredients,
                     "other_ingredients_ko": getattr(product, 'other_ingredients_ko', None),
                     "allergen_info": product.allergen_info, "allergen_info_ko": getattr(product, 'allergen_info_ko', None),
                     "serving_size": product.serving_size,
                     "servings_per_container": product.servings_per_container},
        "specs": {"product_form": product.product_form, "count": product.count, "weight": product.weight,
                 "dimensions": product.dimensions, "upc_barcode": product.upc_barcode, "sku": product.sku,
                 "shipping_weight": product.shipping_weight},
        "certifications": {"badges": json.loads(product.badges) if isinstance(product.badges, str) and product.badges.startswith("[") else (product.badges or []),
                          "certifications": product.certifications, "best_by_date": product.best_by_date},
        "category": {"category": product.category, "sub_category": product.sub_category, "path": product.category_path,
                    "category_ko": getattr(product, 'category_ko', None), "sub_category_ko": getattr(product, 'sub_category_ko', None),
                    "path_ko": getattr(product, 'category_path_ko', None)},
        "social": {"qa_count": product.qa_count, "top_questions": product.top_questions, "reviews": product.reviews_data},
        "related": {"related_products": product.related_products, "also_bought": product.also_bought, "bundle_deals": product.bundle_deals},
        "meta": {"tags": product.tags, "popularity_rank": product.popularity_rank,
                "scraped_at": product.scraped_at.isoformat() if product.scraped_at else None,
                "ko_scraped_at": getattr(product, 'ko_scraped_at', None) and product.ko_scraped_at.isoformat() if getattr(product, 'ko_scraped_at', None) else None,
                "updated_at": product.updated_at.isoformat() if product.updated_at else None},
    }

@app.get("/api/iherb/stats")
def get_iherb_stats(db: Session = Depends(get_db)):
    total = db.query(IHerbProduct).count()
    in_stock = db.query(IHerbProduct).filter(IHerbProduct.in_stock == True).count()
    has_details = db.query(IHerbProduct).filter(
        (IHerbProduct.description != None) & (IHerbProduct.description != "")
    ).count()
    by_category = db.query(
        IHerbProduct.category, func.count(IHerbProduct.id).label("count"),
        func.avg(IHerbProduct.price_usd).label("avg_price"), func.avg(IHerbProduct.rating).label("avg_rating"),
    ).group_by(IHerbProduct.category).order_by(desc("count")).limit(20).all()
    by_brand = db.query(
        IHerbProduct.brand, func.count(IHerbProduct.id).label("count"),
        func.avg(IHerbProduct.price_usd).label("avg_price"), func.avg(IHerbProduct.rating).label("avg_rating"),
    ).group_by(IHerbProduct.brand).order_by(desc("count")).limit(20).all()
    top_rated = db.query(IHerbProduct).filter(
        IHerbProduct.review_count >= 100, IHerbProduct.rating > 0,
    ).order_by(desc(IHerbProduct.rating)).limit(10).all()
    return {
        "total": total, "total_products": total, "in_stock": in_stock, "has_details": has_details,
        "by_category": [
            {"category": c.category or "기타", "count": c.count, "avg_price": round(c.avg_price or 0, 2), "avg_rating": round(c.avg_rating or 0, 1)}
            for c in by_category
        ],
        "by_brand": [
            {"brand": b.brand or "Unknown", "count": b.count, "avg_price": round(b.avg_price or 0, 2), "avg_rating": round(b.avg_rating or 0, 1)}
            for b in by_brand
        ],
        "top_rated": [
            {"name": p.name, "brand": p.brand, "rating": p.rating, "review_count": p.review_count, "price_usd": p.price_usd, "image_url": p.image_url}
            for p in top_rated
        ],
    }

# ── Analytics API ─────────────────────────────────────────
@app.get("/api/analytics/brand-comparison")
def brand_comparison(db: Session = Depends(get_db)):
    results = db.query(
        Product.brand, func.count(IHerbMapping.id).label("mapped_count"),
        func.avg(Product.price_usd).label("avg_ople_price"),
        func.avg(IHerbMapping.iherb_price_usd).label("avg_iherb_price"),
        func.avg(IHerbMapping.price_diff_pct).label("avg_diff_pct"),
    ).join(IHerbMapping, Product.it_id == IHerbMapping.ople_id).group_by(Product.brand).having(
        func.count(IHerbMapping.id) >= 3
    ).order_by(desc("mapped_count")).limit(15).all()
    return [
        {"brand": r.brand or "Unknown", "mapped_count": r.mapped_count,
         "avg_ople_price": round(r.avg_ople_price or 0, 2), "avg_iherb_price": round(r.avg_iherb_price or 0, 2),
         "avg_diff_pct": round(r.avg_diff_pct or 0, 1)}
        for r in results
    ]

@app.get("/api/analytics/review-keywords")
def review_keywords(db: Session = Depends(get_db)):
    reviews = db.query(Review.keywords).filter(Review.keywords.isnot(None)).limit(1000).all()
    keyword_counts = {}
    for r in reviews:
        if r.keywords:
            for kw in r.keywords:
                keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
    sorted_kw = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    return [{"keyword": k, "count": v} for k, v in sorted_kw]

# ── Auth Endpoints ───────────────────────────────────────

@app.post("/api/auth/google")
async def google_auth(data: dict, db: Session = Depends(get_db)):
    """Verify Google ID token and create/update user."""
    token = data.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="No token provided")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://oauth2.googleapis.com/tokeninfo?id_token={token}",
                timeout=10.0
            )

        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid Google token")

        token_info = response.json()
        email = token_info.get("email")
        name = token_info.get("name")
        picture = token_info.get("picture")
        google_uid = token_info.get("sub")

        if not email or not google_uid:
            raise HTTPException(status_code=400, detail="Missing email or google_uid")

        # Get or create user
        user = db.query(User).filter(User.google_uid == google_uid).first()

        if not user:
            # Determine role: first user or emails in ADMIN_EMAILS get admin role
            is_first_user = db.query(User).count() == 0
            is_admin_email = email.lower() in [e.lower().strip() for e in ADMIN_EMAILS]
            role = "admin" if (is_first_user or is_admin_email) else "viewer"

            user = User(
                email=email,
                name=name,
                picture=picture,
                google_uid=google_uid,
                role=role,
                is_active=True,
            )
            db.add(user)
        else:
            # Update existing user
            user.name = name
            user.picture = picture

        user.last_login = datetime.utcnow()
        db.commit()
        db.refresh(user)

        # Create JWT token
        token_data = {
            "sub": str(user.id),
            "email": user.email,
            "name": user.name,
            "role": user.role,
        }
        expires = datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS)
        token_data["exp"] = expires
        jwt_token = jwt.encode(token_data, JWT_SECRET, algorithm=JWT_ALGORITHM)

        return {
            "token": jwt_token,
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "picture": user.picture,
                "role": user.role,
                "is_active": user.is_active,
                "last_login": user.last_login.isoformat() if user.last_login else None,
            },
        }

    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Failed to verify token: {str(e)}")


@app.post("/api/auth/email-login")
async def email_login(data: dict, db: Session = Depends(get_db)):
    """Admin email + password login."""
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="No email provided")
    if not password:
        raise HTTPException(status_code=400, detail="비밀번호를 입력해주세요")

    allowed = [e.lower().strip() for e in ADMIN_EMAILS]
    if email not in allowed:
        raise HTTPException(status_code=403, detail="이 이메일은 관리자 로그인이 허용되지 않습니다")

    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="비밀번호가 올바르지 않습니다")

    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(
            email=email,
            name=email.split("@")[0],
            google_uid=f"email_{email}",
            role="admin",
            is_active=True,
        )
        db.add(user)

    user.last_login = datetime.utcnow()
    db.commit()
    db.refresh(user)

    token_data = {
        "sub": str(user.id),
        "email": user.email,
        "name": user.name,
        "role": user.role,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS),
    }
    jwt_token = jwt.encode(token_data, JWT_SECRET, algorithm=JWT_ALGORITHM)

    return {
        "token": jwt_token,
        "user": {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "picture": user.picture,
            "role": user.role,
            "is_active": user.is_active,
        },
    }


@app.get("/api/auth/me")
def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current authenticated user info."""
    return {
        "id": current_user.id,
        "email": current_user.email,
        "name": current_user.name,
        "picture": current_user.picture,
        "role": current_user.role,
        "is_active": current_user.is_active,
        "last_login": current_user.last_login.isoformat() if current_user.last_login else None,
    }


@app.get("/api/users")
def list_users(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """List all users (admin only)."""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    users = db.query(User).all()
    return [
        {
            "id": u.id,
            "email": u.email,
            "name": u.name,
            "picture": u.picture,
            "role": u.role,
            "is_active": u.is_active,
            "last_login": u.last_login.isoformat() if u.last_login else None,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }
        for u in users
    ]


@app.put("/api/users/{user_id}/role")
def update_user_role(user_id: int, data: dict, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Update user role (admin only)."""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    role = data.get("role")
    if role not in ["admin", "editor", "viewer"]:
        raise HTTPException(status_code=400, detail="Invalid role")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = role
    db.commit()
    db.refresh(user)

    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "role": user.role,
    }


@app.put("/api/users/{user_id}/active")
def toggle_user_active(user_id: int, data: dict, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Toggle user active status (admin only)."""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    is_active = data.get("is_active")
    if is_active is None:
        raise HTTPException(status_code=400, detail="Missing is_active field")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.is_active = is_active
    db.commit()
    db.refresh(user)

    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "is_active": user.is_active,
    }

# ── Demo Data Seeding ─────────────────────────────────────
def seed_demo_data(db: Session):
    demo_products = [
        {"it_id": "1319032894", "name_ko": "Now Foods 울트라 오메가-3, 180캡슐", "name_en": "Now Foods Ultra Omega-3 180 Softgels", "brand": "Now Foods", "price_usd": 25.99, "price_krw": 39115, "review_count": 1473, "parent_category": "건강식품", "category_name": "오메가3/피쉬오일"},
        {"it_id": "1505216341", "name_ko": "Solgar 콜라겐 히알루론산, 30정", "name_en": "Solgar Collagen Hyaluronic Acid Complex", "brand": "Solgar", "price_usd": 14.99, "price_krw": 22560, "review_count": 607, "parent_category": "뷰티용품", "category_name": "콜라겐"},
        {"it_id": "1407165807", "name_ko": "Doctor's Best 루테인+루트맥스 20mg, 180캡슐", "name_en": "Doctor's Best Lutein with FloraGlo 20mg", "brand": "Doctor's Best", "price_usd": 26.99, "price_krw": 40620, "review_count": 954, "parent_category": "건강식품", "category_name": "눈건강"},
        {"it_id": "1511431863", "name_ko": "Jarrow Formulas 비건 MSM 1000mg, 200캡슐", "name_en": "Jarrow Formulas Vegan MSM 1000mg", "brand": "Jarrow Formulas", "price_usd": 25.99, "price_krw": 39115, "review_count": 1106, "parent_category": "건강식품", "category_name": "관절건강"},
        {"it_id": "1417406111", "name_ko": "Solgar 글루코사민+코드로이친+MSM, 120정", "name_en": "Solgar Glucosamine Chondroitin MSM", "brand": "Solgar", "price_usd": 28.99, "price_krw": 43630, "review_count": 156, "parent_category": "건강식품", "category_name": "관절건강"},
        {"it_id": "1511224023", "name_ko": "Solgar 마그네슘+비타민B6, 250정", "name_en": "Solgar Magnesium with Vitamin B6", "brand": "Solgar", "price_usd": 11.99, "price_krw": 18045, "review_count": 204, "parent_category": "건강식품", "category_name": "미네랄"},
        {"it_id": "1510428215", "name_ko": "Solgar 에스터-C 비타민C 1000mg, 180정", "name_en": "Solgar Ester-C Plus Vitamin C 1000mg", "brand": "Solgar", "price_usd": 21.59, "price_krw": 32493, "review_count": 445, "parent_category": "건강식품", "category_name": "비타민C"},
        {"it_id": "1511818877", "name_ko": "Doctor's Best 비타민 D3 2000IU, 180캡슐", "name_en": "Doctor's Best Vitamin D3 2000IU", "brand": "Doctor's Best", "price_usd": 7.99, "price_krw": 12025, "review_count": 110, "parent_category": "건강식품", "category_name": "비타민D"},
        {"it_id": "1505100130", "name_ko": "Now Foods L-아르기닜 1000mg, 120정", "name_en": "Now Foods L-Arginine 1000mg", "brand": "Now Foods", "price_usd": 13.99, "price_krw": 21055, "review_count": 440, "parent_category": "건강식품", "category_name": "아미노산"},
        {"it_id": "1417406120", "name_ko": "Solgar 비오틴 10000mcg, 60캡슐", "name_en": "Solgar Biotin 10000mcg", "brand": "Solgar", "price_usd": 13.99, "price_krw": 21055, "review_count": 70, "parent_category": "뷰티용품", "category_name": "비오틴"},
        {"it_id": "1672906957", "name_ko": "Jarrow Formulas 비건 펨 도피러스 유산균, 30캡슐", "name_en": "Jarrow Formulas Fem Dophilus", "brand": "Jarrow Formulas", "price_usd": 22.99, "price_krw": 34600, "review_count": 145, "parent_category": "건강식품", "category_name": "유산균"},
        {"it_id": "1513534579", "name_ko": "Double Wood 포스파티딜세린 300mg, 120캡슐", "name_en": "Double Wood Phosphatidylserine 300mg", "brand": "Double Wood", "price_usd": 14.29, "price_krw": 21507, "review_count": 230, "parent_category": "건강식품", "category_name": "두뇌건강"},
        {"it_id": "1672905857", "name_ko": "Swanson 유산균 가세리 30억, 60캡슐", "name_en": "Swanson L. Gasseri 3 Billion", "brand": "Swanson", "price_usd": 12.99, "price_krw": 19550, "review_count": 187, "parent_category": "여스/다이어트", "category_name": "다이어트"},
        {"it_id": "1511560477", "name_ko": "Absonutrix 시서스 1600mg, 120캡슐", "name_en": "Absonutrix Cissus 1600mg", "brand": "Absonutrix", "price_usd": 19.99, "price_krw": 30085, "review_count": 268, "parent_category": "헬스/다이어트", "category_name": "다이어트"},
        {"it_id": "1510480064", "name_ko": "Jarrow Formulas 비건 MSM 파우더, 1kg", "name_en": "Jarrow Formulas MSM Powder 1kg", "brand": "Jarrow Formulas", "price_usd": 38.99, "price_krw": 58679, "review_count": 604, "parent_category": "건강식품", "category_name": "관절건강"},
        {"it_id": "1268694786", "name_ko": "Solgar 루테인 20mg, 60캡슐", "name_en": "Solgar Lutein 20mg", "brand": "Solgar", "price_usd": 15.59, "price_krw": 23463, "review_count": 79, "parent_category": "건강식품", "category_name": "눈건강"},
        {"it_id": "1505230641", "name_ko": "Solgar 칼슘+마그네슘/아연, 250정", "name_en": "Solgar Calcium Magnesium Zinc", "brand": "Solgar", "price_usd": 13.99, "price_krw": 21055, "review_count": 300, "parent_category": "건강식품", "category_name": "미네랄"},
        {"it_id": "1334058959", "name_ko": "Nature's Way 얼라이브 맨즈 50+ 멀티비타민, 60정", "name_en": "Nature's Way Alive Men's 50+ Multi", "brand": "Nature's Way", "price_usd": 17.99, "price_krw": 27075, "review_count": 480, "parent_category": "건강식품", "category_name": "종합비타민"},
    ]
    for p in demo_products:
        product = Product(
            it_id=p["it_id"], name_ko=p["name_ko"], name_en=p["name_en"], brand=p["brand"],
            price_usd=p["price_usd"], price_krw=p["price_krw"], review_count=p["review_count"],
            parent_category=p["parent_category"], category_name=p["category_name"],
            url=f"https://www.ople.com/mall5/shop/item.php?it_id={p['it_id']}",
            image_url=f"https://img.ople.com/data/item/{p['it_id'][:4]}/{p['it_id']}.jpg",
        )
        db.add(product)
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
        mapping = IHerbMapping(
            ople_id=m["ople_id"], iherb_id=m["iherb_id"], iherb_name=m["iherb_name"],
            iherb_brand=m["iherb_brand"], iherb_price_usd=m["iherb_price_usd"],
            match_method=m["match_method"], match_score=m["match_score"],
            price_diff=m["price_diff"], price_diff_pct=m["price_diff_pct"],
            verified=m["match_method"] == "upc",
        )
        db.add(mapping)
    demo_reviews = [
        {"product_id": "1319032894", "reviewer": "건강맘***", "rating": 5, "text": "오메가3 먹고 나서 피로가 화 줄었어요.", "date": "2024-12-15", "keywords": ["피로", "눈건강", "오메가3"]},
        {"product_id": "1319032894", "reviewer": "운동매***", "rating": 5, "text": "운동 후 회복이 빨라진 것 같아요.", "date": "2024-11-20", "keywords": ["운동", "회복", "관절"]},
        {"product_id": "1505216341", "reviewer": "뷰티러***", "rating": 4, "text": "콜라겐 한달 먹었는데 피부 탄력이 좋아진 느낌이에요", "date": "2024-12-01", "keywords": ["콜라겐", "피부", "탄력"]},
        {"product_id": "1407165807", "reviewer": "직장인***", "rating": 5, "text": "루테인 먹고 나서 눈 피로가 확실히 줄었습니다.", "date": "2025-01-10", "keywords": ["루테인", "눈피로", "직장인"]},
        {"product_id": "1511431863", "reviewer": "등산러***", "rating": 5, "text": "MSM 관절에 정말 좋아요. 무릎 통증이 많이 줄었습니다", "date": "2024-10-25", "keywords": ["MSM", "관절", "무릎", "통증"]},
        {"product_id": "1510428215", "reviewer": "면역전***", "rating": 5, "text": "비타민C 에스터C가 위에 부담 없어서 좋아요.", "date": "2024-11-15", "keywords": ["비타민C", "면역", "감기", "위장"]},
    ]
    for r in demo_reviews:
        review = Review(product_id=r["product_id"], reviewer=r["reviewer"], rating=r["rating"],
                       text=r["text"], date=r["date"], keywords=r["keywords"])
        db.add(review)

    # ── iHerb 시드 데이터 (20개 베스트셀러, 상세정보 포함) ──
    demo_iherb_products = [
        {"iherb_id": "DRB-00087", "product_id": "16567", "name": "Doctor's Best High Absorption Magnesium Glycinate Lysinate 240 Tablets", "name_ko": "Doctor's Best, 고흡수 마그네슘 리시네이트 글리시네이트, 킬레이트화, 240정", "brand": "Doctor's Best", "price_usd": 18.89, "price_krw": 27343, "rating": 4.7, "review_count": 193755, "category": "Supplements", "sub_category": "Minerals", "category_ko": "보충제", "sub_category_ko": "미네랄", "product_form": "Tablet", "count": "240 Tablets", "in_stock": True, "url": "https://www.iherb.com/pr/16567", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/drb/drb00087/s/214.jpg", "description": "Science-Based Nutrition™ 리시네이트 글리시네이트 100% 킬레이트화. 근육 이완 및 수면에 도움. Doctor's Best의 고흡수 마그네슘은 다양한 신체 기능에 핵심적인 역할을 하는 만능 보충제입니다. 당사의 스페셜 마그네슘 포뮬라는 근육, 신경, 수면의 질, 정서적 균형 유지에 필수적인 효능을 제공합니다.", "suggested_use": "성인 복용법: 하루에 2정씩 2회 복용하거나 영양학적 지식을 갖춘 의사의 조언에 따라 복용하십시오.", "other_ingredients": "미결정셀룰로오스, 크로스카멜로스나트륨, 마그네슘스테아레이트(식물성원료), 스테아르산, 하이드록시프로필셀룰로오스, 이산화규소, 하이프로멜로오스.", "warnings": "서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Gluten-Free", "Vegan", "Soy Free"]},
        {"iherb_id": "MLI-00952", "product_id": "62118", "name": "California Gold Nutrition Omega-3 Premium Fish Oil 100 Softgels", "name_ko": "California Gold Nutrition, 오메가3 프리미엄 피쉬 오일, 소프트젤 100정", "brand": "California Gold Nutrition", "price_usd": 12.92, "price_krw": 18710, "rating": 4.7, "review_count": 478643, "category": "Supplements", "sub_category": "Fish Oil & Omegas", "category_ko": "보충제", "sub_category_ko": "오메가3", "product_form": "Softgel", "count": "100 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/62118", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/mli/mli00952/s/60.jpg", "description": "California Gold Nutrition® 오메가3 프리미엄 피쉬 오일. 전 세계에서 공급된 고도로 정제된 피쉬 오일 함유. 농축 및 분자 증류됨. 전반적인 면역계 건강 증진. 지질 성분을 최적으로 유지하는 데 도움.", "suggested_use": "매일 1~2회, 식사와 함께 소프트젤 1정씩 복용하십시오.", "other_ingredients": "소프트젤 캡슐(젤라틴, 글리세린, 정제수), 천연 레몬 향.", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Gluten-Free", "Non-GMO"]},
        {"iherb_id": "NOW-01652", "product_id": "88819", "name": "NOW Foods Magnesium Glycinate 180 Tablets", "name_ko": "NOW Foods, 마그네슘 글리시네이트, 180정", "brand": "NOW Foods", "price_usd": 16.19, "price_krw": 23449, "rating": 4.7, "review_count": 32606, "category": "Supplements", "sub_category": "Minerals", "category_ko": "보충제", "sub_category_ko": "미네랄", "product_form": "Tablet", "count": "180 Tablets", "in_stock": True, "url": "https://www.iherb.com/pr/88819", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/now/now01289/s/41.jpg", "description": "NOW Foods 마그네슘 글리시네이트. 신경 및 근육 지원. 우수한 생체이용률. 마그네슘 글리시네이트는 킬레이트화된 형태로 최적의 흡수율을 제공합니다.", "suggested_use": "매일 2정씩 1~2회 식사와 함께 복용하십시오.", "other_ingredients": "하이프로멜로오스(셀룰로오스 캡슐), 스테아르산(식물성원료), 이산화규소.", "warnings": "서늘하고 건조한 곳에 보관하십시오. 개봉 후에는 냉장 보관하십시오.", "badges": ["Non-GMO", "Vegan", "GMP Certified"]},
        {"iherb_id": "NOW-00369", "product_id": "10056", "name": "NOW Foods Vitamin D3 & K2 120 Capsules", "name_ko": "NOW Foods, 비타민D3 & K2, 캡슐 120정", "brand": "NOW Foods", "price_usd": 8.99, "price_krw": 13021, "rating": 4.7, "review_count": 72958, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민", "product_form": "Capsule", "count": "120 Capsules", "in_stock": True, "url": "https://www.iherb.com/pr/10056", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/now/now00369/s/83.jpg", "description": "비타민D3 1,000IU / 비타민K2 45mcg. 뼈 건강 증진. 심혈관계 건강 지원. NOW는 뼈, 치아, 심혈관계 지원 효능이 검증된 두 가지 영양소를 함께 담았습니다. 비타민D3는 칼슘 운반과 흡수를 촉진합니다.", "suggested_use": "매일 1~2회, 캡슐 1정씩 식사 후에 복용하십시오.", "other_ingredients": "쌀가루, 하이프로멜로오스(셀룰로오스캡슐), 아스코빌팔미테이트, 이산화규소.", "warnings": "성인 전용 제품입니다. 임신 또는 수유 중이거나, 항응혈제를 복용 중이거나 질환이 있는 경우, 의사와 상의하십시오.", "badges": ["Non-GMO", "GMP Certified"]},
        {"iherb_id": "DRB-00015", "product_id": "15", "name": "Doctor's Best High Absorption Magnesium 120 Tablets", "name_ko": "Doctor's Best, 고흡수 마그네슘, 120정", "brand": "Doctor's Best", "price_usd": 10.78, "price_krw": 15609, "rating": 4.7, "review_count": 193755, "category": "Supplements", "sub_category": "Minerals", "category_ko": "보충제", "sub_category_ko": "미네랄", "product_form": "Tablet", "count": "120 Tablets", "in_stock": True, "url": "https://www.iherb.com/pr/15", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/drb/drb00015/s/227.jpg", "description": "Science-Based Nutrition™ 리시네이트 글리시네이트 100% 킬레이트화. 근육 이완 및 수면에 도움. Doctor's Best의 고흡수 마그네슘은 신체 기능에 핵심적인 역할을 하는 보충제입니다.", "suggested_use": "성인 복용법: 하루에 2정씩 2회 복용하거나 의사의 조언에 따라 복용하십시오.", "other_ingredients": "미결정셀룰로오스, 크로스카멜로스나트륨, 마그네슘스테아레이트(식물성원료), 스테아르산.", "warnings": "서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Gluten-Free", "Vegan", "Soy Free"]},
        {"iherb_id": "NOW-01283", "product_id": "85898", "name": "NOW Foods Magnesium Citrate 200mg 250 Tablets", "name_ko": "NOW Foods, 마그네슘 시트레이트, 200mg, 250정", "brand": "NOW Foods", "price_usd": 13.49, "price_krw": 19530, "rating": 4.7, "review_count": 72474, "category": "Supplements", "sub_category": "Minerals", "category_ko": "보충제", "sub_category_ko": "미네랄", "product_form": "Tablet", "count": "250 Tablets", "in_stock": True, "url": "https://www.iherb.com/pr/85898", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/now/now01283/s/70.jpg", "description": "NOW Foods 마그네슘 시트레이트 200mg. 신경계 지원. 에너지 생성 및 대사에 필수적. 마그네슘은 300가지 이상의 생화학 반응에 관여하는 필수 미네랄입니다.", "suggested_use": "매일 2정씩 1~2회 식사와 함께 복용하십시오.", "other_ingredients": "셀룰로오스, 스테아르산(식물성원료), 이산화규소, 크로스카멜로스나트륨.", "warnings": "서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Vegan", "GMP Certified"]},
        {"iherb_id": "NVT-22498", "product_id": "22498", "name": "Natural Vitality CALM Magnesium Supplement Raspberry Lemon 453g", "name_ko": "Natural Vitality, CALM, 마그네슘 보충제 음료 분말, 라즈베리레몬 맛, 453g", "brand": "Natural Vitality", "price_usd": 27.49, "price_krw": 39810, "rating": 4.6, "review_count": 63178, "category": "Supplements", "sub_category": "Minerals", "category_ko": "보충제", "sub_category_ko": "미네랄", "product_form": "Powder", "count": "453g", "in_stock": True, "url": "https://www.iherb.com/pr/22498", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/nvt/nvt22498/s/4.jpg", "description": "Natural Vitality CALM 마그네슘 보충제 음료 분말. 라즈베리 레몬 맛. 스트레스 해소 및 이완에 도움. 건강한 마그네슘 수치 유지를 지원합니다.", "suggested_use": "뜨거운 물 2~3oz에 2티스푼을 녹여 드십시오. 일단 녹이면 냉수나 얼음을 추가해도 됩니다.", "other_ingredients": "유기농 라즈베리 향, 유기농 레몬 향, 유기농 스테비아.", "warnings": "서늘하고 건조한 곳에 보관하십시오. 임신 또는 수유 중인 경우 의사와 상의하십시오.", "badges": ["Non-GMO", "Vegan", "Organic"]},
        {"iherb_id": "NOW-01653", "product_id": "84928", "name": "NOW Foods Ultra Omega-3 180 Softgels", "name_ko": "NOW Foods, 울트라 오메가3, 소프트젤 180정", "brand": "NOW Foods", "price_usd": 22.99, "price_krw": 33289, "rating": 4.7, "review_count": 37258, "category": "Supplements", "sub_category": "Fish Oil & Omegas", "category_ko": "보충제", "sub_category_ko": "오메가3", "product_form": "Softgel", "count": "180 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/84928", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/now/now01653/s/67.jpg", "description": "NOW Foods 울트라 오메가-3. 분자 증류된 고품질 피쉬 오일. EPA 500mg / DHA 250mg. 심혈관 건강 및 두뇌 기능 지원.", "suggested_use": "매일 2회, 식사와 함께 소프트젤 1정씩 복용하십시오.", "other_ingredients": "소프트젤 캡슐(소 젤라틴, 글리세린, 정제수), 천연 레몬향.", "warnings": "서늘하고 건조한 곳에 보관하십시오. 항응혈제를 복용 중인 경우 의사와 상의하십시오.", "badges": ["Non-GMO", "GMP Certified"]},
        {"iherb_id": "SPN-02688", "product_id": "96587", "name": "Sports Research Vitamin K2 + D3 60 Veggie Softgels", "name_ko": "Sports Research, 비타민K2 + D3, 식물성 소프트젤 60정", "brand": "Sports Research", "price_usd": 14.95, "price_krw": 21643, "rating": 4.7, "review_count": 24361, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민", "product_form": "Softgel", "count": "60 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/96587", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/spn/spn02688/s/26.jpg", "description": "Sports Research 비타민K2 + D3. 식물성 소프트젤. 코코넛 오일 함유로 흡수율 증진. 뼈와 면역 건강을 동시에 지원합니다.", "suggested_use": "매일 식사와 함께 소프트젤 1정을 복용하십시오.", "other_ingredients": "코코넛 오일, 식물성 소프트젤(타피오카 전분, 글리세린, 정제수).", "warnings": "임신 또는 수유 중인 경우 의사와 상의하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Vegan", "Soy Free"]},
        {"iherb_id": "CGN-01101", "product_id": "59852", "name": "California Gold Nutrition Gold C Vitamin C 1000mg 240 Veggie Capsules", "name_ko": "California Gold Nutrition, 비타민C, Quali-C, 1000mg, 베지 캡슐 240정", "brand": "California Gold Nutrition", "price_usd": 19.9, "price_krw": 28809, "rating": 4.7, "review_count": 377236, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민C", "product_form": "Capsule", "count": "240 Capsules", "in_stock": True, "url": "https://www.iherb.com/pr/59852", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn00931/s/383.jpg", "description": "California Gold Nutrition Gold C 비타민C 1000mg. Quali-C 유럽산 비타민C 사용. 면역 건강 지원. 항산화 보호.", "suggested_use": "매일 1회 캡슐 1정을 식사 여부와 관계없이 복용하십시오.", "other_ingredients": "하이드록시프로필메틸셀룰로오스(베지 캡슐), 마그네슘스테아레이트.", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Soy Free", "GMP Certified"]},
        {"iherb_id": "CGN-01065", "product_id": "61864", "name": "California Gold Nutrition Gold C Vitamin C 1000mg 60 Veggie Capsules", "name_ko": "California Gold Nutrition, Gold C, 비타민C, 1000mg, 베지 캡슐 60정", "brand": "California Gold Nutrition", "price_usd": 5.9, "price_krw": 8542, "rating": 4.7, "review_count": 377236, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민C", "product_form": "Capsule", "count": "60 Capsules", "in_stock": True, "url": "https://www.iherb.com/pr/61864", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01065/s/204.jpg", "description": "California Gold Nutrition Gold C 비타민C 1000mg 60정. Quali-C 유럽산 비타민C 사용. 면역 건강 지원. 항산화 보호.", "suggested_use": "매일 1회 캡슐 1정을 식사 여부와 관계없이 복용하십시오.", "other_ingredients": "하이드록시프로필메틸셀룰로오스(베지 캡슐), 마그네슘스테아레이트.", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Soy Free", "GMP Certified"]},
        {"iherb_id": "CGN-01252", "product_id": "86598", "name": "California Gold Nutrition Omega-3 Premium Fish Oil 240 Softgels", "name_ko": "California Gold Nutrition, 오메가3, 프리미엄 피쉬 오일, 소프트젤 240정", "brand": "California Gold Nutrition", "price_usd": 28.1, "price_krw": 40688, "rating": 4.7, "review_count": 478643, "category": "Supplements", "sub_category": "Fish Oil & Omegas", "category_ko": "보충제", "sub_category_ko": "오메가3", "product_form": "Softgel", "count": "240 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/86598", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01252/s/54.jpg", "description": "California Gold Nutrition® 오메가3 프리미엄 피쉬 오일 240정. 고도로 정제된 피쉬 오일 함유. 농축 및 분자 증류됨. 면역계 건강 증진 및 지질 성분 최적 유지.", "suggested_use": "매일 1~2회, 식사와 함께 소프트젤 1정씩 복용하십시오.", "other_ingredients": "소프트젤 캡슐(젤라틴, 글리세린, 정제수), 천연 레몬 향.", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Gluten-Free", "Non-GMO"]},
        {"iherb_id": "CGN-01180", "product_id": "70316", "name": "California Gold Nutrition Vitamin D3 5000IU 90 Softgels", "name_ko": "California Gold Nutrition, 비타민D3, 125mcg(5000IU), 소프트젤 90정", "brand": "California Gold Nutrition", "price_usd": 5.85, "price_krw": 8468, "rating": 4.7, "review_count": 316568, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D", "product_form": "Softgel", "count": "90 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/70316", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01180/s/74.jpg", "description": "California Gold Nutrition 비타민D3 125mcg(5000IU). 뼈 건강 및 면역 기능 지원. 칼슘 흡수를 촉진합니다.", "suggested_use": "매일 1회 소프트젤 1정을 식사와 함께 복용하십시오.", "other_ingredients": "해바라기유, 소프트젤 캡슐(젤라틴, 글리세린, 정제수).", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Soy Free", "GMP Certified"]},
        {"iherb_id": "CGN-01059", "product_id": "71026", "name": "California Gold Nutrition Sport Creatine Monohydrate Unflavored 454g", "name_ko": "California Gold Nutrition, 스포츠, 순수 크레아틴 일수화물, 무맛, 454g", "brand": "California Gold Nutrition", "price_usd": 18.43, "price_krw": 26701, "rating": 4.6, "review_count": 31497, "category": "Sports Nutrition", "sub_category": "Creatine", "category_ko": "스포츠 영양", "sub_category_ko": "크레아틴", "product_form": "Powder", "count": "454g", "in_stock": True, "url": "https://www.iherb.com/pr/71026", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01059/s/100.jpg", "description": "California Gold Nutrition 스포츠 크레아틴 일수화물 분말 무맛. 1회 제공량당 크레아틴 일수화물 5g. 순근육량 증가 및 근육 피로 감소에 도움.", "suggested_use": "매일 1스쿱 정량을 6~8oz의 물 또는 원하는 음료에 혼합하여 복용하십시오.", "other_ingredients": "크레아틴일수화물. 기타 성분 없음.", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["GMP Certified"]},
        {"iherb_id": "CGN-02034", "product_id": "143853", "name": "California Gold Nutrition LactoBif 30 Probiotics 30 Billion CFU 120 Capsules", "name_ko": "California Gold Nutrition, LactoBif 30 프로바이오틱, 300억CFU, 베지 캡슐 120정", "brand": "California Gold Nutrition", "price_usd": 29.9, "price_krw": 43290, "rating": 4.6, "review_count": 23104, "category": "Supplements", "sub_category": "Probiotics", "category_ko": "보충제", "sub_category_ko": "유산균", "product_form": "Capsule", "count": "120 Capsules", "in_stock": True, "url": "https://www.iherb.com/pr/143853", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn02034/s/16.jpg", "description": "California Gold Nutrition LactoBif 30 프로바이오틱 300억CFU. 8가지 활성 유산균 함유. 소화 건강 및 면역 기능 지원.", "suggested_use": "매일 1회 캡슐 1정을 복용하십시오. 식사 여부와 관계없이 복용 가능합니다.", "other_ingredients": "베지 캡슐(하이프로멜로오스), 쌀가루.", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 냉장 보관 권장.", "badges": ["Non-GMO", "Soy Free", "GMP Certified"]},
        {"iherb_id": "SOL-03312", "product_id": "36230", "name": "Solgar Vitamin D3 Cholecalciferol 10000IU 120 Softgels", "name_ko": "Solgar, 비타민D3 (콜레칼시페롤), 250mcg(10000IU), 소프트젤 120정", "brand": "Solgar", "price_usd": 13.99, "price_krw": 20260, "rating": 4.7, "review_count": 21847, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D", "product_form": "Softgel", "count": "120 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/36230", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/sol/sol03312/s/98.jpg", "description": "Solgar 비타민D3 250mcg(10,000IU). 뼈, 치아, 근육 건강 유지에 도움. 면역 체계 지원. 1947년 이래 선진 제조 기술.", "suggested_use": "성인은 매일 소프트젤 1정을 식사와 함께 복용하십시오.", "other_ingredients": "홍화유, 젤라틴, 식물성 글리세린, 옥수수유.", "warnings": "임신 또는 수유 중이거나 약물을 복용 중인 경우 의사와 상의하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Gluten-Free", "Non-GMO"]},
        {"iherb_id": "JRW-03022", "product_id": "18800", "name": "Jarrow Formulas Saccharomyces Boulardii + MOS 90 Veggie Capsules", "name_ko": "Jarrow Formulas, 사큐로마이세스 보울라디 + MOS, 90베지 캡슐", "brand": "Jarrow Formulas", "price_usd": 17.99, "price_krw": 26047, "rating": 4.6, "review_count": 17896, "category": "Supplements", "sub_category": "Probiotics", "category_ko": "보충제", "sub_category_ko": "유산균", "product_form": "Capsule", "count": "90 Capsules", "in_stock": True, "url": "https://www.iherb.com/pr/18800", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/jrw/jrw03022/s/30.jpg", "description": "Jarrow Formulas 사큐로마이세스 보울라디 + MOS. 장내 균총 및 장 건강 지원. 여행자 보호. 5십억 생균 보장.", "suggested_use": "매일 1~2회, 캡슐 1정씩 식사 여부와 관계없이 복용하십시오.", "other_ingredients": "MOS(만난올리고당), 셀룰로오스, 마그네슘스테아레이트, 이산화규소.", "warnings": "서늘하고 건조한 곳에 보관하십시오. 면역력이 저하된 경우 의사와 상의하십시오.", "badges": ["Non-GMO", "Vegan", "GMP Certified"]},
        {"iherb_id": "LKA-01960", "product_id": "71610", "name": "Lake Avenue Nutrition Vitamin D3 5000IU 360 Softgels", "name_ko": "Lake Avenue Nutrition, 비타민D3, 125mcg(5000IU), 소프트젤 360정", "brand": "Lake Avenue Nutrition", "price_usd": 11.5, "price_krw": 16649, "rating": 4.7, "review_count": 78923, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D", "product_form": "Softgel", "count": "360 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/71610", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/miz/miz90164/s/6.jpg", "description": "Lake Avenue Nutrition 비타민D3 125mcg(5,000IU). 뼈 건강 및 면역 기능 지원. 고품질 콜레칼시페롤 사용.", "suggested_use": "매일 1회 소프트젤 1정을 식사와 함께 복용하십시오.", "other_ingredients": "해바라기유, 소프트젤 캡슐(젤라틴, 글리세린, 정제수).", "warnings": "어린이의 손이 닿지 않는 곳에 보관하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Soy Free", "GMP Certified"]},
        {"iherb_id": "OGA-00325", "product_id": "69269", "name": "Orgain Organic Protein Powder Plant Based Vanilla Bean 920g", "name_ko": "Orgain, 유기농 프로틴 파우더, 식물성, 바닐라빈 맛, 920g", "brand": "Orgain", "price_usd": 28.49, "price_krw": 41253, "rating": 4.5, "review_count": 2846, "category": "Sports Nutrition", "sub_category": "Protein", "category_ko": "스포츠 영양", "sub_category_ko": "프로틴", "product_form": "Powder", "count": "920g", "in_stock": True, "url": "https://www.iherb.com/pr/69269", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/oga/oga00325/s/13.jpg", "description": "Orgain 유기농 프로틴 파우더 식물성 바닐라빈 맛. 식물성 단백질 21g. 설탕 1g 미만. USDA 유기농 인증. 대두, 글루텐, 유제품 무함유.", "suggested_use": "물 또는 식물성 우유 12oz에 2스쿱을 넣고 셰이커 컵이나 믹서기로 섞으십시오.", "other_ingredients": "유기농 완두콩 단백질, 유기농 현미 단백질, 유기농 녹두 단백질, 유기농 치아씨, 유기농 천연 향료, 유기농 아카시아.", "warnings": "서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "Gluten-Free", "Vegan", "Organic", "Soy Free"]},
        {"iherb_id": "LEX-02218", "product_id": "56886", "name": "Life Extension Super Omega-3 EPA/DHA Fish Oil 120 Softgels", "name_ko": "Life Extension, 슈퍼 오메가3 EPA/DHA, 피쉬 오일, 소프트젤 120정", "brand": "Life Extension", "price_usd": 19.5, "price_krw": 28230, "rating": 4.7, "review_count": 13542, "category": "Supplements", "sub_category": "Fish Oil & Omegas", "category_ko": "보충제", "sub_category_ko": "오메가3", "product_form": "Softgel", "count": "120 Softgels", "in_stock": True, "url": "https://www.iherb.com/pr/56886", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/lex/lex02218/s/51.jpg", "description": "Life Extension 슈퍼 오메가-3 EPA/DHA. IFOS 5성 인증 피쉬 오일. 심혈관, 두뇌, 관절 건강 지원. 참깨 리그난 및 올리브 추출물 함유.", "suggested_use": "매일 2회, 식사와 함께 소프트젤 1정씩 복용하십시오.", "other_ingredients": "소프트젤 캡슐(젤라틴, 글리세린, 정제수), 참깨 리그난 추출물, 올리브 과일 추출물.", "warnings": "항응혈제를 복용 중인 경우 의사와 상의하십시오. 서늘하고 건조한 곳에 보관하십시오.", "badges": ["Non-GMO", "GMP Certified"]}
    ]
    # ── Chrome 브라우저 수집 iHerb 비타민 제품 (2026-04-08) ──
    chrome_scraped_products = [
        {"iherb_id": "CGN-01033", "product_id": "64903", "name": "California Gold Nutrition CollagenUP Hydrolyzed Marine Collagen Peptides 206g", "name_ko": "California Gold Nutrition, CollagenUP®, 가수분해 해양 콜라겐 펩타이드, 히알루론산 및 비타민C 함유, 무맛, 206g(7.26oz)", "brand": "California Gold Nutrition", "price_usd": 18.49, "price_krw": 26777, "rating": 4.7, "review_count": 312084, "category": "Supplements", "sub_category": "Collagen", "category_ko": "보충제", "sub_category_ko": "콜라겐 보충제", "product_form": "Powder", "count": "206g", "in_stock": True, "url": "https://kr.iherb.com/pr/64903", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01033/v/255.jpg", "description": "가수분해 해양 콜라겐 펩타이드, 히알루론산 및 비타민C 함유. 피부 탄력, 관절 건강, 모발/피부/손발톱 지원.", "badges": ["iHerb 브랜드", "베스트셀러"]},
        {"iherb_id": "CGN-02333", "product_id": "124745", "name": "California Gold Nutrition Vitamin D3 + K2 (MK-7) 180 Veggie Capsules", "name_ko": "California Gold Nutrition, 비타민D3 + K2(MK-7), 베지 캡슐 180정", "brand": "California Gold Nutrition", "price_usd": 9.76, "price_krw": 14140, "rating": 4.8, "review_count": 0, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D3 & K2", "product_form": "Capsule", "count": "180 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/124745", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn02333/v/63.jpg", "description": "비타민D3 125mcg + 비타민K2(MK-7) 120mcg. 뼈, 심혈관 건강 지원.", "badges": ["iHerb 브랜드"]},
        {"iherb_id": "BNR-01272", "product_id": "125086", "name": "Best Naturals Inositol 1 lb (454g)", "name_ko": "Best Naturals, 이노시톨, 454g(1lb)", "brand": "Best Naturals", "price_usd": 21.45, "price_krw": 31074, "rating": 4.7, "review_count": 362, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "이노시톨", "product_form": "Powder", "count": "454g", "in_stock": True, "url": "https://kr.iherb.com/pr/125086", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/bnr/bnr01272/v/8.jpg", "description": "미오이노시톨 분말. 여성 건강, 인지 기능, 정서적 균형 지원.", "badges": []},
        {"iherb_id": "BNR-01862", "product_id": "125078", "name": "Best Naturals L-Methyl Folate 60 Tablets 25000mcg", "name_ko": "Best Naturals, l-메틸엽산, 60정(1정당 25,000mcg)", "brand": "Best Naturals", "price_usd": 15.01, "price_krw": 21741, "rating": 4.8, "review_count": 128, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "메틸엽산", "product_form": "Tablet", "count": "60 Tablets", "in_stock": True, "url": "https://kr.iherb.com/pr/125078", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/bnr/bnr01862/v/24.jpg", "description": "L-메틸엽산(5-MTHF) 활성형 엽산. 여성 건강, 임산부 영양.", "badges": []},
        {"iherb_id": "CGN-00932", "product_id": "61865", "name": "California Gold Nutrition Gold C USP Grade Vitamin C 1000mg 240 Veggie Capsules", "name_ko": "California Gold Nutrition, Gold C®, USP 등급 비타민C, 1,000mg, 베지 캡슐 240정", "brand": "California Gold Nutrition", "price_usd": 5.90, "price_krw": 8542, "rating": 4.8, "review_count": 377236, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민C", "product_form": "Capsule", "count": "240 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/61865", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn00932/v/298.jpg", "description": "USP 등급 비타민C 1000mg. 면역력, 항산화 지원. 베지 캡슐.", "badges": ["iHerb 브랜드", "베스트셀러"]},
        {"iherb_id": "BNR-01293", "product_id": "125109", "name": "Best Naturals Vitamin B-12 Methylcobalamin 6000mcg 120 Tablets", "name_ko": "Best Naturals, 비타민B-12(메틸코발라민), 6,000mcg, 120정", "brand": "Best Naturals", "price_usd": 21.44, "price_krw": 31059, "rating": 4.7, "review_count": 0, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "메틸코발라민", "product_form": "Tablet", "count": "120 Tablets", "in_stock": True, "url": "https://kr.iherb.com/pr/125109", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/bnr/bnr01293/v/32.jpg", "description": "메틸코발라민 B-12 고함량. 에너지 대사, 신경계 건강 지원.", "badges": []},
        {"iherb_id": "BNR-01863", "product_id": "125077", "name": "Best Naturals P-5-P Pyridoxal-5-Phosphate 120 Tablets 50mg", "name_ko": "Best Naturals, P-5-P(피리독살-5-포스페이트), 120정(1정당 50mg)", "brand": "Best Naturals", "price_usd": 10.30, "price_krw": 14909, "rating": 4.8, "review_count": 359, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민B6", "product_form": "Tablet", "count": "120 Tablets", "in_stock": True, "url": "https://kr.iherb.com/pr/125077", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/bnr/bnr01863/v/24.jpg", "description": "활성형 비타민B6 피리독살-5-포스페이트. 아미노산 대사, 신경계 지원.", "badges": []},
        {"iherb_id": "CGN-00931", "product_id": "61864", "name": "California Gold Nutrition Gold C USP Grade Vitamin C 1000mg 60 Veggie Capsules", "name_ko": "California Gold Nutrition, Gold C®, USP 등급 비타민C, 1,000mg, 베지 캡슐 60정", "brand": "California Gold Nutrition", "price_usd": 5.90, "price_krw": 8542, "rating": 4.8, "review_count": 0, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민C", "product_form": "Capsule", "count": "60 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/61864", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn00931/v/383.jpg", "description": "USP 등급 비타민C 1000mg. 면역력, 항산화 지원. 베지 캡슐 60정.", "badges": ["iHerb 브랜드"]},
        {"iherb_id": "CGN-01065", "product_id": "70316", "name": "California Gold Nutrition Vitamin D3 125mcg 5000IU 90 Fish Gelatin Softgels", "name_ko": "California Gold Nutrition, 비타민D3, 125mcg(5,000IU), 피쉬 젤라틴 소프트젤 90정", "brand": "California Gold Nutrition", "price_usd": 5.85, "price_krw": 8468, "rating": 4.8, "review_count": 316568, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D", "product_form": "Softgel", "count": "90 Softgels", "in_stock": True, "url": "https://kr.iherb.com/pr/70316", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01065/v/204.jpg", "description": "비타민D3 5,000IU. 뼈, 면역력, 근육 건강 지원. 피쉬 젤라틴 소프트젤.", "badges": ["iHerb 브랜드", "베스트셀러"]},
        {"iherb_id": "CGN-01032", "product_id": "64902", "name": "California Gold Nutrition CollagenUP Hydrolyzed Marine Collagen Peptides 464g", "name_ko": "California Gold Nutrition, CollagenUP®, 가수분해 해양 콜라겐 펩타이드, 히알루론산 및 비타민C 함유, 무맛, 464g(1.02lb)", "brand": "California Gold Nutrition", "price_usd": 39.30, "price_krw": 56929, "rating": 4.7, "review_count": 312084, "category": "Supplements", "sub_category": "Collagen", "category_ko": "보충제", "sub_category_ko": "콜라겐 보충제", "product_form": "Powder", "count": "464g", "in_stock": True, "url": "https://kr.iherb.com/pr/64902", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01032/v/292.jpg", "description": "가수분해 해양 콜라겐 펩타이드 대용량. 히알루론산 및 비타민C 함유. 피부 탄력, 관절 건강.", "badges": ["iHerb 브랜드"]},
        {"iherb_id": "CGN-02332", "product_id": "124743", "name": "California Gold Nutrition Vitamin D3 + K2 (MK-7) 60 Veggie Capsules", "name_ko": "California Gold Nutrition, 비타민D3 + K2(MK-7), 베지 캡슐 60정", "brand": "California Gold Nutrition", "price_usd": 9.76, "price_krw": 14140, "rating": 4.8, "review_count": 0, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D3 & K2", "product_form": "Capsule", "count": "60 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/124743", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn02332/v/78.jpg", "description": "비타민D3 125mcg + 비타민K2(MK-7) 120mcg. 뼈, 심혈관 건강 지원.", "badges": ["iHerb 브랜드"]},
        {"iherb_id": "DVH-84965", "product_id": "139389", "name": "Divine Health Brain Zone Basic 120 Capsules", "name_ko": "Divine Health, Brain Zone® 베이직, 캡슐 120정", "brand": "Divine Health", "price_usd": 32.60, "price_krw": 47210, "rating": 4.8, "review_count": 6916, "category": "Supplements", "sub_category": "Brain & Cognitive", "category_ko": "보충제", "sub_category_ko": "두뇌 & 인지", "product_form": "Capsule", "count": "120 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/139389", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/dvh/dvh84965/v/24.jpg", "description": "브레인 존 베이직. 집중력, 기억력, 인지 기능 지원. 비타민B, 아미노산 복합.", "badges": []},
        {"iherb_id": "NOW-00475", "product_id": "684", "name": "NOW Foods Inositol 500mg 100 Veg Capsules", "name_ko": "NOW Foods, 이노시톨, 500mg, 베지 캡슐 100정", "brand": "NOW Foods", "price_usd": 8.99, "price_krw": 13022, "rating": 4.8, "review_count": 0, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "이노시톨", "product_form": "Capsule", "count": "100 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/684", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/now/now00475/v/70.jpg", "description": "이노시톨 500mg 베지 캡슐. 세포 신호 전달, 정서 건강 지원.", "badges": ["GMP Certified"]},
        {"iherb_id": "NCS-67495", "product_id": "124160", "name": "Nutricost Women Myo D-Chiro Inositol 120 Capsules", "name_ko": "Nutricost, 여성, 미오 및 d-카이로 이노시톨, 캡슐 120정", "brand": "Nutricost", "price_usd": 13.69, "price_krw": 19830, "rating": 4.8, "review_count": 3741, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "이노시톨", "product_form": "Capsule", "count": "120 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/124160", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/ncs/ncs67495/v/24.jpg", "description": "여성용 미오 + d-카이로 이노시톨 40:1 비율. 호르몬 균형, 여성 건강 지원.", "badges": []},
        {"iherb_id": "BNR-01845", "product_id": "125076", "name": "Best Naturals Niacinamide 500mg 240 Tablets", "name_ko": "Best Naturals, 나이아신아마이드, 500mg, 240정", "brand": "Best Naturals", "price_usd": 12.87, "price_krw": 18641, "rating": 4.6, "review_count": 79, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민B3", "product_form": "Tablet", "count": "240 Tablets", "in_stock": True, "url": "https://kr.iherb.com/pr/125076", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/bnr/bnr01845/v/24.jpg", "description": "나이아신아마이드(비타민B3) 500mg. 에너지 대사, 피부 건강 지원.", "badges": []},
        {"iherb_id": "CGN-01179", "product_id": "77548", "name": "California Gold Nutrition Vitamin D3 50mcg 2000IU 90 Fish Gelatin Softgels", "name_ko": "California Gold Nutrition, 비타민D3, 50mcg(2,000IU), 피쉬 젤라틴 소프트젤 90정", "brand": "California Gold Nutrition", "price_usd": 5.11, "price_krw": 7407, "rating": 4.9, "review_count": 0, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민D", "product_form": "Softgel", "count": "90 Softgels", "in_stock": True, "url": "https://kr.iherb.com/pr/77548", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn01179/v/191.jpg", "description": "비타민D3 2,000IU. 뼈, 면역력 건강 지원. 피쉬 젤라틴 소프트젤.", "badges": ["iHerb 브랜드"]},
        {"iherb_id": "THR-10403", "product_id": "18791", "name": "Thorne Basic B Complex 60 Capsules", "name_ko": "Thorne, 기본 B 복합체, 캡슐 60정", "brand": "Thorne", "price_usd": 21.71, "price_krw": 31432, "rating": 4.8, "review_count": 22052, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민B 복합체", "product_form": "Capsule", "count": "60 Capsules", "in_stock": True, "url": "https://kr.iherb.com/pr/18791", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/thr/thr10403/v/54.jpg", "description": "Thorne 기본 B 복합체. 활성형 비타민B 포뮬라. 에너지 대사, 신경계 건강 지원.", "badges": ["NSF Certified"]},
        {"iherb_id": "CGN-00854", "product_id": "69309", "name": "California Gold Nutrition Folinic Acid Alcohol Free 30ml", "name_ko": "California Gold Nutrition, 폴린산, 알코올 무함유, 30ml(1fl oz)", "brand": "California Gold Nutrition", "price_usd": 12.89, "price_krw": 18674, "rating": 4.9, "review_count": 3593, "category": "Supplements", "sub_category": "Vitamins", "category_ko": "보충제", "sub_category_ko": "비타민B", "product_form": "Liquid", "count": "30ml", "in_stock": True, "url": "https://kr.iherb.com/pr/69309", "image_url": "https://cloudinary.images-iherb.com/image/upload/f_auto,q_auto:eco/images/cgn/cgn00854/v/79.jpg", "description": "폴린산 액상. 알코올 무함유. 엽산의 활성 형태. 임산부, 여성 건강 지원.", "badges": ["iHerb 브랜드"]},
    ]
    demo_iherb_products.extend(chrome_scraped_products)

    for ip in demo_iherb_products:
        iherb_product = IHerbProduct(
            iherb_id=ip["iherb_id"], product_id=ip["product_id"], name=ip["name"],
            name_ko=ip["name_ko"], brand=ip["brand"], price_usd=ip.get("price_usd", 0),
            price_krw=ip.get("price_krw", 0), rating=ip["rating"], review_count=ip["review_count"],
            category=ip.get("category", ""), sub_category=ip.get("sub_category", ""),
            category_ko=ip.get("category_ko", ""), sub_category_ko=ip.get("sub_category_ko", ""),
            product_form=ip.get("product_form", ""), count=ip.get("count", ""),
            in_stock=ip.get("in_stock", True), url=ip.get("url", ""), image_url=ip.get("image_url", ""),
            category_path=f'{ip.get("category", "")} > {ip.get("sub_category", "")}',
            category_path_ko=f'{ip.get("category_ko", "")} > {ip.get("sub_category_ko", "")}',
            description=ip.get("description", ""),
            suggested_use=ip.get("suggested_use", ""),
            other_ingredients=ip.get("other_ingredients", ""),
            warnings=ip.get("warnings", ""),
            badges=ip.get("badges", []),
        )
        db.merge(iherb_product)

    db.commit()
    print(f"Seeded {len(demo_products)} products, {len(demo_mappings)} mappings, {len(demo_reviews)} reviews, {len(demo_iherb_products)} iHerb products (incl. {len(chrome_scraped_products)} Chrome-scraped)")

# ── Run ──────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
