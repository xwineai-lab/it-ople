"""
Database models and setup for IT.OPLE
SQLAlchemy + SQLite (dev) / PostgreSQL (prod)
"""

import os
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text, DateTime, Boolean,
    ForeignKey, JSON, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Render persistent disk: mount at /var/data in Render dashboard.
# Fallback to local ./data for development.
_DEFAULT_DB_DIR = "/var/data" if os.path.isdir("/var/data") else "./data"
_DB_PATH = os.path.join(_DEFAULT_DB_DIR, "ople.db")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{_DB_PATH}")

# SQLite needs check_same_thread=False
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ── Models ───────────────────────────────────────────────

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    it_id = Column(String(20), unique=True, index=True, nullable=False)
    name_ko = Column(String(500))
    name_en = Column(String(500))
    brand = Column(String(200), index=True)
    price_usd = Column(Float, default=0.0)
    price_krw = Column(Integer, default=0)
    category_id = Column(String(20), index=True)
    category_name = Column(String(200))
    parent_category = Column(String(200))
    review_count = Column(Integer, default=0)
    image_url = Column(Text)
    description = Column(Text)
    url = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    reviews = relationship("Review", back_populates="product")
    mapping = relationship("IHerbMapping", back_populates="product", uselist=False)

    __table_args__ = (
        Index("idx_brand_category", "brand", "category_id"),
    )


class Review(Base):
    __tablename__ = "reviews"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(20), ForeignKey("products.it_id"), index=True)
    reviewer = Column(String(200))
    rating = Column(Integer, default=5)
    text = Column(Text)
    date = Column(String(50))
    keywords = Column(JSON)  # extracted keywords
    created_at = Column(DateTime, default=datetime.utcnow)

    product = relationship("Product", back_populates="reviews")


class IHerbMapping(Base):
    __tablename__ = "iherb_mappings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ople_id = Column(String(20), ForeignKey("products.it_id"), unique=True, index=True)
    iherb_id = Column(String(50))
    iherb_name = Column(String(500))
    iherb_brand = Column(String(200))
    iherb_price_usd = Column(Float)
    iherb_url = Column(Text)
    match_method = Column(String(20))  # upc, fuzzy, ai, manual
    match_score = Column(Float)
    price_diff = Column(Float)
    price_diff_pct = Column(Float)
    verified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    product = relationship("Product", back_populates="mapping")


# ── iHerb Full Product Model ────────────────────────────
# 아이허브 상품 전체 정보를 저장하는 포괄적 모델

class IHerbProduct(Base):
    __tablename__ = "iherb_products"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── 기본 식별 정보 ──
    iherb_id = Column(String(50), unique=True, index=True, nullable=False)  # e.g. "NOW-01652"
    product_id = Column(String(20), index=True)  # iHerb numeric product ID
    url = Column(Text)
    slug = Column(String(500))  # URL slug

    # ── 상품명 ──
    name = Column(String(1000))
    name_ko = Column(String(1000))  # 한국어 상품명
    subtitle = Column(String(500))  # sub-title or tagline

    # ── 브랜드 정보 ──
    brand = Column(String(300), index=True)
    brand_ko = Column(String(300))  # 한국어 브랜드명 (e.g. "올게인")
    brand_url = Column(Text)
    manufacturer = Column(String(300))

    # ── 가격 정보 ──
    price_usd = Column(Float, default=0.0)
    price_original = Column(Float, default=0.0)  # 할인 전 원래 가격
    discount_pct = Column(Float, default=0.0)  # 할인율
    price_krw = Column(Integer, default=0)
    price_per_unit = Column(String(100))  # e.g. "₩250.00 / Count"
    in_stock = Column(Boolean, default=True)
    stock_status = Column(String(100))  # "In Stock", "Out of Stock", "Low Stock"
    currency = Column(String(10), default="USD")

    # ── 카테고리 ──
    category = Column(String(300), index=True)
    sub_category = Column(String(300))
    category_path = Column(Text)  # 전체 카테고리 경로: "Supplements > Vitamins > Vitamin C"
    category_ko = Column(String(300))  # 한국어 카테고리
    sub_category_ko = Column(String(300))  # 한국어 서브카테고리
    category_path_ko = Column(Text)  # 한국어 카테고리 경로: "보충제 > 비타민 > 비타민 C"
    category_ids = Column(JSON)  # [cat_id1, cat_id2, ...]

    # ── 이미지 ──
    image_url = Column(Text)  # 메인 이미지
    image_urls = Column(JSON)  # 모든 이미지 URL 리스트 [url1, url2, ...]
    thumbnail_url = Column(Text)

    # ── 평점 & 리뷰 ──
    rating = Column(Float, default=0.0)  # 평균 별점 (0-5)
    review_count = Column(Integer, default=0)
    rating_distribution = Column(JSON)  # {5: 1234, 4: 567, 3: 89, 2: 12, 1: 3}
    top_positive_review = Column(Text)
    top_critical_review = Column(Text)

    # ── 상품 설명 ──
    description = Column(Text)  # 메인 설명 (영문)
    description_ko = Column(Text)  # 한국어 설명
    description_html = Column(Text)  # HTML 원본
    features = Column(JSON)  # 주요 특징 리스트 ["Non-GMO", "Vegan", ...]
    features_ko = Column(JSON)  # 한국어 특징 리스트
    warnings = Column(Text)  # 주의사항 (영문)
    warnings_ko = Column(Text)  # 한국어 주의사항
    suggested_use = Column(Text)  # 복용법/사용법 (영문)
    suggested_use_ko = Column(Text)  # 한국어 복용법/사용법
    storage_info = Column(Text)  # 보관방법 (영문)
    storage_info_ko = Column(Text)  # 한국어 보관방법

    # ── 성분 & 영양정보 ──
    ingredients = Column(Text)  # 전체 성분 텍스트 (영문)
    ingredients_ko = Column(Text)  # 한국어 성분 텍스트
    ingredients_list = Column(JSON)  # 파싱된 성분 리스트
    supplement_facts = Column(JSON)  # 영양성분표 [{name, amount, daily_value}, ...]
    nutrition_facts = Column(JSON)  # 일반 영양정보 (식품용)
    other_ingredients = Column(Text)  # 기타 성분 (영문)
    other_ingredients_ko = Column(Text)  # 한국어 기타 성분
    allergen_info = Column(Text)  # 알레르기 정보 (영문)
    allergen_info_ko = Column(Text)  # 한국어 알레르기 정보

    # ── 상품 규격 ──
    serving_size = Column(String(200))  # 1회 섭취량
    servings_per_container = Column(String(100))  # 총 섭취 횟수
    product_form = Column(String(100))  # 제형: Capsule, Tablet, Powder, Liquid...
    count = Column(String(100))  # 총 수량: "180 Softgels", "250 Tablets"
    weight = Column(String(100))  # 무게
    dimensions = Column(String(200))  # 크기
    upc_barcode = Column(String(50), index=True)  # UPC 바코드
    sku = Column(String(50))  # SKU

    # ── 인증 & 배지 ──
    badges = Column(JSON)  # ["Non-GMO", "Vegan", "Gluten-Free", "GMP Certified", ...]
    certifications = Column(JSON)  # 공식 인증 정보
    best_by_date = Column(String(50))  # 유통기한
    date_first_available = Column(String(50))  # 최초 출시일

    # ── Q&A ──
    qa_count = Column(Integer, default=0)
    top_questions = Column(JSON)  # [{question, answer}, ...]

    # ── 관련 상품 ──
    related_products = Column(JSON)  # [{id, name, price, image}, ...]
    also_bought = Column(JSON)  # 함께 구매한 상품
    bundle_deals = Column(JSON)  # 번들 할인 정보

    # ── 배송 정보 ──
    shipping_weight = Column(String(100))
    ships_from = Column(String(200))  # 출하지

    # ── 메타 데이터 ──
    page_title = Column(String(500))
    meta_description = Column(Text)
    tags = Column(JSON)  # 태그 리스트
    popularity_rank = Column(Integer)  # 인기 순위

    # ── 리뷰 상세 (상위 리뷰들) ──
    reviews_data = Column(JSON)  # [{reviewer, rating, title, text, date, helpful_count}, ...]

    # ── 타임스탬프 ──
    scraped_at = Column(DateTime, default=datetime.utcnow)
    ko_scraped_at = Column(DateTime)  # 한국어 데이터 수집 시간
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_iherb_brand_cat", "brand", "category"),
        Index("idx_iherb_price", "price_usd"),
        Index("idx_iherb_rating", "rating"),
    )


# ── OPLE Category System ───────────────────────────────
# 오플닷컴 카테고리 (태그 전략의 기반 데이터)
# category_depth 예: "대상별 > 부모님 > 혈행/혈압/당뇨"
# → Shopify 태그: cat:대상별, sub:부모님, sub2:혈행/혈압/당뇨

class Category(Base):
    """OPLE category tree — 517 unique categories across multiple axes."""
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(String(20), unique=True, index=True, nullable=False)  # 오플 카테고리 코드 (e.g. "101213")
    depth_path = Column(String(500), nullable=False)      # 전체 경로: "대상별 > 부모님 > 혈행/혈압/당뇨"
    level1 = Column(String(100), index=True)               # 1차: "대상별"
    level2 = Column(String(100), index=True)               # 2차: "부모님"
    level3 = Column(String(100))                           # 3차: "혈행/혈압/당뇨"
    depth = Column(Integer, default=1)                     # 깊이 (1~3)
    product_count = Column(Integer, default=0)             # 이 카테고리에 속한 상품 수

    # Shopify 태그 매핑
    shopify_tag_cat = Column(String(100))    # cat:{level1} → Shopify main category tag
    shopify_tag_sub = Column(String(100))    # sub:{level2} → Shopify subcategory tag
    shopify_tag_sub2 = Column(String(100))   # sub2:{level3} → Shopify 3rd-level tag

    created_at = Column(DateTime, default=datetime.utcnow)

    products = relationship("ProductCategory", back_populates="category")

    __table_args__ = (
        Index("idx_cat_level12", "level1", "level2"),
    )


class ProductCategory(Base):
    """Many-to-many: one product can belong to multiple categories."""
    __tablename__ = "product_categories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    it_id = Column(String(20), index=True, nullable=False)        # OPLE product ID
    category_id = Column(String(20), ForeignKey("categories.category_id"), index=True, nullable=False)

    category = relationship("Category", back_populates="products")

    __table_args__ = (
        Index("idx_pc_unique", "it_id", "category_id", unique=True),
    )


class ShopifyProduct(Base):
    """Curated products selected for Shopify. Separate from WMS products table
    so WMS data remains untouched and selection state is independent."""
    __tablename__ = "shopify_products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    it_id = Column(String(32), unique=True, index=True, nullable=False)
    # e.g., WMS Master SKU like "3M-P022334"

    # ── Selection state ──
    status = Column(String(20), default="candidate", index=True)
    # candidate → approved → syncing → synced / failed / archived

    wave = Column(String(50), index=True)
    # "1차-런칭", "2차", "Japan", etc.

    priority = Column(Integer, default=0)

    # ── Shopify sync state ──
    shopify_product_id = Column(String(100))   # gid://shopify/Product/xxx
    shopify_handle = Column(String(200))       # URL slug
    shopify_status = Column(String(20))        # active / draft / archived
    last_synced_at = Column(DateTime)
    sync_error = Column(Text)

    # ── Selection metadata ──
    selected_by = Column(Integer)              # optional user id
    selected_at = Column(DateTime, default=datetime.utcnow)
    notes = Column(Text)

    # ── Override fields (Shopify 전용 값, null 이면 WMS 원본 사용) ──
    custom_title = Column(String(500))
    custom_description = Column(Text)
    custom_tags = Column(Text)                 # JSON array of extra tags
    custom_price_usd = Column(Float)           # Shopify 판매가 override
    custom_compare_at_price = Column(Float)    # Shopify 할인전 가격

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_sp_status_wave", "status", "wave"),
    )


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50))  # "ople_products", "ople_reviews", "iherb_mapping", "iherb_full"
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    total_items = Column(Integer, default=0)
    processed_items = Column(Integer, default=0)
    error_message = Column(Text)
    config = Column(JSON)  # 스크래핑 설정 (카테고리, 페이지 수 등)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class User(Base):
    """User accounts with Google OAuth and role-based access."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(320), unique=True, nullable=False, index=True)
    name = Column(String(200))
    picture = Column(Text)               # Google profile picture URL
    google_uid = Column(String(128), unique=True, index=True)
    role = Column(String(20), default="viewer")  # admin | editor | viewer
    is_active = Column(Boolean, default=True)
    last_login = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ── DB Initialization ────────────────────────────────────

def init_db():
    """Create all tables."""
    os.makedirs("data", exist_ok=True)
    Base.metadata.create_all(bind=engine)


def get_db():
    """Dependency for FastAPI."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
