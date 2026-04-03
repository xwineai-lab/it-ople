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

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/ople.db")

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


class ScrapeJob(Base):
    __tablename__ = "scrape_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50))  # "ople_products", "ople_reviews", "iherb_mapping"
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    total_items = Column(Integer, default=0)
    processed_items = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


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
