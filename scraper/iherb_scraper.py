"""
iHerb.com Comprehensive Product Scraper
────────────────────────────────────────
iHerb 건강보조제(Supplements) 전체 카테고리 상품을
최대한 많은 정보와 함께 수집합니다.

수집 정보:
  - 기본: 상품명, 브랜드, 가격(USD/KRW), 할인정보, 재고상태
  - 이미지: 메인이미지, 전체 이미지 리스트, 썸네일
  - 성분: 전체 성분표, 영양성분표(Supplement Facts), 알레르기정보
  - 설명: 상품설명, 특징, 주의사항, 복용법, 보관방법
  - 리뷰: 평점, 리뷰수, 평점분포, 상위 리뷰
  - 규격: 제형, 수량, 무게, 크기, UPC바코드, SKU
  - 인증: 배지(Non-GMO, Vegan 등), 인증정보
  - Q&A: 질문/답변 수, 상위 Q&A
  - 관련: 관련상품, 함께구매, 번들딜
  - 메타: 카테고리경로, 태그, 인기순위

Rate Limiting:
  - 요청 간 2초 딜레이 (서버 부담 최소화)
  - 에러 시 exponential backoff
  - 동시 요청 없음 (순차 처리)
"""

import asyncio
import json
import re
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path

import httpx
from bs4 import BeautifulSoup, Tag

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("iherb_scraper")

BASE_URL = "https://www.iherb.com"
BASE_URL_KR = "https://kr.iherb.com"
DELAY = 2.0  # seconds between requests
MAX_RETRIES = 3

# ── iHerb Supplements 카테고리 맵 ──────────────────────

SUPPLEMENT_CATEGORIES = {
    "vitamins": {"path": "/c/vitamins", "name_ko": "비타민"},
    "supplements": {"path": "/c/supplements", "name_ko": "보충제"},
    "minerals": {"path": "/c/minerals", "name_ko": "미네랄"},
    "amino-acids": {"path": "/c/amino-acids", "name_ko": "아미노산"},
    "antioxidants": {"path": "/c/antioxidants", "name_ko": "항산화제"},
    "bone-joint": {"path": "/c/bone-joint", "name_ko": "뼈/관절"},
    "brain-cognitive": {"path": "/c/brain-cognitive", "name_ko": "두뇌/인지"},
    "childrens-health": {"path": "/c/childrens-health", "name_ko": "어린이건강"},
    "digestive-support": {"path": "/c/digestive-support", "name_ko": "소화건강"},
    "energy": {"path": "/c/energy-supplements", "name_ko": "에너지"},
    "eye-health": {"path": "/c/eye-health", "name_ko": "눈건강"},
    "fish-oil-omegas": {"path": "/c/fish-oil-omegas-epa-dha", "name_ko": "피쉬오일/오메가"},
    "heart-health": {"path": "/c/heart-support", "name_ko": "심장건강"},
    "herbs-homeopathy": {"path": "/c/herbs-homeopathy", "name_ko": "허브/동종요법"},
    "immune-support": {"path": "/c/immune-support", "name_ko": "면역"},
    "mens-health": {"path": "/c/mens-health", "name_ko": "남성건강"},
    "probiotics": {"path": "/c/probiotics", "name_ko": "유산균"},
    "protein": {"path": "/c/protein", "name_ko": "프로틴"},
    "sleep": {"path": "/c/sleep-formulas", "name_ko": "수면"},
    "sports-nutrition": {"path": "/c/sports-nutrition", "name_ko": "스포츠영양"},
    "weight-management": {"path": "/c/weight-loss", "name_ko": "체중관리"},
    "womens-health": {"path": "/c/womens-health", "name_ko": "여성건강"},
    "superfoods": {"path": "/c/superfoods", "name_ko": "슈퍼푸드"},
    "collagen": {"path": "/c/collagen-supplements", "name_ko": "콜라겐"},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


# ── Helper Functions ───────────────────────────────────

def clean_text(text: str) -> str:
    """Clean whitespace from text."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def parse_price(text: str) -> float:
    """Extract numeric price from text like '$25.99' or '₩38,900'."""
    if not text:
        return 0.0
    numbers = re.findall(r'[\d,.]+', text.replace(',', ''))
    if numbers:
        try:
            return float(numbers[0])
        except ValueError:
            return 0.0
    return 0.0


def extract_number(text: str) -> int:
    """Extract integer from text like '1,234 Reviews'."""
    if not text:
        return 0
    numbers = re.findall(r'[\d,]+', text.replace(',', ''))
    if numbers:
        try:
            return int(numbers[0])
        except ValueError:
            return 0
    return 0


async def fetch_with_retry(client: httpx.AsyncClient, url: str, retries: int = MAX_RETRIES) -> Optional[httpx.Response]:
    """Fetch URL with retry and exponential backoff."""
    for attempt in range(retries):
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:  # Rate limited
                wait = (2 ** attempt) * 5
                logger.warning(f"Rate limited (429), waiting {wait}s...")
                await asyncio.sleep(wait)
            elif resp.status_code == 403:
                logger.warning(f"Forbidden (403) for {url}, attempt {attempt + 1}")
                await asyncio.sleep(5)
            else:
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return resp
        except Exception as e:
            logger.error(f"Request error for {url}: {e}")
            await asyncio.sleep(2 ** attempt)

    return None


# ── Category & Product List Scraping ───────────────────

async def get_category_page_count(client: httpx.AsyncClient, category_path: str) -> int:
    """Get total pages for a category listing."""
    url = f"{BASE_URL}{category_path}?p=1&pageSize=24"
    resp = await fetch_with_retry(client, url)
    if not resp:
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")

    # Try to find pagination info
    # iHerb uses various pagination patterns
    page_info = soup.find(class_="pagination")
    if page_info:
        page_links = page_info.find_all("a")
        max_page = 1
        for link in page_links:
            text = link.get_text(strip=True)
            try:
                page_num = int(text)
                max_page = max(max_page, page_num)
            except ValueError:
                pass
        return max_page

    # Fallback: check for "showing X of Y results"
    result_count = soup.find(class_="sub-header-title")
    if result_count:
        total_match = re.search(r'(\d[\d,]*)\s*(?:Results|개)', result_count.get_text())
        if total_match:
            total = int(total_match.group(1).replace(',', ''))
            return (total + 23) // 24  # 24 items per page

    # Also try product-count element
    count_el = soup.find(attrs={"data-ga-event-action": "product_count"})
    if count_el:
        count = extract_number(count_el.get_text())
        if count > 0:
            return (count + 23) // 24

    return 1


async def scrape_category_listing(client: httpx.AsyncClient, category_path: str, page: int) -> list[dict]:
    """Scrape product IDs and basic info from a category listing page."""
    url = f"{BASE_URL}{category_path}?p={page}&pageSize=24"
    resp = await fetch_with_retry(client, url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    products = []

    # iHerb product cards - try multiple selectors
    product_cards = soup.find_all(class_="product-cell-container")
    if not product_cards:
        product_cards = soup.find_all(attrs={"data-ga-event-action": "product"})
    if not product_cards:
        product_cards = soup.find_all(class_="product-inner")

    for card in product_cards:
        try:
            product = extract_listing_info(card)
            if product and product.get("product_id"):
                products.append(product)
        except Exception as e:
            logger.debug(f"Error parsing product card: {e}")

    # Fallback: try parsing from JSON-LD or script data
    if not products:
        products = extract_products_from_scripts(soup)

    return products


def extract_listing_info(card) -> Optional[dict]:
    """Extract basic product info from a listing card."""
    info = {}

    # Product link & ID
    link = card.find("a", href=re.compile(r"/pr/"))
    if link:
        href = link.get("href", "")
        info["url"] = href if href.startswith("http") else f"{BASE_URL}{href}"
        # Extract product ID from URL like /pr/product-name/12345
        id_match = re.search(r'/pr/[^/]+/(\d+)', href)
        if id_match:
            info["product_id"] = id_match.group(1)
        # Extract slug
        slug_match = re.search(r'/pr/([^/]+)/', href)
        if slug_match:
            info["slug"] = slug_match.group(1)

    if not info.get("product_id"):
        # Try data attributes
        pid = card.get("data-product-id") or card.get("data-pid")
        if pid:
            info["product_id"] = str(pid)
        else:
            return None

    # Product name
    name_el = card.find(class_="product-title") or card.find("bdi")
    if name_el:
        info["name"] = clean_text(name_el.get_text())

    # Brand
    brand_el = card.find(class_="product-brand") or card.find(attrs={"itemprop": "brand"})
    if brand_el:
        info["brand"] = clean_text(brand_el.get_text())

    # Price
    price_el = card.find(class_="price") or card.find(attrs={"itemprop": "price"})
    if price_el:
        price_val = price_el.get("content") or price_el.get_text()
        info["price_usd"] = parse_price(str(price_val))

    # Original price (before discount)
    orig_price = card.find(class_="price-strikethrough") or card.find(class_="discount-old-price")
    if orig_price:
        info["price_original"] = parse_price(orig_price.get_text())

    # Discount
    discount_el = card.find(class_="discount-percentage") or card.find(class_="discount-red")
    if discount_el:
        info["discount_pct"] = parse_price(discount_el.get_text())

    # Rating
    rating_el = card.find(class_="rating") or card.find(attrs={"itemprop": "ratingValue"})
    if rating_el:
        rating_val = rating_el.get("content") or rating_el.get("title")
        if rating_val:
            try:
                info["rating"] = float(rating_val)
            except ValueError:
                pass

    # Review count
    review_el = card.find(class_="rating-count") or card.find(class_="reviews")
    if review_el:
        info["review_count"] = extract_number(review_el.get_text())

    # Image
    img_el = card.find("img")
    if img_el:
        img_src = img_el.get("src") or img_el.get("data-src") or img_el.get("loading-src")
        if img_src:
            info["image_url"] = img_src
            info["thumbnail_url"] = img_src

    # Stock status
    stock_el = card.find(class_="out-of-stock") or card.find(class_="stock-status")
    if stock_el:
        info["in_stock"] = "out" not in stock_el.get_text().lower()
    else:
        info["in_stock"] = True

    return info


def extract_products_from_scripts(soup) -> list[dict]:
    """Extract product data from embedded JavaScript/JSON."""
    products = []
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "ItemList":
                for item in data.get("itemListElement", []):
                    if "item" in item:
                        p = item["item"]
                        products.append({
                            "product_id": str(p.get("productID", "")),
                            "name": p.get("name", ""),
                            "brand": p.get("brand", {}).get("name", ""),
                            "url": p.get("url", ""),
                            "image_url": p.get("image", ""),
                            "price_usd": parse_price(str(p.get("offers", {}).get("price", 0))),
                        })
        except (json.JSONDecodeError, AttributeError):
            continue

    return products


# ── Product Detail Scraping ─────────────────────────────

async def scrape_product_detail(client: httpx.AsyncClient, product_url: str, product_id: str = "") -> Optional[dict]:
    """Scrape comprehensive product details from an iHerb product page.

    This is the main workhorse - extracts EVERYTHING available on the page.
    """
    resp = await fetch_with_retry(client, product_url)
    if not resp or resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    info = {"url": product_url, "product_id": product_id}

    # ── 1. JSON-LD 구조화 데이터 추출 (가장 정화) ──
    json_ld_data = extract_json_ld(soup)
    if json_ld_data:
        info.update(json_ld_data)

    # ── 2. 기본 상품 정보 ──
    info.update(extract_basic_info(soup))

    # ── 3. 가격 & 할인 정보 ──
    info.update(extract_price_info(soup))

    # ── 4. 이미지 (모든 이미지) ──
    info.update(extract_images(soup))

    # ── 5. 평점 & 리뷰 ──
    info.update(extract_rating_info(soup))

    # ── 6. 상품 설명 & 특징 ──
    info.update(extract_description(soup))

    # ── 7. 성분 & 영양정보 (Supplement Facts) ──
    info.update(extract_supplement_facts(soup))

    # ── 8. 상품 규격 ──
    info.update(extract_specifications(soup))

    # ── 9. 인증 & 배지 ──
    info.update(extract_badges(soup))

    # ── 10. 카테고리 경로 ──
    info.update(extract_breadcrumbs(soup))

    # ── 11. Q&A ──
    info.update(extract_qa(soup))

    # ── 12. 관련 상품 ──
    info.update(extract_related_products(soup))

    # ── 13. 배송 정보 ──
    info.update(extract_shipping_info(soup))

    # ── 14. 메타 데이터 ──
    info.update(extract_meta(soup))

    info["scraped_at"] = datetime.utcnow().isoformat()

    return info


# ── Detail Extraction Functions ─────────────────────────

def extract_json_ld(soup) -> dict:
    """Extract structured data from JSON-LD scripts."""
    info = {}
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                info["name"] = data.get("name", "")
                info["description"] = data.get("description", "")
                info["sku"] = data.get("sku", "")
                info["upc_barcode"] = data.get("gtin13") or data.get("gtin12") or data.get("gtin", "")
                info["brand"] = data.get("brand", {}).get("name", "") if isinstance(data.get("brand"), dict) else str(data.get("brand", ""))
                info["image_url"] = data.get("image", "")

                # Offers
                offers = data.get("offers", {})
                if isinstance(offers, dict):
                    info["price_usd"] = parse_price(str(offers.get("price", 0)))
                    info["currency"] = offers.get("priceCurrency", "USD")
                    info["in_stock"] = offers.get("availability", "").endswith("InStock")

                # Rating
                rating = data.get("aggregateRating", {})
                if isinstance(rating, dict):
                    try:
                        info["rating"] = float(rating.get("ratingValue", 0))
                    except (ValueError, TypeError):
                        pass
                    info["review_count"] = extract_number(str(rating.get("reviewCount", 0)))

                # Product ID from URL
                url = data.get("url", "")
                if url:
                    id_match = re.search(r'/(\d+)$', url)
                    if id_match:
                        info["product_id"] = id_match.group(1)

        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    return info


def extract_basic_info(soup) -> dict:
    """Extract basic product info: name, brand, iherb_id."""
    info = {}

    # iHerb product code (e.g. "NOW-01652")
    code_el = soup.find(id="product-specs-list")
    if code_el:
        code_items = code_el.find_all("li")
        for item in code_items:
            text = item.get_text(strip=True)
            if "상품 코드" in text or "Product Code" in text or "제품코드" in text:
                code = text.split(":")[-1].strip()
                info["iherb_id"] = code
            elif "UPC" in text.upper():
                upc = text.split(":")[-1].strip()
                info["upc_barcode"] = upc
            elif "SKU" in text:
                sku = text.split(":")[-1].strip()
                info["sku"] = sku

    # Also try from product-code span
    code_span = soup.find(attrs={"itemprop": "productID"}) or soup.find(class_="product-code")
    if code_span:
        info.setdefault("iherb_id", clean_text(code_span.get_text()).replace("제품코드:", "").replace("Product Code:", "").strip())

    # Title (if not from JSON-LD)
    if not info.get("name"):
        title_el = soup.find("h1", id="name") or soup.find("h1", class_="product-title") or soup.find("h1")
        if title_el:
            info["name"] = clean_text(title_el.get_text())

    # Subtitle
    subtitle_el = soup.find(id="product-subtitle") or soup.find(class_="product-grouping-info")
    if subtitle_el:
        info["subtitle"] = clean_text(subtitle_el.get_text())

    # Brand & manufacturer
    brand_el = soup.find(attrs={"itemprop": "brand"}) or soup.find(class_="brand-name")
    if brand_el:
        brand_link = brand_el.find("a")
        if brand_link:
            info["brand"] = clean_text(brand_link.get_text())
            info["brand_url"] = brand_link.get("href", "")
            if not info["brand_url"].startswith("http"):
                info["brand_url"] = f"{BASE_URL}{info['brand_url']}"

    # Count / amount from title
    name = info.get("name", "")
    count_match = re.search(r'(\d+)\s*(Softgels?|Capsules?|Tablets?|Veggie Caps?|Gummies?|Caplets?|Packets?|Lozenges?|Chewables?)', name, re.IGNORECASE)
    if count_match:
        info["count"] = f"{count_match.group(1)} {count_match.group(2)}"

    weight_match = re.search(r'([\d.]+)\s*(g|kg|oz|lb|ml|fl oz|lbs?)\b', name, re.IGNORECASE)
    if weight_match:
        info["weight"] = f"{weight_match.group(1)} {weight_match.group(2)}"

    return info


def extract_price_info(soup) -> dict:
    """Extract all pricing information."""
    info = {}

    # Current price
    price_el = soup.find(id="price") or soup.find(class_="product-price")
    if price_el:
        price_text = price_el.get_text()
        info["price_usd"] = parse_price(price_text)

    # Original price (before discount)
    orig_el = soup.find(class_="price-strikethrough") or soup.find(id="old-price")
    if orig_el:
        info["price_original"] = parse_price(orig_el.get_text())

    # Discount percentage
    disc_el = soup.find(class_="discount-percentage") or soup.find(class_="discount-red")
    if disc_el:
        info["discount_pct"] = parse_price(disc_el.get_text())

    # Price per unit
    unit_el = soup.find(class_="price-per-unit")
    if unit_el:
        info["price_per_unit"] = clean_text(unit_el.get_text())

    # Stock status
    stock_el = soup.find(class_="out-of-stock-text") or soup.find(id="stock-status")
    if stock_el:
        stock_text = stock_el.get_text(strip=True)
        info["stock_status"] = stock_text
        info["in_stock"] = "out" not in stock_text.lower() and "unavailable" not in stock_text.lower()

    return info


def extract_images(soup) -> dict:
    """Extract all product images."""
    info = {}
    images = []

    # Main product image
    main_img = soup.find(id="iherb-product-image") or soup.find(class_="product-image")
    if main_img:
        img_tag = main_img.find("img") if main_img.name != "img" else main_img
        if img_tag:
            src = img_tag.get("src") or img_tag.get("data-large-img") or img_tag.get("data-src")
            if src:
                info["image_url"] = src
                images.append(src)

    # Thumbnail gallery
    gallery = soup.find(id="product-image-gallery") or soup.find(class_="product-gallery")
    if gallery:
        for img in gallery.find_all("img"):
            src = img.get("data-large-img") or img.get("src") or img.get("data-src")
            if src and src not in images:
                images.append(src)

    # Also from data attributes
    for el in soup.find_all(attrs={"data-large-img": True}):
        src = el.get("data-large-img")
        if src and src not in images:
            images.append(src)

    if images:
        info["image_urls"] = images
        info["thumbnail_url"] = images[0] if images else ""
        if not info.get("image_url"):
            info["image_url"] = images[0]

    return info


def extract_rating_info(soup) -> dict:
    """Extract rating and review information."""
    info = {}

    # Rating stars
    rating_el = soup.find(attrs={"itemprop": "ratingValue"})
    if rating_el:
        try:
            info["rating"] = float(rating_el.get("content", 0))
        except (ValueError, TypeError):
            pass

    # Review count
    count_el = soup.find(attrs={"itemprop": "reviewCount"})
    if count_el:
        info["review_count"] = extract_number(count_el.get("content") or count_el.get_text())

    # Rating distribution (5-star, 4-star, etc.)
    distribution = {}
    rating_bars = soup.find_all(class_="rating-bar") or soup.find_all(class_="star-bar")
    for bar in rating_bars:
        star_match = re.search(r'(\d)', bar.get_text())
        count_el = bar.find(class_="count") or bar.find(class_="total")
        if star_match and count_el:
            stars = int(star_match.group(1))
            count = extract_number(count_el.get_text())
            distribution[stars] = count

    if distribution:
        info["rating_distribution"] = distribution

    # Top reviews (first few visible)
    reviews = []
    review_cards = soup.find_all(class_="review-card")[:5]
    if not review_cards:
        review_cards = soup.find_all(attrs={"itemprop": "review"})[:5]

    for card in review_cards:
        review = {}
        # Reviewer name
        author = card.find(attrs={"itemprop": "author"})
        if author:
            review["reviewer"] = clean_text(author.get_text())

        # Rating
        r_val = card.find(attrs={"itemprop": "ratingValue"})
        if r_val:
            try:
                review["rating"] = int(float(r_val.get("content", 5)))
            except (ValueError, TypeError):
                review["rating"] = 5

        # Title
        title_el = card.find(class_="review-title") or card.find("b")
        if title_el:
            review["title"] = clean_text(title_el.get_text())

        # Text
        body_el = card.find(class_="review-text") or card.find(attrs={"itemprop": "reviewBody"})
        if body_el:
            review["text"] = clean_text(body_el.get_text())[:500]

        # Date
        date_el = card.find(attrs={"itemprop": "datePublished"})
        if date_el:
            review["date"] = date_el.get("content") or clean_text(date_el.get_text())

        # Helpful count
        helpful_el = card.find(class_="helpful-count") or card.find(class_="review-helpful")
        if helpful_el:
            review["helpful_count"] = extract_number(helpful_el.get_text())

        if review.get("text"):
            reviews.append(review)

    if reviews:
        info["reviews_data"] = reviews
        # Set top positive and critical
        positive = [r for r in reviews if r.get("rating", 5) >= 4]
        critical = [r for r in reviews if r.get("rating", 5) <= 3]
        if positive:
            info["top_positive_review"] = positive[0].get("text", "")
        if critical:
            info["top_critical_review"] = critical[0].get("text", "")

    return info


def extract_description(soup) -> dict:
    """Extract product description, features, warnings, usage."""
    info = {}

    # Main description
    desc_el = soup.find(id="product-desc-content") or soup.find(class_="prodOverviewDetail")
    if desc_el:
        info["description"] = clean_text(desc_el.get_text())
        info["description_html"] = str(desc_el)[:5000]

    # Product overview / features list
    features = []
    feature_list = soup.find(id="product-overview") or soup.find(class_="product-overview")
    if feature_list:
        for li in feature_list.find_all("li"):
            text = clean_text(li.get_text())
            if text:
                features.append(text)

    if not features:
        # Try from description bullets
        if desc_el:
            for li in desc_el.find_all("li"):
                text = clean_text(li.get_text())
                if text and len(text) < 200:
                    features.append(text)

    if features:
        info["features"] = features[:20]

    # Suggested Use / 복용법
    use_el = soup.find(id="suggested-use") or soup.find(string=re.compile(r"Suggested Use|복용법|사용법|권장 사용법", re.IGNORECASE))
    if use_el:
        parent = use_el.parent if use_el.name is None else use_el
        next_content = parent.find_next_sibling() if parent else None
        if next_content:
            info["suggested_use"] = clean_text(next_content.get_text())[:500]
        else:
            info["suggested_use"] = clean_text(parent.get_text())[:500]

    # Warnings / 주의사항
    warn_el = soup.find(id="warnings") or soup.find(string=re.compile(r"Warnings?|주의사항|경고", re.IGNORECASE))
    if warn_el:
        parent = warn_el.parent if warn_el.name is None else warn_el
        next_content = parent.find_next_sibling() if parent else None
        if next_content:
            info["warnings"] = clean_text(next_content.get_text())[:500]
        else:
            info["warnings"] = clean_text(parent.get_text())[:500]

    # Storage info
    storage_el = soup.find(string=re.compile(r"Storage|보관|Store in", re.IGNORECASE))
    if storage_el:
        parent = storage_el.parent if storage_el.name is None else storage_el
        info["storage_info"] = clean_text(parent.get_text())[:300]

    return info


def extract_supplement_facts(soup) -> dict:
    """Extract Supplement Facts / Nutrition Facts table."""
    info = {}

    # ── Supplement Facts 테이블 ──
    supplement_table = soup.find(id="supplement-facts") or soup.find(class_="supplement-facts-table")
    if not supplement_table:
        # Try to find by heading
        sf_heading = soup.find(string=re.compile(r"Supplement Facts|영양성분|보충제 성분표", re.IGNORECASE))
        if sf_heading:
            parent = sf_heading.parent if sf_heading.name is None else sf_heading
            supplement_table = parent.find_next("table") or parent.find_parent("div")

    facts = []
    if supplement_table:
        rows = supplement_table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                name = clean_text(cells[0].get_text())
                amount = clean_text(cells[1].get_text()) if len(cells) > 1 else ""
                daily_value = clean_text(cells[2].get_text()) if len(cells) > 2 else ""

                if name and name.lower() not in ("amount per serving", "% daily value", "성분", "함량", "1일 영양소 기준치"):
                    facts.append({
                        "name": name,
                        "amount": amount,
                        "daily_value": daily_value.replace("%", "").strip(),
                    })

        # Serving size from table header
        header = supplement_table.find(string=re.compile(r"Serving Size|1회 섭취량", re.IGNORECASE))
        if header:
            parent = header.parent if header.name is None else header
            info["serving_size"] = clean_text(parent.get_text()).replace("Serving Size", "").replace("1회 섭취량", "").strip(": ")

        servings = supplement_table.find(string=re.compile(r"Servings Per Container|총 섭취 횟수", re.IGNORECASE))
        if servings:
            parent = servings.parent if servings.name is None else servings
            text = clean_text(parent.get_text())
            num = re.search(r'(\d+)', text)
            if num:
                info["servings_per_container"] = num.group(1)

    if facts:
        info["supplement_facts"] = facts

    # ── 전체 성분 (Ingredients) ──
    ingredients_el = soup.find(id="product-ingredients") or soup.find(class_="ingredientsList")
    if not ingredients_el:
        ing_heading = soup.find(string=re.compile(r"Other Ingredients|기타 성분|Ingredients|성분", re.IGNORECASE))
        if ing_heading:
            parent = ing_heading.parent if ing_heading.name is None else ing_heading
            ingredients_el = parent.find_next_sibling() or parent

    if ingredients_el:
        ingredients_text = clean_text(ingredients_el.get_text())
        info["ingredients"] = ingredients_text[:2000]

        # Parse into list
        parts = re.split(r'[,;]', ingredients_text)
        info["ingredients_list"] = [clean_text(p) for p in parts if clean_text(p)][:50]

    # Other ingredients (별도 표시되는 경우)
    other_ing = soup.find(string=re.compile(r"Other Ingredients|기타 성분"))
    if other_ing:
        parent = other_ing.parent if other_ing.name is None else other_ing
        next_el = parent.find_next_sibling()
        if next_el:
            info["other_ingredients"] = clean_text(next_el.get_text())[:500]

    # Allergen info
    allergen = soup.find(string=re.compile(r"Allergen|Contains:|알레르기|함유|Warning.*contains", re.IGNORECASE))
    if allergen:
        parent = allergen.parent if allergen.name is None else allergen
        info["allergen_info"] = clean_text(parent.get_text())[:300]

    return info


def extract_specifications(soup) -> dict:
    """Extract product specifications: form, weight, dimensions, UPC, etc."""
    info = {}

    # Product specs list
    specs = soup.find(id="product-specs-list") or soup.find(class_="product-specs")
    if specs:
        for item in specs.find_all("li"):
            text = item.get_text(strip=True)
            text_lower = text.lower()

            if any(k in text_lower for k in ["product code", "상품 코드", "제품코드"]):
                info["iherb_id"] = text.split(":")[-1].strip()
            elif any(k in text_lower for k in ["upc code", "upc"]):
                info["upc_barcode"] = text.split(":")[-1].strip()
            elif any(k in text_lower for k in ["package quantity", "수량"]):
                info["count"] = text.split(":")[-1].strip()
            elif any(k in text_lower for k in ["weight", "무게", "shipping weight"]):
                info["shipping_weight"] = text.split(":")[-1].strip()
            elif any(k in text_lower for k in ["dimensions", "크기"]):
                info["dimensions"] = text.split(":")[-1].strip()
            elif any(k in text_lower for k in ["best by", "유통기한"]):
                info["best_by_date"] = text.split(":")[-1].strip()
            elif any(k in text_lower for k in ["date first available", "최초 등록"]):
                info["date_first_available"] = text.split(":")[-1].strip()
            elif "sku" in text_lower:
                info["sku"] = text.split(":")[-1].strip()

    # Product form from title or features
    name = info.get("name", "")
    form_patterns = [
        "Softgels?", "Capsules?", "Tablets?", "Powder", "Liquid", "Gummies?",
        "Chewables?", "Lozenges?", "Drops?", "Spray", "Cream", "Oil",
        "Veggie Caps?", "Caplets?"
    ]
    for pattern in form_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            info["product_form"] = re.search(pattern, name, re.IGNORECASE).group(0)
            break

    return info


def extract_badges(soup) -> dict:
    """Extract product badges and certifications."""
    info = {}
    badges = []
    certs = []

    # Badge containers
    badge_containers = soup.find_all(class_="badge") + soup.find_all(class_="product-flag") + soup.find_all(class_="icon-shield")
    for badge in badge_containers:
        text = clean_text(badge.get_text() or badge.get("title", "") or badge.get("alt", ""))
        if text and len(text) < 100:
            badges.append(text)

    # Known badge patterns in page text
    badge_keywords = [
        "Non-GMO", "Vegan", "Vegetarian", "Gluten-Free", "Gluten Free",
        "Organic", "USDA Organic", "Kosher", "Halal",
        "GMP Certified", "GMP Quality", "NSF Certified",
        "Third Party Tested", "Dairy Free", "Soy Free", "Sugar Free",
        "No Artificial Colors", "No Preservatives", "Made in USA",
        "cGMP", "USP Verified", "Non-GMO Project Verified",
    ]

    page_text = soup.get_text()
    for keyword in badge_keywords:
        if keyword.lower() in page_text.lower() and keyword not in badges:
            badges.append(keyword)

    if badges:
        info["badges"] = list(set(badges))[:30]
    if certs:
        info["certifications"] = certs

    return info


def extract_breadcrumbs(soup) -> dict:
    """Extract category breadcrumbs."""
    info = {}

    breadcrumb = soup.find(class_="breadcrumb") or soup.find(attrs={"itemtype": re.compile("BreadcrumbList")})
    if breadcrumb:
        crumbs = []
        for item in breadcrumb.find_all("a"):
            text = clean_text(item.get_text())
            if text and text.lower() not in ("home", "홈"):
                crumbs.append(text)

        if crumbs:
            info["category_path"] = " > ".join(crumbs)
            if len(crumbs) >= 1:
                info["category"] = crumbs[-1]  # Last crumb is most specific
            if len(crumbs) >= 2:
                info["sub_category"] = crumbs[-1]
                info["category"] = crumbs[-2]

    return info


def extract_qa(soup) -> dict:
    """Extract Q&A section."""
    info = {}

    qa_section = soup.find(id="product-questions") or soup.find(class_="qa-section")
    if qa_section:
        count_el = qa_section.find(class_="count") or qa_section.find(class_="total")
        if count_el:
            info["qa_count"] = extract_number(count_el.get_text())

    # Top questions
    questions = []
    qa_items = soup.find_all(class_="qa-item")[:5]
    for item in qa_items:
        q_el = item.find(class_="question") or item.find("dt")
        a_el = item.find(class_="answer") or item.find("dd")
        if q_el and a_el:
            questions.append({
                "question": clean_text(q_el.get_text())[:300],
                "answer": clean_text(a_el.get_text())[:500],
            })

    if questions:
        info["top_questions"] = questions

    return info


def extract_related_products(soup) -> dict:
    """Extract related products and also-bought suggestions."""
    info = {}

    # Related / Also Bought
    for section_class in ["also-bought", "related-products", "frequently-bought", "customers-also-viewed"]:
        section = soup.find(class_=section_class) or soup.find(id=section_class)
        if section:
            products = []
            for card in section.find_all(class_="product-cell")[:8]:
                p = {}
                link = card.find("a", href=re.compile(r"/pr/"))
                if link:
                    p["url"] = link.get("href", "")
                    id_match = re.search(r'/(\d+)', p["url"])
                    if id_match:
                        p["id"] = id_match.group(1)

                name_el = card.find(class_="product-title")
                if name_el:
                    p["name"] = clean_text(name_el.get_text())[:100]

                price_el = card.find(class_="price")
                if price_el:
                    p["price"] = parse_price(price_el.get_text())

                img_el = card.find("img")
                if img_el:
                    p["image"] = img_el.get("src") or img_el.get("data-src", "")

                if p.get("name"):
                    products.append(p)

            if products:
                key = "also_bought" if "bought" in section_class else "related_products"
                info[key] = products

    return info


def extract_shipping_info(soup) -> dict:
    """Extract shipping information."""
    info = {}

    shipping = soup.find(class_="shipping-info") or soup.find(id="shipping")
    if shipping:
        weight = shipping.find(string=re.compile(r"weight|무게", re.IGNORECASE))
        if weight:
            info["shipping_weight"] = clean_text(weight.parent.get_text() if weight.parent else weight)

        origin = shipping.find(string=re.compile(r"ships from|출하", re.IGNORECASE))
        if origin:
            info["ships_from"] = clean_text(origin.parent.get_text() if origin.parent else origin)

    return info


def extract_meta(soup) -> dict:
    """Extract page meta information."""
    info = {}

    # Page title
    title = soup.find("title")
    if title:
        info["page_title"] = clean_text(title.get_text())

    # Meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        info["meta_description"] = meta_desc.get("content", "")

    # Tags / keywords
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw:
        kw_text = meta_kw.get("content", "")
        info["tags"] = [k.strip() for k in kw_text.split(",") if k.strip()][:30]

    return info


# ── Korean Data Scraper (kr.iherb.com) ──────────────────

def _to_kr_url(url: str) -> str:
    """Convert www.iherb.com URL to kr.iherb.com URL."""
    if not url:
        return ""
    return url.replace("https://www.iherb.com", BASE_URL_KR).replace("http://www.iherb.com", BASE_URL_KR)


async def scrape_korean_detail(client: httpx.AsyncClient, product_url: str) -> dict:
    """
    Scrape Korean-language product data from kr.iherb.com.
    Uses the same URL structure, just with kr.iherb.com domain.
    Returns dict with _ko suffixed fields.
    """
    kr_url = _to_kr_url(product_url)
    if not kr_url:
        return {}

    resp = await fetch_with_retry(client, kr_url)
    if not resp or resp.status_code != 200:
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    info = {}

    # ── 1. JSON-LD 한국어 데이터 (가장 정화) ──
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                info["name_ko"] = data.get("name", "")
                info["description_ko"] = data.get("description", "")
                brand = data.get("brand", {})
                if isinstance(brand, dict):
                    brand_name = brand.get("name", "")
                else:
                    brand_name = str(brand)
                # Extract Korean brand name from pattern like "Orgain (오게인)"
                ko_match = re.search(r'\(([가-힣\s]+)\)', brand_name)
                if ko_match:
                    info["brand_ko"] = ko_match.group(1).strip()
                else:
                    info["brand_ko"] = brand_name
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    # ── 2. 한국어 카테고리 경로 ──
    breadcrumb = soup.find(class_="breadcrumb") or soup.find(attrs={"itemtype": re.compile("BreadcrumbList")})
    if breadcrumb:
        crumbs = []
        for item in breadcrumb.find_all("a"):
            text = clean_text(item.get_text())
            if text and text not in ("홈", "Home"):
                crumbs.append(text)
        if crumbs:
            info["category_path_ko"] = " > ".join(crumbs)
            if len(crumbs) >= 1:
                info["category_ko"] = crumbs[-1]
            if len(crumbs) >= 2:
                info["sub_category_ko"] = crumbs[-1]
                info["category_ko"] = crumbs[-2]

    # ── 3. 한국어 상품 설명 & 특징 ──
    desc_el = soup.find(id="product-desc-content") or soup.find(class_="prodOverviewDetail")
    if desc_el:
        info["description_ko"] = clean_text(desc_el.get_text())[:3000]

    # Features (Korean)
    features_ko = []
    feature_list = soup.find(id="product-overview") or soup.find(class_="product-overview")
    if feature_list:
        for li in feature_list.find_all("li"):
            text = clean_text(li.get_text())
            if text:
                features_ko.append(text)
    if not features_ko and desc_el:
        for li in desc_el.find_all("li"):
            text = clean_text(li.get_text())
            if text and len(text) < 200:
                features_ko.append(text)
    if features_ko:
        info["features_ko"] = features_ko[:20]

    # ── 4. 한국어 복용법 ──
    use_el = soup.find(id="suggested-use") or soup.find(string=re.compile(r"권장 사용법|복용법|사용법|Suggested Use", re.IGNORECASE))
    if use_el:
        parent = use_el.parent if use_el.name is None else use_el
        next_content = parent.find_next_sibling() if parent else None
        if next_content:
            info["suggested_use_ko"] = clean_text(next_content.get_text())[:500]
        else:
            info["suggested_use_ko"] = clean_text(parent.get_text())[:500]

    # ── 5. 한국어 주의사항 ──
    warn_el = soup.find(id="warnings") or soup.find(string=re.compile(r"주의사항|경고|Warnings?", re.IGNORECASE))
    if warn_el:
        parent = warn_el.parent if warn_el.name is None else warn_el
        next_content = parent.find_next_sibling() if parent else None
        if next_content:
            info["warnings_ko"] = clean_text(next_content.get_text())[:500]
        else:
            info["warnings_ko"] = clean_text(parent.get_text())[:500]

    # ── 6. 한국어 보관방법 ──
    storage_el = soup.find(string=re.compile(r"보관|Store in|Storage", re.IGNORECASE))
    if storage_el:
        parent = storage_el.parent if storage_el.name is None else storage_el
        info["storage_info_ko"] = clean_text(parent.get_text())[:300]

    # ── 7. 한국어 성분 정보 ──
    ingredients_el = soup.find(id="product-ingredients") or soup.find(class_="ingredientsList")
    if not ingredients_el:
        ing_heading = soup.find(string=re.compile(r"기타 성분|성분|Other Ingredients|Ingredients", re.IGNORECASE))
        if ing_heading:
            parent = ing_heading.parent if ing_heading.name is None else ing_heading
            ingredients_el = parent.find_next_sibling() or parent
    if ingredients_el:
        info["ingredients_ko"] = clean_text(ingredients_el.get_text())[:2000]

    # Other ingredients (Korean)
    other_ing = soup.find(string=re.compile(r"기타 성분|Other Ingredients"))
    if other_ing:
        parent = other_ing.parent if other_ing.name is None else other_ing
        next_el = parent.find_next_sibling()
        if next_el:
            info["other_ingredients_ko"] = clean_text(next_el.get_text())[:500]

    # Allergen info (Korean)
    allergen = soup.find(string=re.compile(r"알레르기|알러지|함유|Allergen|Contains:", re.IGNORECASE))
    if allergen:
        parent = allergen.parent if allergen.name is None else allergen
        info["allergen_info_ko"] = clean_text(parent.get_text())[:300]

    # ── 8. 한국어 원화 가격 ──
    price_el = soup.find(id="price") or soup.find(class_="product-price")
    if price_el:
        price_text = price_el.get_text()
        krw_match = re.search(r'₩([\d,]+)', price_text)
        if krw_match:
            try:
                info["price_krw"] = int(krw_match.group(1).replace(",", ""))
            except ValueError:
                pass

    info["ko_scraped_at"] = datetime.utcnow().isoformat()
    return info


# ── Main Scraper Pipeline ───────────────────────────────

async def run_iherb_scrape(
    output_dir: str = "data",
    categories: list[str] = None,
    max_products_per_category: int = None,
    max_pages_per_category: int = 10,
    scrape_details: bool = True,
    scrape_korean: bool = True,
    progress_callback=None,
):
    """
    Full iHerb scraping pipeline.

    Args:
        output_dir: Directory to save results
        categories: List of category keys to scrape (None = all)
        max_products_per_category: Max products per category (None = no limit)
        max_pages_per_category: Max listing pages per category
        scrape_details: Whether to scrape individual product pages
        progress_callback: async function(processed, total, message) for progress updates
    """
    output = Path(output_dir)
    output.mkdir(exist_ok=True)

    cats = categories or list(SUPPLEMENT_CATEGORIES.keys())
    total_products = {}  # product_id -> basic info
    detailed_products = []

    async with httpx.AsyncClient(headers=HEADERS, timeout=30, follow_redirects=True) as client:

        # ═══ Phase 1: Collect product IDs from category listings ═══
        logger.info("═══ Phase 1: Collecting product IDs from category listings ═══")

        for cat_key in cats:
            if cat_key not in SUPPLEMENT_CATEGORIES:
                continue

            cat_info = SUPPLEMENT_CATEGORIES[cat_key]
            cat_path = cat_info["path"]
            cat_name = cat_info["name_ko"]

            logger.info(f"📂 Category: {cat_name} ({cat_key})")

            # Get page count
            page_count = await get_category_page_count(client, cat_path)
            page_count = min(page_count, max_pages_per_category)
            logger.info(f"  Pages to scrape: {page_count}")

            await asyncio.sleep(DELAY)

            cat_product_count = 0
            for page in range(1, page_count + 1):
                products = await scrape_category_listing(client, cat_path, page)

                for p in products:
                    pid = p.get("product_id")
                    if pid and pid not in total_products:
                        p["category"] = cat_name
                        p["category_key"] = cat_key
                        total_products[pid] = p
                        cat_product_count += 1

                if max_products_per_category and cat_product_count >= max_products_per_category:
                    break

                logger.info(f"  Page {page}/{page_count}: +{len(products)} products (total: {len(total_products)})")
                await asyncio.sleep(DELAY)

            if progress_callback:
                await progress_callback(len(total_products), 0, f"카테고리 수집: {cat_name} ({cat_product_count}개)")

        # Save product IDs
        with open(output / "iherb_product_ids.json", "w", encoding="utf-8") as f:
            json.dump(list(total_products.values()), f, ensure_ascii=False, indent=2)
        logger.info(f"Total unique products: {len(total_products)}")

        # ═══ Phase 2: Scrape product details ═══
        if scrape_details:
            logger.info("═══ Phase 2: Scraping product details ═══")
            product_list = list(total_products.values())
            total_count = len(product_list)

            for i, basic_info in enumerate(product_list):
                url = basic_info.get("url", "")
                pid = basic_info.get("product_id", "")

                if not url:
                    url = f"{BASE_URL}/pr/-/{pid}" if pid else ""

                if not url:
                    continue

                detail = await scrape_product_detail(client, url, pid)
                if detail:
                    # Merge with basic listing info
                    for key, val in basic_info.items():
                        if key not in detail or not detail[key]:
                            detail[key] = val
                    detailed_products.append(detail)

                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"  Progress: {i + 1}/{total_count} ({len(detailed_products)} successful)")

                    # Save intermediate results
                    with open(output / "iherb_products.json", "w", encoding="utf-8") as f:
                        json.dump(detailed_products, f, ensure_ascii=False, indent=2)

                if progress_callback:
                    await progress_callback(i + 1, total_count, f"상세 수집: {basic_info.get('name', pid)[:30]}")

                await asyncio.sleep(DELAY)

        # ═══ Phase 3: Scrape Korean data from kr.iherb.com ═══
        ko_count = 0
        target_list = detailed_products if detailed_products else list(total_products.values())

        if scrape_korean and target_list:
            logger.info("═══ Phase 3: Scraping Korean data from kr.iherb.com ═══")
            total_count = len(target_list)

            for i, product in enumerate(target_list):
                url = product.get("url", "")
                if not url:
                    continue

                ko_data = await scrape_korean_detail(client, url)
                if ko_data:
                    product.update(ko_data)
                    ko_count += 1

                if (i + 1) % 10 == 0:
                    logger.info(f"  Korean progress: {i + 1}/{total_count} ({ko_count} successful)")

                if progress_callback:
                    await progress_callback(i + 1, total_count, f"한국어 수집: {product.get('name_ko', product.get('name', ''))[:30]}")

                await asyncio.sleep(DELAY)

        # Save final results
        with open(output / "iherb_products.json", "w", encoding="utf-8") as f:
            json.dump(target_list, f, ensure_ascii=False, indent=2)

    # Summary
    result = {
        "total_product_ids": len(total_products),
        "detailed_products": len(detailed_products),
        "korean_enriched": ko_count,
        "categories_scraped": len(cats),
        "scraped_at": datetime.utcnow().isoformat(),
    }

    logger.info("═══ iHerb Scraping Complete ═══")
    logger.info(f"  Product IDs: {result['total_product_ids']}")
    logger.info(f"  Detailed: {result['detailed_products']}")
    logger.info(f"  Korean enriched: {result['korean_enriched']}")
    logger.info(f"  Categories: {result['categories_scraped']}")

    with open(output / "iherb_scrape_summary.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


# ── Quick Test ──────────────────────────────────────────

async def run_test_scrape(output_dir: str = "data"):
    """Quick test: scrape 5 products from 1 category."""
    return await run_iherb_scrape(
        output_dir=output_dir,
        categories=["vitamins"],
        max_products_per_category=5,
        max_pages_per_category=1,
        scrape_details=True,
    )


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "full":
        asyncio.run(run_iherb_scrape())
    elif mode == "quick":
        asyncio.run(run_iherb_scrape(max_products_per_category=20, max_pages_per_category=2))
    else:
        asyncio.run(run_test_scrape())
