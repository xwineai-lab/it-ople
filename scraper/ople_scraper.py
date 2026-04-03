"""
OPLE.com Product & Review Scraper
─────────────────────────────────
영카트5(그누보드) 기반 ople.com에서 상품/리뷰 데이터를 수집합니다.
서버 부담 최소화를 위해 delay를 두고 수집합니다.

HTML 구조 (2026-04 기준):
  상품명: h1.itemtitle / span.item_name_brand_deatil (브랜드)
         span.item_name_eng_deatil (한국어명) / span.item_name_kor_deatil (영문명)
  가격:  span.cust_amount_usd (달러) / span.cust_amount_won (원)
  리뷰:  #item_use 섹션 내 <tr> 4행 단위 (제목+작성자, 본문, 날짜, 구분선)
         리뷰 수: td 내 "리뷰 수 : N" 또는 탭 "상품후기 (N)"
  이미지: .leftArea img[src*='img.ople.com']
  목록:  .item_box 컨테이너, .item_review 내 사용후기(N)
  카테고리: a[href*='ca_id='] 링크 (list.php 페이지)
  페이지네이션: a[href*='page='] / 리뷰는 use_page= 파라미터
"""

import asyncio
import json
import re
import logging
from dataclasses import dataclass, asdict
from typing import Optional
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ople_scraper")

BASE_URL = "https://www.ople.com/mall5"
DELAY = 1.5  # seconds between requests (서버 부담 최소화)


# ── Data Models ──────────────────────────────────────────

@dataclass
class OpleProduct:
    it_id: str
    name_ko: str
    name_en: str
    brand: str
    price_usd: float
    price_krw: int
    category_id: str
    category_name: str
    review_count: int
    image_url: str
    description: str
    url: str

@dataclass
class OpleReview:
    product_id: str
    reviewer: str
    rating: int
    title: str
    text: str
    date: str


# ── Category Discovery ───────────────────────────────────

MAIN_CATEGORIES = {
    "10": "대상별",
    "11": "성분별",
    "12": "증상별",
    "13": "비타민&미네랄",
    "14": "오메가-3",
    "15": "유산균",
    "16": "허브/각종 추출물",
    "17": "항산화·면역력",
    "18": "동종요법",
    "19": "다이어트/스포츠",
    "20": "뷰티",
    "30": "출산/육아",
    "40": "식품",
    "50": "생활",
}

# Known top-level parent groups (s_id mapping)
PARENT_MAP = {
    "10": "건강식품", "11": "건강식품", "12": "건강식품",
    "13": "건강식품", "14": "건강식품", "15": "건강식품",
    "16": "건강식품", "17": "건강식품", "18": "건강식품",
    "19": "건강식품",
    "20": "뷰티용품",
    "30": "출산/육아",
    "40": "식품",
    "50": "생활",
}


async def discover_categories(client: httpx.AsyncClient) -> list[dict]:
    """Discover all sub-categories by crawling main category pages."""
    categories = []

    for ca_id, ca_name in MAIN_CATEGORIES.items():
        try:
            url = f"{BASE_URL}/shop/list.php?ca_id={ca_id}"
            resp = await client.get(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Find sub-category links
            cat_links = soup.find_all("a", href=re.compile(r"ca_id="))
            seen = set()
            for link in cat_links:
                href = link.get("href", "")
                match = re.search(r"ca_id=(\w+)", href)
                if match:
                    sub_ca_id = match.group(1)
                    if sub_ca_id not in seen and sub_ca_id != ca_id:
                        seen.add(sub_ca_id)
                        sub_name = link.get_text(strip=True)
                        if sub_name:
                            categories.append({
                                "ca_id": sub_ca_id,
                                "name": sub_name,
                                "parent_id": ca_id,
                                "parent_name": PARENT_MAP.get(ca_id, ca_name),
                            })

            # Also include the main category itself
            categories.append({
                "ca_id": ca_id,
                "name": ca_name,
                "parent_id": ca_id,
                "parent_name": PARENT_MAP.get(ca_id, ca_name),
            })

            logger.info(f"Category {ca_name} (ca_id={ca_id}): found {len(seen)} sub-categories")
            await asyncio.sleep(DELAY)

        except Exception as e:
            logger.error(f"Error discovering categories for ca_id={ca_id}: {e}")

    return categories


# ── Product List Crawling ────────────────────────────────

async def get_product_list_page(client: httpx.AsyncClient, ca_id: str, page: int) -> list[dict]:
    """Get product list from a category page. Uses .item_box containers."""
    url = f"{BASE_URL}/shop/list.php?ca_id={ca_id}&page={page}"

    try:
        resp = await client.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        products = []

        # OPLE uses .item_box containers for product cards
        item_boxes = soup.find_all(class_="item_box")

        seen_ids = set()
        for box in item_boxes:
            # Extract item ID from link
            link = box.find("a", href=re.compile(r"it_id="))
            if not link:
                continue

            href = link.get("href", "")
            match = re.search(r"it_id=(\d+)", href)
            if not match:
                continue

            it_id = match.group(1)
            if it_id in seen_ids:
                continue
            seen_ids.add(it_id)

            # Brand from list page
            brand_el = box.find(class_="item_name_brand")
            brand_text = brand_el.get_text(strip=True) if brand_el else ""

            # Korean name from list page
            ko_el = box.find(class_="item_name_eng")  # confusing: eng class = Korean name
            name_ko = ko_el.get_text(strip=True) if ko_el else ""

            # English name from list page
            en_el = box.find(class_="item_name_kor")  # confusing: kor class = English name
            name_en = en_el.get_text(strip=True) if en_el else ""

            # Review count from .item_review
            review_count = 0
            review_el = box.find(class_="item_review")
            if review_el:
                review_match = re.search(r"\((\d+)\)", review_el.get_text())
                if review_match:
                    review_count = int(review_match.group(1))

            # Image
            image_url = ""
            img_el = box.find("img", src=re.compile(r"img\.ople\.com"))
            if img_el:
                image_url = img_el.get("src", "")

            products.append({
                "it_id": it_id,
                "brand": brand_text,
                "name_ko": name_ko,
                "name_en": name_en,
                "review_count": review_count,
                "image_url": image_url,
                "ca_id": ca_id,
            })

        # Fallback: if no item_box found, try direct link extraction
        if not products:
            links = soup.find_all("a", href=re.compile(r"it_id="))
            for link in links:
                href = link.get("href", "")
                match = re.search(r"it_id=(\d+)", href)
                if match:
                    it_id = match.group(1)
                    if it_id not in seen_ids:
                        seen_ids.add(it_id)
                        products.append({
                            "it_id": it_id,
                            "brand": "",
                            "name_ko": link.get_text(strip=True)[:100],
                            "name_en": "",
                            "review_count": 0,
                            "image_url": "",
                            "ca_id": ca_id,
                        })

        return products

    except Exception as e:
        logger.error(f"Error getting product list ca_id={ca_id} page={page}: {e}")
        return []


async def get_total_pages(client: httpx.AsyncClient, ca_id: str) -> int:
    """Get total number of pages for a category."""
    url = f"{BASE_URL}/shop/list.php?ca_id={ca_id}"
    try:
        resp = await client.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find pagination links
        page_links = soup.find_all("a", href=re.compile(r"page="))
        max_page = 1
        for link in page_links:
            href = link.get("href", "")
            match = re.search(r"page=(\d+)", href)
            if match:
                max_page = max(max_page, int(match.group(1)))

        return max_page
    except:
        return 1


# ── Brand Extraction Helper ─────────────────────────────

def clean_brand(raw_brand: str) -> str:
    """Extract clean English brand name from '[Brand] 한국어명' format.

    Examples:
        '[Now Foods] 나우 푸드' → 'Now Foods'
        '[Biogaia] 바이오가이아' → 'Biogaia'
        '[The Honest Company] 더 어니스트 컴퍼니' → 'The Honest Company'
        'Solgar' → 'Solgar'
    """
    if not raw_brand:
        return ""

    # Try to extract brand from brackets [Brand]
    bracket_match = re.search(r"\[([^\]]+)\]", raw_brand)
    if bracket_match:
        return bracket_match.group(1).strip()

    # If no brackets, try to extract English portion
    # Split by whitespace and take non-Korean parts
    parts = raw_brand.strip().split()
    english_parts = []
    for part in parts:
        # Check if the part is primarily ASCII/English
        if re.match(r"^[A-Za-z0-9'&.\-]+$", part):
            english_parts.append(part)
        elif english_parts:
            break  # Stop at first non-English part after English text

    if english_parts:
        return " ".join(english_parts)

    return raw_brand.strip()


# ── Product Detail Parsing ───────────────────────────────

async def get_product_detail(client: httpx.AsyncClient, it_id: str) -> Optional[dict]:
    """Get detailed product info from item page.

    OPLE HTML structure:
    - h1.itemtitle: full title (brand + ko + en + description)
    - span.item_name_brand_deatil: '[Brand] 한국어 브랜드명'
    - span.item_name_eng_deatil: Korean product name (despite 'eng' in class)
    - span.item_name_kor_deatil: English product name (despite 'kor' in class)
    - span.item_name_etc_deatil: extra description
    - span.cust_amount_usd: USD price (just numbers)
    - span.cust_amount_won: KRW price (formatted with commas)
    - .item_tab_wrap: tab bar with '상품후기 (N)' for review count
    - #item_use td: '리뷰 수 : N' for review count
    - .leftArea img[src*='img.ople.com']: product image
    """
    url = f"{BASE_URL}/shop/item.php?it_id={it_id}"

    try:
        resp = await client.get(url)

        # Check for unavailable product
        if "구매할 수 없는 상품" in resp.text or len(resp.text) < 500:
            logger.debug(f"Product {it_id} is unavailable")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Brand
        brand_el = soup.find("span", class_="item_name_brand_deatil")
        raw_brand = brand_el.get_text(strip=True) if brand_el else ""
        brand = clean_brand(raw_brand)

        # Korean name
        name_ko = ""
        ko_el = soup.find("span", class_="item_name_eng_deatil")
        if ko_el:
            name_ko = ko_el.get_text(strip=True)

        # English name
        name_en = ""
        en_el = soup.find("span", class_="item_name_kor_deatil")
        if en_el:
            name_en = en_el.get_text(strip=True)

        # Extra description
        etc_el = soup.find("span", class_="item_name_etc_deatil")
        etc_text = etc_el.get_text(strip=True) if etc_el else ""

        # Price USD (from span.cust_amount_usd)
        price_usd = 0.0
        usd_el = soup.find("span", class_="cust_amount_usd")
        if usd_el:
            usd_text = usd_el.get_text(strip=True)
            if usd_text:
                try:
                    price_usd = float(usd_text.replace(",", ""))
                except ValueError:
                    pass

        # If cust_amount_usd is empty, try sell price (amount_usd)
        if price_usd == 0.0:
            sell_usd = soup.find("span", class_="amount_usd")
            if sell_usd:
                usd_text = sell_usd.get_text(strip=True)
                if usd_text:
                    try:
                        price_usd = float(usd_text.replace(",", ""))
                    except ValueError:
                        pass

        # Price KRW (from span.cust_amount_won)
        price_krw = 0
        krw_el = soup.find("span", class_="cust_amount_won")
        if krw_el:
            krw_text = krw_el.get_text(strip=True).replace(",", "")
            if krw_text:
                try:
                    price_krw = int(krw_text)
                except ValueError:
                    pass

        # Review count - try tab first, then review section
        review_count = 0
        tab = soup.find(class_="item_tab_wrap")
        if tab:
            tab_match = re.search(r"상품후기\s*\((\d+)\)", tab.get_text())
            if tab_match:
                review_count = int(tab_match.group(1))

        # Fallback: check review section td
        if review_count == 0:
            item_use = soup.find(id="item_use")
            if item_use:
                count_td = item_use.find("td", string=re.compile(r"리뷰 수"))
                if count_td:
                    count_match = re.search(r"(\d+)", count_td.get_text())
                    if count_match:
                        review_count = int(count_match.group(1))

        # Image (from .leftArea)
        image_url = ""
        left_area = soup.find(class_="leftArea")
        if left_area:
            img_el = left_area.find("img", src=re.compile(r"img\.ople\.com"))
            if img_el:
                image_url = img_el.get("src", "")

        # Fallback image: any img.ople.com image
        if not image_url:
            img_el = soup.find("img", src=re.compile(r"img\.ople\.com"))
            if img_el:
                image_url = img_el.get("src", "")

        # Description from .item_explanBOX
        description = ""
        desc_el = soup.find(class_="item_explanBOX")
        if desc_el:
            description = desc_el.get_text(strip=True)[:500]
        elif etc_text:
            description = etc_text

        return {
            "it_id": it_id,
            "name_ko": name_ko,
            "name_en": name_en,
            "brand": brand,
            "price_usd": price_usd,
            "price_krw": price_krw,
            "review_count": review_count,
            "image_url": image_url,
            "description": description,
            "url": url,
        }

    except Exception as e:
        logger.error(f"Error getting product detail it_id={it_id}: {e}")
        return None


# ── Review Scraping ──────────────────────────────────────

async def get_product_reviews(client: httpx.AsyncClient, it_id: str, max_pages: int = 5) -> list[dict]:
    """Get reviews for a specific product.

    Reviews are in the product page itself under #item_use section.
    Pagination uses use_page= parameter: item.php?it_id=XXX&use_page=N#use

    Review HTML structure (4 rows per review):
    - Row with 4 tds: td.lt = title, td[1] = reviewer (masked), td[2] = ?, td[3] = expand/collapse
    - Row.talkMore with 1 td: review body text
    - Row with 1 td: date (YYYY-MM-DD HH:MM:SS)
    - Row with 1 td: separator (empty)
    """
    reviews = []

    for page in range(1, max_pages + 1):
        try:
            # Reviews are on the product page with use_page parameter
            url = f"{BASE_URL}/shop/item.php?it_id={it_id}&use_page={page}"
            resp = await client.get(url)

            if "구매할 수 없는 상품" in resp.text or len(resp.text) < 500:
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find #item_use section
            item_use = soup.find(id="item_use")
            if not item_use:
                break

            # Check total review count
            count_td = item_use.find("td", string=re.compile(r"리뷰 수"))
            total_count = 0
            if count_td:
                count_match = re.search(r"(\d+)", count_td.get_text())
                if count_match:
                    total_count = int(count_match.group(1))

            if total_count == 0 and page == 1:
                break

            # Parse reviews from table rows
            rows = item_use.find_all("tr")
            page_reviews = []

            i = 0
            while i < len(rows):
                row = rows[i]
                tds = row.find_all("td")

                # Review header row: 4 tds (title, reviewer, ?, expand)
                if len(tds) == 4:
                    title_td = row.find("td", class_="lt")
                    title = title_td.get_text(strip=True) if title_td else tds[0].get_text(strip=True)
                    reviewer = tds[1].get_text(strip=True) if len(tds) > 1 else ""

                    # Skip the "리뷰 수" header row
                    if "리뷰 수" in title:
                        i += 1
                        continue

                    # Next row should be .talkMore (review body)
                    body = ""
                    if i + 1 < len(rows):
                        next_row = rows[i + 1]
                        if "talkMore" in " ".join(next_row.get("class", [])):
                            body = next_row.get_text(strip=True)

                    # Row after that should be the date
                    date = ""
                    if i + 2 < len(rows):
                        date_row = rows[i + 2]
                        date_text = date_row.get_text(strip=True)
                        date_match = re.search(r"\d{4}-\d{2}-\d{2}", date_text)
                        if date_match:
                            date = date_match.group(0)

                    if title or body:
                        page_reviews.append({
                            "product_id": it_id,
                            "reviewer": reviewer,
                            "rating": 5,  # OPLE doesn't show individual ratings
                            "title": title,
                            "text": body,
                            "date": date,
                        })

                    # Skip ahead by 4 rows (header, body, date, separator)
                    i += 4
                    continue

                i += 1

            reviews.extend(page_reviews)

            # Check if we've gotten all reviews
            if len(page_reviews) == 0:
                break
            if len(reviews) >= total_count:
                break

            logger.debug(f"Product {it_id} page {page}: {len(page_reviews)} reviews (total so far: {len(reviews)})")
            await asyncio.sleep(DELAY)

        except Exception as e:
            logger.error(f"Error getting reviews for it_id={it_id} page={page}: {e}")
            break

    return reviews


# ── Main Scraper ─────────────────────────────────────────

async def run_full_scrape(output_dir: str = "data", max_products: int = None):
    """
    Full scraping pipeline:
    1. Discover categories
    2. Collect product IDs from category pages
    3. Scrape product details
    4. Scrape reviews for top products
    """
    output = Path(output_dir)
    output.mkdir(exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    async with httpx.AsyncClient(headers=headers, timeout=30, follow_redirects=True) as client:

        # Step 1: Categories
        logger.info("═══ Step 1: Discovering categories ═══")
        categories = await discover_categories(client)
        with open(output / "categories.json", "w", encoding="utf-8") as f:
            json.dump(categories, f, ensure_ascii=False, indent=2)
        logger.info(f"Found {len(categories)} categories")

        # Step 2: Product IDs from list pages
        logger.info("═══ Step 2: Collecting product IDs ═══")
        all_product_ids = {}

        for cat in categories:
            ca_id = cat["ca_id"]
            total_pages = await get_total_pages(client, ca_id)
            await asyncio.sleep(DELAY)

            for page in range(1, min(total_pages + 1, 6)):  # Max 5 pages per category
                products = await get_product_list_page(client, ca_id, page)
                for p in products:
                    if p["it_id"] not in all_product_ids:
                        all_product_ids[p["it_id"]] = {
                            **p,
                            "category_name": cat["name"],
                            "parent_category": cat["parent_name"],
                        }
                await asyncio.sleep(DELAY)

            logger.info(f"Category {cat['name']}: collected products (page 1-{min(total_pages, 5)})")

            if max_products and len(all_product_ids) >= max_products:
                break

        logger.info(f"Total unique product IDs: {len(all_product_ids)}")

        # Step 3: Product details
        logger.info("═══ Step 3: Scraping product details ═══")
        detailed_products = []

        product_ids = list(all_product_ids.keys())
        if max_products:
            product_ids = product_ids[:max_products]

        for i, it_id in enumerate(product_ids):
            detail = await get_product_detail(client, it_id)
            if detail:
                # Merge with list data
                list_data = all_product_ids[it_id]
                detail["category_id"] = list_data.get("ca_id", "")
                detail["category_name"] = list_data.get("category_name", "")
                detail["parent_category"] = list_data.get("parent_category", "")

                # Use list page review count if detail didn't find it
                if detail["review_count"] == 0 and list_data.get("review_count", 0) > 0:
                    detail["review_count"] = list_data["review_count"]

                # Use list page image if detail didn't find it
                if not detail["image_url"] and list_data.get("image_url"):
                    detail["image_url"] = list_data["image_url"]

                detailed_products.append(detail)

            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{len(product_ids)} products scraped")
                # Save intermediate results
                with open(output / "products.json", "w", encoding="utf-8") as f:
                    json.dump(detailed_products, f, ensure_ascii=False, indent=2)

            await asyncio.sleep(DELAY)

        # Save final products
        with open(output / "products.json", "w", encoding="utf-8") as f:
            json.dump(detailed_products, f, ensure_ascii=False, indent=2)
        logger.info(f"Scraped {len(detailed_products)} product details")

        # Step 4: Reviews for top products (by review count)
        logger.info("═══ Step 4: Scraping reviews for top products ═══")
        top_products = sorted(detailed_products, key=lambda x: x.get("review_count", 0), reverse=True)[:200]

        all_reviews = []
        for i, prod in enumerate(top_products):
            if prod.get("review_count", 0) > 0:
                reviews = await get_product_reviews(client, prod["it_id"])
                all_reviews.extend(reviews)

                if (i + 1) % 20 == 0:
                    logger.info(f"Reviews progress: {i + 1}/{len(top_products)}, total reviews: {len(all_reviews)}")

        with open(output / "reviews.json", "w", encoding="utf-8") as f:
            json.dump(all_reviews, f, ensure_ascii=False, indent=2)
        logger.info(f"Scraped {len(all_reviews)} reviews from top {len(top_products)} products")

    # Summary
    logger.info("═══ Scraping Complete ═══")
    logger.info(f"Categories: {len(categories)}")
    logger.info(f"Products: {len(detailed_products)}")
    logger.info(f"Reviews: {len(all_reviews)}")

    return {
        "categories": len(categories),
        "products": len(detailed_products),
        "reviews": len(all_reviews),
    }


# ── Quick Test Mode ──────────────────────────────────────

async def run_test_scrape(output_dir: str = "data"):
    """Quick test: scrape just 10 products to verify the scraper works."""
    return await run_full_scrape(output_dir=output_dir, max_products=10)


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "full":
        asyncio.run(run_full_scrape())
    else:
        asyncio.run(run_test_scrape())
