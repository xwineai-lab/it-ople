"""
Shopify Metafield Definition Creator for IT.OPLE
=================================================
22개 메타필드 Definition을 Shopify Admin API (GraphQL)로 생성합니다.

사용법:
  1) 환경변수 방식:
     SHOPIFY_STORE=ople-7502.myshopify.com SHOPIFY_ACCESS_TOKEN=shpat_xxx python shopify_metafields.py

  2) FastAPI 엔드포인트에서 호출 (main.py에서 import)

  3) SQLite 세션에서 자동으로 access_token 읽기:
     SHOPIFY_STORE=ople-7502.myshopify.com python shopify_metafields.py --from-session
"""

import os
import sys
import json
import sqlite3
import argparse
import httpx
from typing import Optional

# ── Shopify Store Config ─────────────────────────────────
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE", "ople-7502.myshopify.com")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION = "2025-01"

GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# ── 22 Metafield Definitions ────────────────────────────
# namespace: "custom" (Shopify 표준 네임스페이스)
# ownerType: PRODUCT

METAFIELD_DEFINITIONS = [
    # ━━━ WMS 핵심 정보 (9개) ━━━
    {
        "name": "OPLE SKU",
        "namespace": "custom",
        "key": "ople_sku",
        "description": "OPLE WMS 고유 SKU 코드",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "UPC 바코드",
        "namespace": "custom",
        "key": "upc",
        "description": "UPC/EAN 바코드 번호",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "브랜드 코드",
        "namespace": "custom",
        "key": "brand_code",
        "description": "WMS 브랜드 식별 코드",
        "type": "single_line_text_field",
        "pin": False,
    },
    {
        "name": "브랜드명 (한국어)",
        "namespace": "custom",
        "key": "brand_name_ko",
        "description": "한국어 브랜드명 (예: 솔가, 나우푸드)",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "상품명 (한국어)",
        "namespace": "custom",
        "key": "name_ko",
        "description": "한국어 상품명",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "상품명 (영문)",
        "namespace": "custom",
        "key": "name_en",
        "description": "영문 상품명 (원어 표기)",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "상품 설명 HTML",
        "namespace": "custom",
        "key": "description_html",
        "description": "HTML 형식 상세 설명",
        "type": "multi_line_text_field",
        "pin": False,
    },
    {
        "name": "OPLE 상품 ID",
        "namespace": "custom",
        "key": "ople_id",
        "description": "IT.OPLE 시스템 상품 고유 ID (it_id)",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "OPLE 상품 URL",
        "namespace": "custom",
        "key": "ople_url",
        "description": "IT.OPLE 대시보드 상품 페이지 URL",
        "type": "url",
        "pin": False,
    },

    # ━━━ 가격 & 재고 정보 (5개) ━━━
    {
        "name": "원가 (USD)",
        "namespace": "custom",
        "key": "price_usd",
        "description": "USD 기준 매입/원가",
        "type": "number_decimal",
        "pin": True,
    },
    {
        "name": "원가 (KRW)",
        "namespace": "custom",
        "key": "price_krw",
        "description": "KRW 기준 매입/원가",
        "type": "number_integer",
        "pin": True,
    },
    {
        "name": "재고 수량",
        "namespace": "custom",
        "key": "stock_qty",
        "description": "WMS 실시간 재고 수량",
        "type": "number_integer",
        "pin": True,
    },
    {
        "name": "박스 수량",
        "namespace": "custom",
        "key": "box_count",
        "description": "WMS 박스 단위 수량",
        "type": "number_integer",
        "pin": False,
    },
    {
        "name": "자식 상품 수",
        "namespace": "custom",
        "key": "child_count",
        "description": "번들/세트 구성 자식 상품 수",
        "type": "number_integer",
        "pin": False,
    },

    # ━━━ 상태 & 분류 (5개) ━━━
    {
        "name": "판매 상태",
        "namespace": "custom",
        "key": "sales_status",
        "description": "판매 상태 (판매중/일시중단/단종 등)",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "예약판매 여부",
        "namespace": "custom",
        "key": "reserve_flag",
        "description": "예약판매 플래그 (Y/N)",
        "type": "boolean",
        "pin": False,
    },
    {
        "name": "카테고리명",
        "namespace": "custom",
        "key": "category_name",
        "description": "OPLE 상품 카테고리명",
        "type": "single_line_text_field",
        "pin": True,
    },
    {
        "name": "상위 카테고리",
        "namespace": "custom",
        "key": "parent_category",
        "description": "OPLE 상위 카테고리명",
        "type": "single_line_text_field",
        "pin": False,
    },
    {
        "name": "카테고리 ID",
        "namespace": "custom",
        "key": "category_id",
        "description": "OPLE 카테고리 코드",
        "type": "single_line_text_field",
        "pin": False,
    },

    # ━━━ 구성품 & 이미지 & 매핑 (3개) ━━━
    {
        "name": "자식 상품 목록",
        "namespace": "custom",
        "key": "child_products",
        "description": "번들/세트 구성 상품 JSON (SKU, 수량 등)",
        "type": "json",
        "pin": False,
    },
    {
        "name": "원본 이미지 URL",
        "namespace": "custom",
        "key": "image_url",
        "description": "OPLE 원본 상품 이미지 URL",
        "type": "url",
        "pin": False,
    },
    {
        "name": "OPLE 매핑 완료",
        "namespace": "custom",
        "key": "ople_mapped",
        "description": "OPLE 데이터 매핑 완료 여부",
        "type": "boolean",
        "pin": False,
    },
    # ━━━ 상품 상세 섹션 (3개) ━━━
    {
        "name": "섭취방법",
        "namespace": "custom",
        "key": "suggested_use",
        "description": "복용법/섭취방법 (WMS 라벨에서 파싱)",
        "type": "multi_line_text_field",
        "pin": False,
    },
    {
        "name": "기타 성분",
        "namespace": "custom",
        "key": "other_ingredients",
        "description": "기타 성분 정보 (WMS 라벨에서 파싱)",
        "type": "multi_line_text_field",
        "pin": False,
    },
    {
        "name": "주의사항",
        "namespace": "custom",
        "key": "warnings",
        "description": "주의사항/경고 (WMS 라벨에서 파싱)",
        "type": "multi_line_text_field",
        "pin": False,
    },
]


# ── GraphQL Mutations ────────────────────────────────────

CREATE_DEFINITION_MUTATION = """
mutation CreateMetafieldDefinition($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition {
      id
      name
      namespace
      key
      type {
        name
      }
      pinnedPosition
    }
    userErrors {
      field
      message
      code
    }
  }
}
"""

DELETE_DEFINITION_MUTATION = """
mutation DeleteMetafieldDefinition($id: ID!, $deleteAllAssociatedMetafields: Boolean!) {
  metafieldDefinitionDelete(id: $id, deleteAllAssociatedMetafields: $deleteAllAssociatedMetafields) {
    deletedDefinitionId
    userErrors {
      field
      message
      code
    }
  }
}
"""

LIST_DEFINITIONS_QUERY = """
query ListMetafieldDefinitions($ownerType: MetafieldOwnerType!) {
  metafieldDefinitions(first: 50, ownerType: $ownerType, namespace: "custom") {
    edges {
      node {
        id
        name
        namespace
        key
        type {
          name
        }
        pinnedPosition
      }
    }
  }
}
"""


# ── Helper Functions ─────────────────────────────────────

def get_access_token_from_session(db_path: str = None) -> Optional[str]:
    """Prisma SQLite 세션 DB에서 access_token을 읽어옵니다."""
    if db_path is None:
        # Docker 내부 경로 또는 로컬 경로
        possible_paths = [
            "/app/shopify-app/prisma/dev.sqlite",
            os.path.join(os.path.dirname(__file__), "..", "shopify-app", "prisma", "dev.sqlite"),
        ]
        for p in possible_paths:
            if os.path.exists(p):
                db_path = p
                break

    if not db_path or not os.path.exists(db_path):
        print(f"❌ Session DB not found. Set SHOPIFY_ACCESS_TOKEN env var instead.")
        return None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT accessToken, shop FROM Session WHERE shop = ? LIMIT 1",
            (SHOPIFY_STORE,)
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            print(f"✅ Found access token for shop: {row[1]}")
            return row[0]
        else:
            print(f"❌ No session found for shop: {SHOPIFY_STORE}")
            return None
    except Exception as e:
        print(f"❌ Error reading session DB: {e}")
        return None


def graphql_request(query: str, variables: dict, access_token: str) -> dict:
    """Shopify Admin GraphQL API 요청"""
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }
    response = httpx.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def list_existing_definitions(access_token: str) -> list:
    """기존 custom 네임스페이스 메타필드 Definition 목록 조회"""
    result = graphql_request(
        LIST_DEFINITIONS_QUERY,
        {"ownerType": "PRODUCT"},
        access_token,
    )
    definitions = []
    edges = result.get("data", {}).get("metafieldDefinitions", {}).get("edges", [])
    for edge in edges:
        node = edge["node"]
        definitions.append({
            "id": node["id"],
            "name": node["name"],
            "namespace": node["namespace"],
            "key": node["key"],
            "type": node["type"]["name"],
            "pinned": node.get("pinnedPosition") is not None,
        })
    return definitions


def create_metafield_definition(defn: dict, access_token: str) -> dict:
    """단일 메타필드 Definition 생성"""
    variables = {
        "definition": {
            "name": defn["name"],
            "namespace": defn["namespace"],
            "key": defn["key"],
            "description": defn["description"],
            "type": defn["type"],
            "ownerType": "PRODUCT",
            "pin": defn.get("pin", False),
        }
    }

    result = graphql_request(CREATE_DEFINITION_MUTATION, variables, access_token)
    data = result.get("data", {}).get("metafieldDefinitionCreate", {})
    errors = data.get("userErrors", [])

    if errors:
        return {"success": False, "key": defn["key"], "errors": errors}

    created = data.get("createdDefinition", {})
    return {
        "success": True,
        "key": defn["key"],
        "id": created.get("id"),
        "name": created.get("name"),
        "type": created.get("type", {}).get("name"),
    }


def delete_metafield_definition(definition_id: str, access_token: str, delete_values: bool = True) -> dict:
    """메타필드 Definition 삭제"""
    variables = {
        "id": definition_id,
        "deleteAllAssociatedMetafields": delete_values,
    }
    result = graphql_request(DELETE_DEFINITION_MUTATION, variables, access_token)
    data = result.get("data", {}).get("metafieldDefinitionDelete", {})
    errors = data.get("userErrors", [])

    if errors:
        return {"success": False, "errors": errors}
    return {"success": True, "deletedId": data.get("deletedDefinitionId")}


def create_all_definitions(access_token: str, skip_existing: bool = True) -> dict:
    """22개 메타필드 Definition 전체 생성"""
    # 기존 Definition 조회
    existing = list_existing_definitions(access_token)
    existing_keys = {d["key"] for d in existing}
    print(f"\n📋 기존 custom 메타필드: {len(existing)}개")
    for d in existing:
        print(f"   - {d['namespace']}.{d['key']} ({d['type']})")

    results = {"created": [], "skipped": [], "failed": []}

    for defn in METAFIELD_DEFINITIONS:
        key = defn["key"]

        if skip_existing and key in existing_keys:
            print(f"⏭️  SKIP: custom.{key} (이미 존재)")
            results["skipped"].append(key)
            continue

        print(f"🔨 Creating: custom.{key} ...", end=" ")
        result = create_metafield_definition(defn, access_token)

        if result["success"]:
            print(f"✅ {result['name']} ({result['type']})")
            results["created"].append(result)
        else:
            error_msg = "; ".join(e["message"] for e in result["errors"])
            print(f"❌ {error_msg}")
            results["failed"].append({"key": key, "error": error_msg})

    # Summary
    print(f"\n{'='*50}")
    print(f"📊 결과 요약:")
    print(f"   ✅ 생성: {len(results['created'])}개")
    print(f"   ⏭️  스킵: {len(results['skipped'])}개")
    print(f"   ❌ 실패: {len(results['failed'])}개")
    print(f"   📋 전체: {len(METAFIELD_DEFINITIONS)}개")
    print(f"{'='*50}")

    return results


# ── FastAPI Integration ──────────────────────────────────
# main.py에서 import하여 사용:
#   from scripts.shopify_metafields import create_all_definitions, list_existing_definitions

async def async_create_all_definitions(access_token: str, skip_existing: bool = True) -> dict:
    """비동기 버전 (FastAPI 엔드포인트용)"""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, create_all_definitions, access_token, skip_existing)


# ── CLI Entry Point ──────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Shopify Metafield Definition Manager")
    parser.add_argument("--from-session", action="store_true",
                        help="Prisma SQLite 세션에서 access_token 읽기")
    parser.add_argument("--list", action="store_true",
                        help="기존 메타필드 Definition 목록만 조회")
    parser.add_argument("--delete-all", action="store_true",
                        help="custom 네임스페이스 모든 Definition 삭제")
    parser.add_argument("--force", action="store_true",
                        help="이미 존재하는 Definition도 재생성 (기존 삭제 후)")
    parser.add_argument("--db-path", type=str, default=None,
                        help="Prisma SQLite DB 경로 직접 지정")
    args = parser.parse_args()

    # Access token 확보
    access_token = SHOPIFY_ACCESS_TOKEN

    if not access_token or args.from_session:
        access_token = get_access_token_from_session(args.db_path)

    if not access_token:
        print("❌ Access token이 필요합니다.")
        print("   SHOPIFY_ACCESS_TOKEN 환경변수를 설정하거나 --from-session 옵션을 사용하세요.")
        sys.exit(1)

    print(f"🏪 Store: {SHOPIFY_STORE}")
    print(f"🔑 Token: {access_token[:8]}...{access_token[-4:]}")
    print(f"📡 API: {SHOPIFY_API_VERSION}")

    if args.list:
        definitions = list_existing_definitions(access_token)
        print(f"\n📋 custom 네임스페이스 메타필드 Definition ({len(definitions)}개):")
        for d in definitions:
            pin = "📌" if d["pinned"] else "  "
            print(f"  {pin} {d['namespace']}.{d['key']} — {d['name']} ({d['type']})")
            print(f"       ID: {d['id']}")
        return

    if args.delete_all:
        confirm = input("⚠️  custom 네임스페이스의 모든 메타필드 Definition을 삭제합니다. 계속? (y/N): ")
        if confirm.lower() != "y":
            print("취소됨.")
            return
        definitions = list_existing_definitions(access_token)
        for d in definitions:
            print(f"🗑️  Deleting: {d['key']} ...", end=" ")
            result = delete_metafield_definition(d["id"], access_token, delete_values=True)
            print("✅" if result["success"] else f"❌ {result.get('errors')}")
        return

    # 메타필드 Definition 전체 생성
    print(f"\n🚀 22개 메타필드 Definition 생성 시작...")
    create_all_definitions(access_token, skip_existing=not args.force)


if __name__ == "__main__":
    main()
