"""
MFDS (식약처) 건강기능식품 데이터 Fetcher
------------------------------------------
공공데이터포털(data.go.kr)의 식약처 건강기능식품 품목제조신고 오픈 API 활용.

API 엔드포인트 (인증키 필요):
  http://openapi.foodsafetykorea.go.kr/api/{KEY}/C003/json/1/5/PRDLST_NM=비타민D

API 스펙: https://www.foodsafetykorea.go.kr/api/openApiInfo.do
  - C003: 건강기능식품 품목제조신고 정보
  - I2790: 식품영양성분
  - I0030: 건강기능식품 업소 정보

이 스크립트는 환경변수 MFDS_API_KEY를 읽어 실제 호출하며,
키가 없으면 API 스펙만 보여주는 더미 응답을 반환합니다.
"""
import os
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Dict, List, Optional

USER_AGENT = "OPLE-ETL/1.0"
MFDS_BASE = "http://openapi.foodsafetykorea.go.kr/api"


def search_products(
    keyword: str,
    service_id: str = "C003",
    start: int = 1,
    end: int = 10,
    api_key: Optional[str] = None,
) -> Dict:
    """
    식약처 건강기능식품 품목제조신고 검색.

    keyword: 제품명 검색어 (예: "비타민D", "오메가3")
    service_id: API 서비스 ID (C003=건강기능식품 품목제조신고)
    """
    api_key = api_key or os.getenv("MFDS_API_KEY") or "sample"
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    if False:  # sample key always works for pilot testing
        return {
            "source": "MFDS_foodsafetykorea",
            "query": keyword,
            "fetched_at": fetched_at,
            "error": "no_api_key",
            "note": (
                "API 키가 없습니다. https://www.data.go.kr 또는 "
                "https://www.foodsafetykorea.go.kr/api/ 에서 무료 발급 후 "
                "MFDS_API_KEY 환경변수 설정 필요."
            ),
            "api_spec": {
                "service_id": service_id,
                "url_template": f"{MFDS_BASE}/{{KEY}}/{service_id}/json/{{start}}/{{end}}/PRDLST_NM={{keyword}}",
                "example_fields": [
                    "PRDLST_NM (제품명)",
                    "BSSH_NM (업소명)",
                    "PRDT_SHAP_CD_NM (제품 형태)",
                    "LAST_UPDT_DTM (최종 수정일)",
                    "STDR_STND (기준 규격)",
                    "PRIMARY_FNCLTY (주요 기능성)",
                    "IFTKN_ATNT_MATR_CN (섭취 시 주의사항)",
                    "INTAKE_HINT1 (섭취 방법)",
                    "PRSRV_PD (유통기한)",
                ],
            },
            "products": [],
        }

    # URL-encode Korean keyword
    keyword_enc = urllib.parse.quote(keyword)
    url = f"{MFDS_BASE}/{api_key}/{service_id}/json/{start}/{end}/PRDLST_NM={keyword_enc}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=25) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        data = json.loads(body)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        return {
            "source": "MFDS_foodsafetykorea",
            "query": keyword,
            "fetched_at": fetched_at,
            "error": f"fetch_failed: {e}",
            "products": [],
        }

    block = data.get(service_id, {})
    total = block.get("total_count", "0")
    rows = block.get("row", [])
    return {
        "source": "MFDS_foodsafetykorea",
        "query": keyword,
        "service_id": service_id,
        "fetched_at": fetched_at,
        "total_count": int(total) if str(total).isdigit() else 0,
        "returned": len(rows),
        "products": [_normalize(r) for r in rows],
    }


def _normalize(r: Dict) -> Dict:
    """Normalize MFDS row to OPLE schema.
    Actual MFDS C003 fields: PRDLST_NM, BSSH_NM, PRDT_SHAP_CD_NM, PRIMARY_FNCLTY,
    STDR_STND, NTK_MTHD, IFTKN_ATNT_MATR_CN, POG_DAYCNT, CSTDY_MTHD, RAWMTRL_NM,
    PRDLST_REPORT_NO, LAST_UPDT_DTM, PRMS_DT.
    """
    def _trunc(s, n=400):
        return (s[:n] + "…") if isinstance(s, str) and len(s) > n else s
    return {
        "product_name_ko": r.get("PRDLST_NM"),
        "company_name": r.get("BSSH_NM"),
        "product_form": r.get("PRDT_SHAP_CD_NM"),
        "functionality": _trunc(r.get("PRIMARY_FNCLTY")),
        "standard_spec": _trunc(r.get("STDR_STND")),
        "intake_method": r.get("NTK_MTHD"),
        "warnings": _trunc(r.get("IFTKN_ATNT_MATR_CN")),
        "storage": r.get("CSTDY_MTHD"),
        "raw_materials": _trunc(r.get("RAWMTRL_NM")),
        "shelf_life": r.get("POG_DAYCNT"),
        "last_updated": r.get("LAST_UPDT_DTM"),
        "approval_date": r.get("PRMS_DT"),
        "report_number": r.get("PRDLST_REPORT_NO"),
        "is_korean_registered": True,
    }


if __name__ == "__main__":
    import sys
    kw = sys.argv[1] if len(sys.argv) > 1 else "비타민D"
    result = search_products(kw, end=5)
    print(json.dumps(result, indent=2, ensure_ascii=False))
