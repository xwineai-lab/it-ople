# OPLE Pilot ETL — 3-Source Ingredient Enrichment

OPLE.COM 상품 DB 보강을 위한 파일럿 ETL. 성분(비타민/미네랄) 단위로 3개 공개 소스에서 정보를 수집하여 통합 마스터 레코드를 생성합니다.

## 소스

| Source | Endpoint | Auth | Status |
|---|---|---|---|
| **NIH ODS** | `https://ods.od.nih.gov/factsheets/*-HealthProfessional/` | 없음 (public) | ✅ 안정 |
| **Open Food Facts** | `https://world.openfoodfacts.org/api/v2/search` (fallback us/fr) | 없음 | ⚠️ 간헐적 503 |
| **MFDS (식약처)** | `http://openapi.foodsafetykorea.go.kr/api/{KEY}/C003/json/*` | sample 키 동작 | ✅ 안정 |

## 사용법

```bash
# 기본: 비타민 D
python3 run_pilot.py

# 다른 성분
python3 run_pilot.py vitamin_c
python3 run_pilot.py omega_3
python3 run_pilot.py magnesium

# 한국어 키워드 오버라이드
python3 run_pilot.py vitamin_d "비타민 D3"
```

결과는 `/sessions/elegant-zealous-rubin/mnt/outputs/pilot_etl/{ingredient}_unified.json`에 저장됩니다.

## 통합 스키마

```json
{
  "ingredient_key": "vitamin_d",
  "name_i18n": { "ko": "비타민D", "en": "Vitamin D" },
  "dosage": { "rda": "...", "upper_limit": "...", "reference_authority": "NIH ODS" },
  "safety_notes": { "description_en": "...", "deficiency": "...", "drug_interactions": "..." },
  "allergens": [...],
  "kr_regulatory": { "is_registered_in_kr": true, "registered_product_count": 5, ... },
  "korean_registered_products": [...],
  "market_samples": [...],
  "sources_used": [...],
  "coverage_score": { "score": "3/3", "detail": {...} }
}
```

## 지원 성분 (11종)

vitamin_d, vitamin_c, vitamin_b12, vitamin_a, vitamin_e, omega_3, calcium, iron, magnesium, zinc, probiotics

## 프로덕션 전환 체크리스트

- [ ] MFDS: data.go.kr에서 실제 API 키 발급 → `MFDS_API_KEY` 환경변수 설정
- [ ] 배치 스케줄링: Celery / cron으로 주 1회 자동 갱신
- [ ] DB 저장: `ingredient` 테이블에 upsert (catalog_v2 스키마)
- [ ] 번역 보강: NIH 영문 텍스트 → 한/일/중 번역 파이프라인 연결
- [ ] 알레르기 표준화: OFF allergens_tags를 식약처 표준 알레르기 목록으로 매핑
- [ ] 모니터링: fetch 실패 시 Slack 알림
