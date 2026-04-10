"""
IT.OPLE — USD/KRW FX Rate Service
==================================
지속적으로 업데이트되는 USD→KRW 환율을 공급하는 얇은 서비스 레이어.

설계 원칙:
  - Google Finance 페이지는 JavaScript 렌더링이라서 서버-사이드 HTTP로는
    값을 뽑을 수 없음 → 여러 public provider 를 fallback chain 으로 사용.
  - In-memory TTL 캐시 (기본 30분) 로 외부 호출 횟수 제한.
  - 동기화/매핑 경로에서 환율 실패가 전체 흐름을 깨지 않도록,
    최종적으로 하드코딩된 안전값(`FALLBACK_RATE`) 으로 떨어짐.
  - `get_usd_krw_rate()` 는 float 만 반환 (하위 호환).
  - `get_usd_krw_info()` 는 {rate, source, fetched_at, age_seconds, cached}
    메타데이터까지 반환 → API 엔드포인트 /api/fx/usd-krw 에서 사용.

Providers (선언 순서대로 시도):
  1. open.er-api.com — 무료, 키 없음, 일일 업데이트
  2. api.frankfurter.app — ECB 데이터, 키 없음, 유럽 시장시간 업데이트
  (원하면 여기에 Google Finance 전용 scraper 나 유료 provider 를 추가 가능)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, asdict
from typing import Optional, Callable

import httpx

log = logging.getLogger(__name__)

# ── 설정 ───────────────────────────────────────────────────
FALLBACK_RATE = 1350.0
CACHE_TTL_SECONDS = int(os.getenv("FX_CACHE_TTL", "1800"))  # 30 min
REQUEST_TIMEOUT = 6.0

# ── 캐시 상태 ──────────────────────────────────────────────
_lock = threading.Lock()
_cached_rate: Optional[float] = None
_cached_source: Optional[str] = None
_cached_fetched_at: Optional[float] = None  # unix epoch seconds


@dataclass
class FxInfo:
    rate: float
    source: str
    fetched_at: Optional[float]
    age_seconds: Optional[float]
    cached: bool
    ttl_seconds: int

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


# ── Provider 구현 ──────────────────────────────────────────

def _provider_open_er_api(client: httpx.Client) -> Optional[float]:
    """open.er-api.com — 무료, 키 없음, 일일 업데이트."""
    try:
        r = client.get("https://open.er-api.com/v6/latest/USD", timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("result") != "success":
            return None
        rate = data.get("rates", {}).get("KRW")
        return float(rate) if rate else None
    except Exception as e:
        log.warning("open.er-api.com fetch failed: %s", e)
        return None


def _provider_frankfurter(client: httpx.Client) -> Optional[float]:
    """api.frankfurter.app — ECB 데이터."""
    try:
        r = client.get(
            "https://api.frankfurter.app/latest",
            params={"from": "USD", "to": "KRW"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        rate = data.get("rates", {}).get("KRW")
        return float(rate) if rate else None
    except Exception as e:
        log.warning("frankfurter fetch failed: %s", e)
        return None


# NOTE:
#   Google Finance (https://www.google.com/finance/beta/quote/USD-KRW) 는 JS
#   렌더링 전용이므로 일반 HTTP 요청으로는 값을 얻을 수 없음. 필요시 별도
#   headless-browser 서비스에서 값을 받아 `set_external_rate(...)` 로 주입 가능.
PROVIDERS: list[tuple[str, Callable[[httpx.Client], Optional[float]]]] = [
    ("open.er-api.com", _provider_open_er_api),
    ("api.frankfurter.app", _provider_frankfurter),
]


# ── 공개 API ───────────────────────────────────────────────

def _is_cache_valid() -> bool:
    if _cached_rate is None or _cached_fetched_at is None:
        return False
    return (time.time() - _cached_fetched_at) < CACHE_TTL_SECONDS


def _fetch_fresh() -> tuple[float, str]:
    """Try providers in order. Returns (rate, source_label)."""
    with httpx.Client() as client:
        for name, fn in PROVIDERS:
            rate = fn(client)
            if rate and rate > 0:
                log.info("FX rate fetched from %s: 1 USD = %s KRW", name, rate)
                return rate, name
    log.warning("All FX providers failed, using hardcoded fallback %.2f", FALLBACK_RATE)
    return FALLBACK_RATE, "fallback-hardcoded"


def get_usd_krw_info(force_refresh: bool = False) -> FxInfo:
    """Return current USD→KRW rate plus metadata.

    Thread-safe. Uses in-memory cache with TTL=CACHE_TTL_SECONDS.
    """
    global _cached_rate, _cached_source, _cached_fetched_at

    with _lock:
        if not force_refresh and _is_cache_valid():
            assert _cached_rate is not None
            assert _cached_source is not None
            assert _cached_fetched_at is not None
            age = time.time() - _cached_fetched_at
            return FxInfo(
                rate=_cached_rate,
                source=_cached_source,
                fetched_at=_cached_fetched_at,
                age_seconds=round(age, 1),
                cached=True,
                ttl_seconds=CACHE_TTL_SECONDS,
            )

        rate, source = _fetch_fresh()
        _cached_rate = rate
        _cached_source = source
        _cached_fetched_at = time.time()

        return FxInfo(
            rate=rate,
            source=source,
            fetched_at=_cached_fetched_at,
            age_seconds=0.0,
            cached=False,
            ttl_seconds=CACHE_TTL_SECONDS,
        )


def get_usd_krw_rate(force_refresh: bool = False) -> float:
    """Convenience: return just the numeric rate (used by metafield mapper)."""
    try:
        return get_usd_krw_info(force_refresh=force_refresh).rate
    except Exception as e:
        log.error("FX service crashed, returning fallback: %s", e)
        return FALLBACK_RATE


def set_external_rate(rate: float, source: str = "manual-override") -> FxInfo:
    """Inject an externally-sourced rate (e.g. from a headless browser
    scraping Google Finance). Overwrites the cache so subsequent calls
    return this value until the TTL expires.
    """
    global _cached_rate, _cached_source, _cached_fetched_at
    if rate <= 0:
        raise ValueError(f"Invalid FX rate: {rate}")
    with _lock:
        _cached_rate = float(rate)
        _cached_source = source
        _cached_fetched_at = time.time()
        log.info("FX rate overridden externally: %s KRW (source=%s)", rate, source)
        return FxInfo(
            rate=_cached_rate,
            source=_cached_source,
            fetched_at=_cached_fetched_at,
            age_seconds=0.0,
            cached=False,
            ttl_seconds=CACHE_TTL_SECONDS,
        )


def clear_cache():
    """Drop the cached rate so the next call re-fetches."""
    global _cached_rate, _cached_source, _cached_fetched_at
    with _lock:
        _cached_rate = None
        _cached_source = None
        _cached_fetched_at = None
