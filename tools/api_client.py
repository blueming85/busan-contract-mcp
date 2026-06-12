"""나라장터 API 공통 HTTP 클라이언트 (httpx 비동기, 전역 동시성 제한)"""
import asyncio
import calendar
import urllib.parse
from datetime import datetime

import httpx

import config


class ApiKeyError(RuntimeError):
    """서비스 키 미설정·인증·트래픽 한도 오류 — 월별 루프에서 삼키지 말고 전파할 것"""


# data.go.kr 인증/등록/한도 계열 오류 코드
_AUTH_ERROR_CODES = {"20", "22", "30", "31", "32", "33"}

_MAX_CONCURRENCY = 20

_client: httpx.AsyncClient | None = None
_sem: asyncio.Semaphore | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=60.0,
            limits=httpx.Limits(
                max_connections=_MAX_CONCURRENCY,
                max_keepalive_connections=_MAX_CONCURRENCY,
            ),
            headers={"User-Agent": "BusanContractMCP/1.0"},
        )
    return _client


def _get_sem() -> asyncio.Semaphore:
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(_MAX_CONCURRENCY)
    return _sem


def _raise_for_api_error(code: str, msg: str):
    if code in _AUTH_ERROR_CODES or "SERVICE KEY" in msg.upper():
        raise ApiKeyError(
            f"API 인증 오류 [{code}]: {msg}\n"
            "→ .env의 NARA_SERVICE_KEY가 올바른지, 해당 서비스 활용신청이 승인됐는지 확인하세요."
        )
    raise ValueError(f"API 오류 [{code}]: {msg}")


async def fetch(endpoint: str, operation: str, params: dict) -> dict:
    """나라장터 REST API 비동기 호출"""
    if not config.SERVICE_KEY:
        raise ApiKeyError(
            "NARA_SERVICE_KEY가 설정되지 않았습니다. "
            ".env 파일에 공공데이터포털 서비스 키를 설정해주세요."
        )

    base_params = {
        "ServiceKey": config.SERVICE_KEY,
        "type": "json",
        "numOfRows": params.pop("numOfRows", 100),
        "pageNo": params.pop("pageNo", 1),
    }
    base_params.update(params)

    # 서비스 키 이중 인코딩 방지를 위해 쿼리 문자열을 직접 구성
    query = urllib.parse.urlencode(base_params, quote_via=urllib.parse.quote)
    url = f"{endpoint}/{operation}?{query}"

    async with _get_sem():
        resp = await _get_client().get(url)
    data = resp.json()

    # 일부 서비스는 비표준 오류 형식 반환: {"nkoneps.com.response.ResponseError": {...}}
    err_key = next((k for k in data if "ResponseError" in k), None)
    if err_key:
        hdr = data[err_key].get("header", {})
        _raise_for_api_error(hdr.get("resultCode", "?"), hdr.get("resultMsg", "알 수 없는 오류"))

    response = data.get("response", {})
    header = response.get("header", {})
    code = header.get("resultCode", "00")
    if code not in ("00", "0000"):
        _raise_for_api_error(code, header.get("resultMsg", "알 수 없는 오류"))

    body = response.get("body", {})
    items = body.get("items", [])
    total_count = body.get("totalCount", 0)

    if isinstance(items, dict):
        items = [items] if items else []

    return {"totalCount": total_count, "items": items}


def month_ranges(months_back: int, cap: int = 48) -> list[tuple[int, int, int]]:
    """이번 달부터 과거로 months_back개월의 (연, 월, 말일) 목록 (이번 달 포함)"""
    now = datetime.now()
    base = now.year * 12 + (now.month - 1)
    ranges = []
    for m in range(min(months_back, cap)):
        y, mo = divmod(base - m, 12)
        mo += 1
        ranges.append((y, mo, calendar.monthrange(y, mo)[1]))
    return ranges


def normalize_bizno(v) -> str:
    """사업자등록번호 → 10자리 숫자 문자열 (하이픈·공백 제거)"""
    s = str(v or "").replace("-", "").replace(" ", "")
    if not s:
        return ""
    return s.zfill(10) if s.isdigit() else s


def parse_amount(value) -> int:
    """금액 문자열 → 정수 (원)"""
    try:
        return int(str(value).replace(",", "").replace(" ", "") or 0)
    except (ValueError, TypeError):
        return 0


def format_amount(amount: int) -> str:
    """원 → 한국식 금액 표현"""
    if amount == 0:
        return "0원"
    if amount >= 100_000_000:
        return f"{amount / 100_000_000:,.1f}억원"
    if amount >= 10_000:
        return f"{amount / 10_000:,.0f}만원"
    return f"{amount:,}원"
