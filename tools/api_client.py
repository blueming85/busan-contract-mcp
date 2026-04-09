"""나라장터 API 공통 HTTP 클라이언트 (urllib 기반, asyncio.to_thread로 비동기 지원)"""
import asyncio
import json
import urllib.request
import urllib.parse
from config import SERVICE_KEY


def _sync_fetch(url: str, params: dict) -> dict:
    """동기 HTTP GET 호출"""
    query = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{query}"
    req = urllib.request.Request(full_url, headers={"User-Agent": "BusanContractMCP/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
    data = json.loads(raw)

    # 일부 서비스는 비표준 오류 형식 반환: {"nkoneps.com.response.ResponseError": {...}}
    err_key = next((k for k in data if "ResponseError" in k), None)
    if err_key:
        hdr = data[err_key].get("header", {})
        raise ValueError(f"API 오류 [{hdr.get('resultCode','?')}]: {hdr.get('resultMsg','알 수 없는 오류')}")

    response = data.get("response", {})
    header = response.get("header", {})
    code = header.get("resultCode", "00")
    if code not in ("00", "0000"):
        raise ValueError(f"API 오류 [{code}]: {header.get('resultMsg', '알 수 없는 오류')}")

    body = response.get("body", {})
    items = body.get("items", [])
    total_count = body.get("totalCount", 0)

    if isinstance(items, dict):
        items = [items] if items else []

    return {"totalCount": total_count, "items": items}


async def fetch(endpoint: str, operation: str, params: dict) -> dict:
    """나라장터 REST API 비동기 호출"""
    base_params = {
        "ServiceKey": SERVICE_KEY,
        "type": "json",
        "numOfRows": params.pop("numOfRows", 100),
        "pageNo": params.pop("pageNo", 1),
    }
    base_params.update(params)

    url = f"{endpoint}/{operation}"
    return await asyncio.to_thread(_sync_fetch, url, base_params)


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
