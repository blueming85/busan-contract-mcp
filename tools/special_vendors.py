"""
장애인기업 / 여성기업 낙찰 순위 도구

데이터 출처:
  장애인기업 — (재)장애인기업종합지원센터 드림365 API (api.odcloud.kr)
  여성기업   — 여성기업종합지원센터 엑셀 (WOMEN_EXCEL_PATH 환경변수 or .env)

교차 분석:
  두 인증 목록을 나라장터 낙찰이력과 사업자등록번호로 교차 → 낙찰 횟수 집계 → 순위 출력
"""
import asyncio
import gzip
import json
import urllib.request
import urllib.parse
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from tools.api_client import (
    ApiKeyError,
    parse_amount,
    format_amount,
    normalize_bizno as _normalize_bizno,
)

# ─── 장애인기업 API (odcloud.kr) ────────────────
_DISABILITY_API = (
    "https://api.odcloud.kr/api/15035258/v1"
    "/uddi:96ddf7e3-c331-47db-8897-8a8784480dbe"
)

# ─── 여성기업 JSON 경로 (레포 내 번들 데이터) ────────
_WOMEN_JSON_PATH = Path(__file__).parent.parent / "data" / "women_enterprises.json"

# ─── 모듈 캐시 (1시간 TTL) ────────────────────────
_cache: dict = {}
_cache_ts: dict = {}
_CACHE_TTL = 3600


# ──────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────
def _is_valid_period(period_str: str) -> bool:
    """'YYYY-MM-DD ~ YYYY-MM-DD' 형식 유효기간이 오늘 이후인지 확인"""
    try:
        end_str = str(period_str or "").split("~")[-1].strip()[:10]
        return datetime.strptime(end_str, "%Y-%m-%d").date() >= date.today()
    except Exception:
        return True  # 파싱 불가 → 유효로 간주


def _cache_get(key: str):
    if key in _cache:
        if (datetime.now() - _cache_ts[key]).seconds < _CACHE_TTL:
            return _cache[key]
    return None


def _cache_set(key: str, val):
    _cache[key] = val
    _cache_ts[key] = datetime.now()


# ──────────────────────────────────────────
# 장애인기업 API 로드
# ──────────────────────────────────────────
def _sync_disability_page(service_key: str, page: int, per_page: int) -> dict:
    params = {
        "serviceKey": service_key,
        "page": page, "perPage": per_page, "returnType": "json",
    }
    url = _DISABILITY_API + "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    req = urllib.request.Request(url, headers={"User-Agent": "BusanContractMCP/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


async def _load_disability(region: str) -> dict[str, dict]:
    """장애인기업 전체 로드 → {bizno: info}  (지역 필터 포함)"""
    key = f"disability_{region}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    from config import SERVICE_KEY
    if not SERVICE_KEY:
        raise ApiKeyError(
            "NARA_SERVICE_KEY가 설정되지 않았습니다. "
            ".env 파일에 공공데이터포털 서비스 키를 설정해주세요."
        )
    per_page = 1000

    first = await asyncio.to_thread(_sync_disability_page, SERVICE_KEY, 1, per_page)
    total = first.get("totalCount", 0)
    all_data = list(first.get("data", []))

    total_pages = (total + per_page - 1) // per_page
    if total_pages > 1:
        tasks = [
            asyncio.to_thread(_sync_disability_page, SERVICE_KEY, p, per_page)
            for p in range(2, min(total_pages + 1, 15))
        ]
        pages = await asyncio.gather(*tasks, return_exceptions=True)
        for pg in pages:
            if not isinstance(pg, Exception):
                all_data.extend(pg.get("data", []))

    result: dict[str, dict] = {}
    for item in all_data:
        addr = str(item.get("소재지", "") or "")
        if region and region not in addr:
            continue
        bizno = _normalize_bizno(item.get("사업자등록번호", ""))
        if bizno:
            result[bizno] = {
                "업체명":   str(item.get("업체명", "") or "").strip(),
                "주소":     addr,
                "주업종":   str(item.get("주업종", "") or ""),
                "인증유형": "장애인기업",
            }

    _cache_set(key, result)
    return result


# ──────────────────────────────────────────
# 여성기업 JSON 로드 (레포 번들 데이터)
# ──────────────────────────────────────────
def _sync_women_json(region: str) -> dict[str, dict]:
    gz_path = _WOMEN_JSON_PATH.with_name(_WOMEN_JSON_PATH.name + ".gz")
    if _WOMEN_JSON_PATH.exists():
        with open(_WOMEN_JSON_PATH, encoding="utf-8") as f:
            rows = json.load(f)
    elif gz_path.exists():
        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            rows = json.load(f)
    else:
        return {}
    result: dict[str, dict] = {}
    for item in rows:
        addr = item.get("a", "")
        if region and region not in addr:
            continue
        bizno = item.get("b", "")
        name  = item.get("n", "")
        if bizno and name:
            result[bizno] = {
                "업체명":   name,
                "주소":     addr,
                "주업종":   item.get("j", ""),
                "인증유형": "여성기업",
            }
    return result


async def _load_women(region: str) -> dict[str, dict]:
    """여성기업 JSON 로드 → {bizno: info}"""
    key = f"women_{region}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    result = await asyncio.to_thread(_sync_women_json, region)
    _cache_set(key, result)
    return result


# ──────────────────────────────────────────
# 나라장터 낙찰이력 광범위 조회 (numOfRows=999)
# ──────────────────────────────────────────
async def _broad_award_search(
    biz_types: list[str],
    keyword: Optional[str],
    months_back: int,
) -> list[dict]:
    """여러 업무구분을 병렬로, numOfRows=999로 광범위 낙찰이력 조회

    동시 호출 수는 api_client의 전역 세마포어가 제한합니다.
    """
    from config import ENDPOINTS
    from tools.api_client import fetch, month_ranges

    PP_OPS = {
        "물품": "getScsbidListSttusThngPPSSrch",
        "공사": "getScsbidListSttusCnstwkPPSSrch",
        "용역": "getScsbidListSttusServcPPSSrch",
    }

    ranges = month_ranges(months_back)

    async def _one(op: str, year: int, month: int, last_day: int) -> list[dict]:
        params: dict = {
            "numOfRows": 999,
            "inqryDiv":  "1",
            "inqryBgnDt": f"{year:04d}{month:02d}010000",
            "inqryEndDt": f"{year:04d}{month:02d}{last_day:02d}2359",
        }
        if keyword:
            params["bidNtceNm"] = keyword
        try:
            result = await fetch(ENDPOINTS["award"], op, params)
            return result.get("items", [])
        except ApiKeyError:
            raise
        except Exception:
            return []

    coros = [
        _one(PP_OPS[bt], yr, mo, ld)
        for bt in biz_types if bt in PP_OPS
        for (yr, mo, ld) in ranges
    ]
    results = await asyncio.gather(*coros, return_exceptions=True)

    key_err = next((r for r in results if isinstance(r, ApiKeyError)), None)
    if key_err:
        raise key_err

    all_items: list[dict] = []
    seen: set[str] = set()
    for res in results:
        if isinstance(res, Exception):
            continue
        for item in res:
            uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
            if uid and uid not in seen:
                seen.add(uid)
                all_items.append(item)
    return all_items


# ──────────────────────────────────────────
# 메인 도구
# ──────────────────────────────────────────
async def search_special_vendors(
    service_keyword: Optional[str] = None,
    vendor_type: str = "all",        # "장애인" / "여성" / "all"
    region: str = "부산",
    biz_type: str = "all",           # "물품"/"공사"/"용역"/"all"
    months_back: int = 24,
    top_n: int = 20,
) -> dict:
    """
    장애인기업 / 여성기업의 나라장터 낙찰 순위를 조회합니다.

    Args:
        service_keyword: 공고명 키워드 (생략 시 광범위 검색)
        vendor_type:     "장애인" / "여성" / "all"
        region:          지역 필터 (기본 "부산")
        biz_type:        업무구분 (기본 "all" = 물품+공사+용역 동시 검색)
        months_back:     낙찰이력 소급 개월 수 (기본 24)
        top_n:           결과 최대 수 (기본 20)
    """
    # ── 1. 인증 업체 목록 병렬 로드 ──
    dis_task = _load_disability(region) if vendor_type in ("장애인", "all") else asyncio.sleep(0)
    wom_task = _load_women(region)      if vendor_type in ("여성",   "all") else asyncio.sleep(0)
    dis_raw, wom_raw = await asyncio.gather(dis_task, wom_task)

    disability_map: dict[str, dict] = dis_raw if isinstance(dis_raw, dict) else {}
    women_map:      dict[str, dict] = wom_raw if isinstance(wom_raw, dict) else {}

    # _error 키 제거
    disability_map = {k: v for k, v in disability_map.items() if not k.startswith("_")}
    women_map      = {k: v for k, v in women_map.items()      if not k.startswith("_")}

    # 통합 인증 맵 (장애인 우선)
    certified: dict[str, dict] = {**women_map, **disability_map}

    if not certified:
        return {
            "error": f"{region} {vendor_type} 인증 업체를 찾을 수 없습니다.",
            "items": [], "summary": "",
        }

    # ── 2. 나라장터 낙찰이력 광범위 조회 ──
    biz_types = ["물품", "공사", "용역"] if biz_type == "all" else [biz_type]

    if service_keyword:
        award_items = await _broad_award_search(biz_types, service_keyword, months_back)
    else:
        # 키워드 없을 때: 주요 업종 키워드 배치 검색으로 커버리지 확대
        _BATCH_KEYWORDS = [
            "청소", "경비", "시설관리", "소프트웨어", "환경미화",
            "소독", "조경", "주차", "건설", "인쇄", "번역",
            "교육", "컨설팅", "디자인", "홍보", "유지보수",
        ]
        batch_coros = [
            _broad_award_search(biz_types, kw, months_back)
            for kw in _BATCH_KEYWORDS
        ]
        batch_results = await asyncio.gather(*batch_coros, return_exceptions=True)

        key_err = next((r for r in batch_results if isinstance(r, ApiKeyError)), None)
        if key_err:
            raise key_err

        award_items_all: list[dict] = []
        seen_uids: set[str] = set()
        for res in batch_results:
            if isinstance(res, Exception):
                continue
            for item in res:
                uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
                if uid and uid not in seen_uids:
                    seen_uids.add(uid)
                    award_items_all.append(item)
        award_items = award_items_all

    # ── 3. 교차 분석 ──
    stats: dict[str, dict] = {}
    for item in award_items:
        raw_bno = item.get("bidwinnrBizno") or item.get("bizno") or ""
        bizno = _normalize_bizno(str(raw_bno))

        if bizno not in certified:
            continue

        amt      = parse_amount(item.get("sucsfbidAmt") or item.get("scsbidAmt") or 0)
        open_dt  = (item.get("rlOpengDt") or item.get("opengDt") or "")[:10]
        inst     = item.get("ntceInsttNm") or item.get("dminsttNm") or ""
        ann_name = item.get("bidNtceNm", "")
        rate     = item.get("sucsfbidRate") or item.get("scsbidRate") or ""

        if bizno not in stats:
            stats[bizno] = {
                **certified[bizno],
                "사업자번호":    bizno,
                "낙찰횟수":      0,
                "낙찰금액합계":  0,
                "금액목록":      [],
                "최근낙찰일":    "",
                "발주기관목록":  [],
                "대표공고명":    ann_name,
                "낙찰률":        rate,
            }

        s = stats[bizno]
        s["낙찰횟수"] += 1
        s["낙찰금액합계"] += amt
        if amt:
            s["금액목록"].append(amt)
        if inst and inst not in s["발주기관목록"]:
            s["발주기관목록"].append(inst)
        if open_dt > s["최근낙찰일"]:
            s["최근낙찰일"] = open_dt
            s["낙찰률"] = rate or s["낙찰률"]

    # ── 4. 정리 ──
    rows = []
    for bizno, s in stats.items():
        amt_list = s.pop("금액목록", [])
        avg = s["낙찰금액합계"] // s["낙찰횟수"] if s["낙찰횟수"] else 0
        rows.append({
            "인증유형":    s["인증유형"],
            "업체명":      s["업체명"],
            "주소":        s["주소"],
            "주업종":      s["주업종"],
            "사업자번호":  bizno,
            "낙찰횟수":    s["낙찰횟수"],
            "평균낙찰금액": format_amount(avg),
            "최근낙찰일":  s["최근낙찰일"],
            "발주기관":    ", ".join(s["발주기관목록"][:3]),
            "대표공고명":  s["대표공고명"],
            "낙찰률":      s["낙찰률"],
        })

    rows.sort(key=lambda x: (-x["낙찰횟수"], x["최근낙찰일"]), reverse=False)
    # sort: 낙찰횟수 내림차순 → 최근낙찰일 내림차순
    rows.sort(key=lambda x: (-x["낙찰횟수"], x["최근낙찰일"]))

    # ── 5. 요약 ──
    dis_cnt = sum(1 for r in rows if r["인증유형"] == "장애인기업")
    wom_cnt = sum(1 for r in rows if r["인증유형"] == "여성기업")
    kw_label = f"키워드: '{service_keyword}'" if service_keyword else "청소·경비·시설관리·SW 등 16개 키워드 배치 검색"
    type_kor = {"장애인": "장애인기업", "여성": "여성기업", "all": "장애인·여성기업"}.get(vendor_type, vendor_type)

    summary = (
        f"🏆 {region} {type_kor} 낙찰 순위\n"
        f"📅 최근 {months_back}개월 나라장터 낙찰이력 기준 | {kw_label}\n\n"
        f"인증 등록 현황 ({region}):\n"
        f"  장애인기업 {len(disability_map):,}개 / 여성기업 {len(women_map):,}개\n\n"
        f"낙찰 이력 확인 업체: {len(rows)}개사\n"
        + (f"  (장애인기업: {dis_cnt}개 / 여성기업: {wom_cnt}개)" if vendor_type == "all" else "")
        + "\n\n⚠️ 나라장터 광범위 샘플 기반입니다. 수의계약 등 비공개 계약은 포함되지 않습니다."
    )

    return {
        "items":   rows[:top_n],
        "summary": summary,
        "certified": {
            "장애인기업": len(disability_map),
            "여성기업":   len(women_map),
        },
        "total_found": len(rows),
    }
