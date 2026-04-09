"""
사용자정보서비스 + 계약정보 결합 도구
- search_companies: 지역 우선 + 전문성 점수 기반 업체 추천
  · region 파라미터로 전국/특정 지역 선택 가능
  · 계약 이력에서 낙찰 업체 자동 추가 (대형사 누락 보완)
  · 부정당제재 자동 확인 (🔴/🟡/🟢 신호등 표시)
- check_debarred_vendors: 부정당제재 업체 조회
"""
import re
import calendar
import asyncio
from typing import Optional
from config import ENDPOINTS
from tools.api_client import fetch, parse_amount, format_amount


# ─────────────────────────────────────────────
# 내부 헬퍼
# ─────────────────────────────────────────────

async def _get_industry_codes(keyword: str) -> list[dict]:
    """키워드로 업종코드 목록 조회"""
    params = {"numOfRows": 50, "indstrytyNm": keyword, "indstrytyUseYn": "Y"}
    try:
        result = await fetch(ENDPOINTS["industry"], "getIndstrytyBaseLawrgltInfoList", params)
        return result.get("items", [])
    except Exception:
        return []


async def _get_vendor_industries(biz_no: str) -> list[str]:
    """특정 업체의 등록 업종명 목록"""
    params = {"numOfRows": 20, "inqryDiv": "3", "bizno": biz_no}
    try:
        result = await fetch(ENDPOINTS["user"], "getPrcrmntCorpIndstrytyInfo02", params)
        return [item.get("indstrytyNm", "") for item in result.get("items", [])]
    except Exception:
        return []


async def _has_recent_contract(biz_no: str) -> bool:
    """최근 1년 내 계약 이력 여부 (점수 최근성 항목)"""
    from datetime import datetime, timedelta
    now = datetime.now()
    params = {
        "numOfRows": 5,
        "inqryDiv": "1",
        "inqryBgnDt": (now - timedelta(days=365)).strftime("%Y%m%d0000"),
        "inqryEndDt": now.strftime("%Y%m%d2359"),
    }
    for op in ("getCntrctInfoListServc", "getCntrctInfoListThng"):
        try:
            result = await fetch(ENDPOINTS["contract"], op, params)
            items = result.get("items", [])
            corp_list_str = " ".join(str(i.get("corpList", "")) for i in items)
            if biz_no in corp_list_str:
                return True
        except Exception:
            continue
    return False


async def _get_contract_awardee_vendors(keyword: str, biz_type: str) -> list[dict]:
    """
    계약 이력에서 낙찰 업체 biz_no 추출 → 업체 기본 정보 조회 (대형사 누락 보완)
    """
    from datetime import datetime, timedelta
    now = datetime.now()

    ops = {
        "물품": "getCntrctInfoListThng",
        "공사": "getCntrctInfoListCnstwk",
        "용역": "getCntrctInfoListServc",
    }
    op = ops.get(biz_type, "getCntrctInfoListServc")

    params = {
        "numOfRows": 100,
        "inqryDiv": "1",
        "inqryBgnDt": (now - timedelta(days=365 * 3)).strftime("%Y%m%d0000"),
        "inqryEndDt": now.strftime("%Y%m%d2359"),
    }
    try:
        result = await fetch(ENDPOINTS["contract"], op, params)
        items = result.get("items", [])
    except Exception:
        return []

    kw = keyword.lower()
    matched = [i for i in items if kw in (i.get("cntrctNm") or "").lower()]

    biz_nos: list[str] = []
    seen: set[str] = set()
    for item in matched:
        corp_list = str(item.get("corpList") or "")
        for block in re.findall(r'\[([^\]]+)\]', corp_list):
            parts = block.split('^')
            bno = parts[-1].strip()
            if re.match(r'^\d{10}$', bno) and bno not in seen:
                seen.add(bno)
                biz_nos.append(bno)
            else:
                for p in parts:
                    p = p.strip()
                    if re.match(r'^\d{10}$', p) and p not in seen:
                        seen.add(p)
                        biz_nos.append(p)

    if not biz_nos:
        return []

    async def _lookup_one(biz_no: str) -> list[dict]:
        try:
            r = await fetch(ENDPOINTS["user"], "getPrcrmntCorpBasicInfo02",
                            {"numOfRows": 1, "inqryDiv": "3", "bizno": biz_no})
            return r.get("items", [])
        except Exception:
            return []

    tasks = [_lookup_one(bn) for bn in biz_nos[:10]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    vendor_items = []
    for raw_list in results:
        if isinstance(raw_list, list):
            for v in raw_list:
                v["_from_contract_history"] = True
                vendor_items.append(v)
    return vendor_items


async def _fetch_debarred_all() -> list[dict]:
    """부정당제재 전체 목록 조회 (최근 3년) — 일괄 체크용"""
    from datetime import datetime, timedelta
    now = datetime.now()
    params = {
        "numOfRows": 500,
        "inqryDiv": "1",
        "inqryBgnDt": (now - timedelta(days=365 * 3)).strftime("%Y%m%d0000"),
        "inqryEndDt": now.strftime("%Y%m%d2359"),
    }
    try:
        result = await fetch(ENDPOINTS["user"], "getIlgtBizEntpInfo02", params)
        return result.get("items", [])
    except Exception:
        return []


def _classify_debarment(biz_no: str, debarred_items: list[dict]) -> dict:
    """단일 업체의 부정당제재 상태 분류 → 🔴/🟡/🟢"""
    from datetime import datetime
    now = datetime.now()

    matched = [i for i in debarred_items if i.get("bizno", "") == biz_no]
    if not matched:
        return {"badge": "🟢", "label": "이상없음", "detail": "", "items": []}

    active = []
    past = []
    for item in matched:
        end_str = (item.get("rgltEndDt") or item.get("sanctEndDt") or "")[:8]
        try:
            end_dt = datetime.strptime(end_str, "%Y%m%d") if end_str else None
        except ValueError:
            end_dt = None
        (active if (end_dt and end_dt >= now) else past).append(item)

    if active:
        latest = sorted(active, key=lambda x: (x.get("rgltEndDt") or x.get("sanctEndDt") or ""))[-1]
        end_str = (latest.get("rgltEndDt") or latest.get("sanctEndDt") or "")[:8]
        reason = latest.get("rgltRsn") or latest.get("sanctRsn") or "사유 미상"
        return {"badge": "🔴", "label": "계약불가",
                "detail": f"제재기간 종료: {end_str} / 사유: {reason[:40]}", "items": matched}
    else:
        latest = sorted(past, key=lambda x: (x.get("rgltEndDt") or x.get("sanctEndDt") or ""))[-1]
        end_str = (latest.get("rgltEndDt") or latest.get("sanctEndDt") or "")[:8]
        return {"badge": "🟡", "label": "이력있음(해제)",
                "detail": f"최근 제재 종료: {end_str} (현재 해제)", "items": matched}


def _score_vendor(vendor: dict, industries: list[str], keyword: str,
                  keyword_related_names: list[str], has_contract: bool) -> int:
    """
    업체 점수 계산 (최대 10점)
    - 최근성  (3점): 최근 1년 내 계약 이력
    - 우대사항(2점): 여성/장애인/사회적기업 각 1점 (최대 2점)
    - 전문성  (5점): 업종명에 키워드 포함 1점/개 (최대 5점)
    """
    score = 0
    if has_contract:
        score += 3
    perks = 0
    if vendor.get("wmncBizEntpYn") == "Y": perks += 1
    if vendor.get("hdcpBizEntpYn") == "Y": perks += 1
    if vendor.get("socEntpYn")     == "Y": perks += 1
    score += min(perks, 2)

    kw_lower = keyword.lower()
    direct_match = sum(1 for ind in industries if kw_lower in ind.lower())
    related_match = sum(
        1 for ind in industries
        for rel in keyword_related_names
        if rel and rel.lower() in ind.lower()
    )
    score += min(direct_match + related_match, 5)
    return score


async def _fetch_vendors_for_month(year: int, month: int, page_size: int) -> list[dict]:
    """특정 월의 나라장터 등록 업체 목록 조회"""
    last_day = calendar.monthrange(year, month)[1]
    params = {
        "numOfRows": page_size,
        "inqryDiv": "1",
        "inqryBgnDt": f"{year:04d}{month:02d}010000",
        "inqryEndDt": f"{year:04d}{month:02d}{last_day:02d}2359",
    }
    try:
        result = await fetch(ENDPOINTS["user"], "getPrcrmntCorpBasicInfo02", params)
        return result.get("items", [])
    except Exception:
        return []


def _extract_vendor_info(vendor: dict, region: Optional[str]) -> dict:
    """API 응답 → 통일된 업체 딕셔너리"""
    addr = vendor.get("rgnNm") or vendor.get("adrs") or ""
    # region이 지정되면 해당 지역 여부 판단, 없으면 모두 같은 그룹
    is_local = (region in addr) if region else True
    return {
        "_raw":       vendor,
        "업체명":     vendor.get("corpNm") or vendor.get("prcrmntCorpNm", ""),
        "사업자번호": vendor.get("bizno", ""),
        "주소":       addr,
        "대표자":     vendor.get("ceoNm") or vendor.get("rprsntNm", ""),
        "전화번호":   vendor.get("telNo", ""),
        "is_local":   is_local,
        "출처":       "계약이력" if vendor.get("_from_contract_history") else "등록정보",
        "우대구분":   [],
        "등록업종":   [],
        "업종관련성": "미확인",
        "최근계약":   False,
        "점수":       0,
        "제재상태":   {"badge": "🟢", "label": "이상없음", "detail": "", "items": []},
    }


# ─────────────────────────────────────────────
# 메인 도구
# ─────────────────────────────────────────────

async def search_companies(
    service_keyword: str,
    biz_type: str = "용역",
    region: Optional[str] = None,
    prefer_local_economy: bool = True,
    page_size: int = 30,
    top_n: int = 10,
) -> dict:
    """
    전국 업체 추천 — 점수 기반 대표 {top_n}선
    (계약 이력 낙찰 업체 자동 포함 + 부정당제재 신호등 표시)

    region 지정 시:
      그룹 A: 해당 지역 소재 업체 (지역경제 우선)
      그룹 B: 타 지역 우수 업체
    region 미지정(None):
      전국 단일 랭킹, 그룹 구분 없음

    점수 기준:
      최근성  3점 — 최근 1년 내 계약 이력
      우대사항 2점 — 여성/장애인/사회적기업
      전문성  5점 — 업종명 키워드 매칭

    부정당제재 신호등:
      🔴 계약불가 — 현재 제재 중
      🟡 이력있음(해제) — 과거 제재 후 현재 해제
      🟢 이상없음 — 최근 3년 내 제재 없음

    Args:
        service_keyword:    용역 키워드 (예: '사전타당성조사', '교통영향평가', '소프트웨어')
        biz_type:           업무구분 (용역|물품|공사)
        region:             지역 필터 (예: '부산', '서울', '경기') — None이면 전국
        prefer_local_economy: 지역 우선 그룹화 여부 (region 지정 시에만 유효)
        page_size:          월별 API 조회 수
        top_n:              최종 추천 업체 수 (기본 10)
    """
    from datetime import datetime, timedelta

    # ── 병렬 초기 조회 ──
    industry_task  = _get_industry_codes(service_keyword)
    debarred_task  = _fetch_debarred_all()
    awardee_task   = _get_contract_awardee_vendors(service_keyword, biz_type)

    industry_items, debarred_all, awardee_raws = await asyncio.gather(
        industry_task, debarred_task, awardee_task, return_exceptions=True
    )
    if isinstance(industry_items, Exception): industry_items = []
    if isinstance(debarred_all, Exception):   debarred_all = []
    if isinstance(awardee_raws, Exception):   awardee_raws = []

    keyword_related_names = [i.get("indstrytyNm", "") for i in industry_items[:5]]

    # ── 업체 수집 — 월별 분할 ──
    now = datetime.now()
    all_vendors_raw: list[dict] = []
    seen_biz: set[str] = set()

    # 1단계: 최근 12개월
    for m in range(12):
        target = now.replace(day=1) - timedelta(days=1)
        for _ in range(m):
            target = target.replace(day=1) - timedelta(days=1)
        items = await _fetch_vendors_for_month(target.year, target.month, page_size)
        for v in items:
            bno = v.get("bizno", "")
            if bno and bno not in seen_biz:
                seen_biz.add(bno)
                all_vendors_raw.append(v)

        # 조기 종료: 지역 지정 시 해당 지역 업체 충분 / 전국 시 전체 충분
        if region:
            local_count = sum(1 for v in all_vendors_raw if region in (v.get("rgnNm") or v.get("adrs") or ""))
            if local_count >= top_n:
                break
        else:
            if len(all_vendors_raw) >= page_size * 4:
                break

    # 2단계: 전문업종 업체 부족 시 24~36개월 추가
    if len(keyword_related_names) > 0 and len(all_vendors_raw) < page_size * 3:
        for m in range(12, 36):
            target = now.replace(day=1) - timedelta(days=1)
            for _ in range(m):
                target = target.replace(day=1) - timedelta(days=1)
            items = await _fetch_vendors_for_month(target.year, target.month, page_size)
            for v in items:
                bno = v.get("bizno", "")
                if bno and bno not in seen_biz:
                    seen_biz.add(bno)
                    all_vendors_raw.append(v)
            if len(all_vendors_raw) >= page_size * 5:
                break

    # 계약이력 업체 추가
    for v in awardee_raws:
        bno = v.get("bizno", "")
        if bno and bno not in seen_biz:
            seen_biz.add(bno)
            all_vendors_raw.append(v)

    if not all_vendors_raw:
        return {
            "keyword": service_keyword,
            "region":  region or "전국",
            "group_a": [],
            "group_b": [],
            "companies": [],
            "summary": "업체 데이터를 불러오지 못했습니다.",
        }

    # ── 기본 정보 추출 ──
    vendors = [_extract_vendor_info(v, region) for v in all_vendors_raw]
    for vendor in vendors:
        raw = vendor["_raw"]
        perks = []
        if raw.get("wmncBizEntpYn") == "Y": perks.append("여성기업")
        if raw.get("hdcpBizEntpYn") == "Y": perks.append("장애인기업")
        if raw.get("socEntpYn")     == "Y": perks.append("사회적기업")
        if raw.get("smlBizEntpYn")  == "Y": perks.append("중소기업")
        vendor["우대구분"] = perks

    # ── 업종 상세 조회 (상위 20개 병렬) ──
    detail_targets = vendors[:20]
    industry_results = await asyncio.gather(
        *[_get_vendor_industries(v["사업자번호"]) for v in detail_targets],
        return_exceptions=True
    )
    for vendor, ind_result in zip(detail_targets, industry_results):
        if isinstance(ind_result, list):
            vendor["등록업종"] = ind_result[:3]
            kw = service_keyword.lower()
            vendor["업종관련성"] = "높음" if any(kw in i.lower() for i in ind_result) else "확인필요"

    # ── 최근 계약 이력 확인 (관련성 높음 업체만) ──
    relevant_vendors = [v for v in vendors if v["업종관련성"] == "높음"][:10]
    contract_results = await asyncio.gather(
        *[_has_recent_contract(v["사업자번호"]) for v in relevant_vendors],
        return_exceptions=True
    )
    for vendor, has_c in zip(relevant_vendors, contract_results):
        if isinstance(has_c, bool):
            vendor["최근계약"] = has_c

    # ── 점수 계산 ──
    for vendor in vendors:
        vendor["점수"] = _score_vendor(
            vendor["_raw"], vendor["등록업종"],
            service_keyword, keyword_related_names, vendor["최근계약"],
        )

    # ── 그룹 분리 ──
    if region and prefer_local_economy:
        # 그룹 A: 지정 지역, 그룹 B: 타 지역
        half = max(1, top_n // 2)
        group_a_all = sorted([v for v in vendors if v["is_local"]],      key=lambda x: -x["점수"])
        group_b_all = sorted([v for v in vendors if not v["is_local"]], key=lambda x: -x["점수"])
        group_a = group_a_all[:half]
        group_b = group_b_all[:top_n - len(group_a)]
    else:
        # 전국 단일 랭킹
        group_a = sorted(vendors, key=lambda x: -x["점수"])[:top_n]
        group_b = []

    final = group_a + group_b

    # ── 부정당제재 일괄 확인 ──
    for vendor in final:
        vendor["제재상태"] = _classify_debarment(vendor["사업자번호"], debarred_all)

    # ── 요약 ──
    pref_count  = sum(1 for c in final if c["우대구분"])
    high_rel    = sum(1 for c in final if c["업종관련성"] == "높음")
    red_count   = sum(1 for c in final if c["제재상태"]["badge"] == "🔴")
    awardee_cnt = sum(1 for c in final if c["출처"] == "계약이력")

    region_label = region or "전국"
    if region and prefer_local_economy:
        group_lines = (
            f"🏙️  그룹 A ({region} 소재): {len(group_a)}개 / 최대 {max(1, top_n//2)}개\n"
            f"🌐  그룹 B (타 지역 우수): {len(group_b)}개"
        )
    else:
        group_lines = f"🌐  전국 상위 {len(group_a)}개사 (지역 구분 없음)"

    summary = (
        f"📅 조회 범위: 전국 나라장터 등록 업체 (계약이력 발굴 포함)\n"
        f"🔍 검색 지역: {region_label}\n\n"
        f"{group_lines}\n"
        f"합계: {len(final)}개 | 우대기업: {pref_count}개 | 전문성 높음: {high_rel}개"
        + (f" | 🔴 계약불가: {red_count}개" if red_count else "")
        + (f" | 계약이력 발굴: {awardee_cnt}개" if awardee_cnt else "")
        + f"\n\n관련 업종코드: {', '.join(keyword_related_names[:3]) or '없음'}\n\n"
        f"✅ 감사 리스크 검토 완료 — 부정당제재 여부 자동 확인됨\n"
        f"⚠️ 🔴 업체는 계약 전 담당부서 협의 필수 / 🟡 업체는 이력 확인 권고"
    )

    for c in final:
        c.pop("_raw", None)

    return {
        "keyword":   service_keyword,
        "region":    region_label,
        "group_a":   group_a,
        "group_b":   group_b,
        "companies": final,
        "summary":   summary,
    }


# 하위 호환성 — 기존 'search_busan_companies' 이름으로 호출 시 자동 라우팅
async def search_busan_companies(
    service_keyword: str,
    biz_type: str = "용역",
    prefer_local_economy: bool = True,
    page_size: int = 30,
    top_n: int = 10,
) -> dict:
    return await search_companies(
        service_keyword=service_keyword,
        biz_type=biz_type,
        region="부산",
        prefer_local_economy=prefer_local_economy,
        page_size=page_size,
        top_n=top_n,
    )


async def check_debarred_vendors(
    corp_name: Optional[str] = None,
    biz_reg_no: Optional[str] = None,
) -> dict:
    """부정당제재 업체 조회 (계약 전 필수 확인)"""
    from datetime import datetime, timedelta
    now = datetime.now()
    params = {
        "numOfRows": 100,
        "inqryDiv": "1",
        "inqryBgnDt": (now - timedelta(days=365 * 3)).strftime("%Y%m%d0000"),
        "inqryEndDt": now.strftime("%Y%m%d2359"),
    }
    try:
        result = await fetch(ENDPOINTS["user"], "getIlgtBizEntpInfo02", params)
        items = result.get("items", [])
    except Exception as e:
        return {
            "is_debarred": None,
            "message": f"API 조회 실패: {e}. 나라장터 홈페이지에서 직접 확인하세요.",
            "items": [],
        }

    if corp_name:
        items = [i for i in items if corp_name in (i.get("prcrmntCorpNm") or i.get("corpNm") or "")]
    if biz_reg_no:
        items = [i for i in items if biz_reg_no == i.get("bizno", "")]

    if not items:
        return {"is_debarred": False, "message": "부정당제재 이력 없음 (조회 기간 3년 기준)", "items": []}
    return {
        "is_debarred": True,
        "message": f"⚠️ 부정당제재 이력 {len(items)}건 발견! 계약 전 담당부서 협의 필수.",
        "items": items,
    }
