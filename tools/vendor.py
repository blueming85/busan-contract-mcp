"""
업체 추천 도구 (낙찰이력 기반 v2)
- search_companies: 낙찰이력 PPSSrch → 실제 전문업체 추출 + 부정당제재 신호등
  · 직접 사례 없으면 키워드 자동 축소 → 유사 사례로 fallback
  · 지역 그룹A/B 분리 (기본: 부산)
- check_debarred_vendors: 부정당제재 업체 조회
"""
import re
import asyncio
from typing import Optional
from config import ENDPOINTS
from tools.api_client import fetch, parse_amount, format_amount


# ─────────────────────────────────────────────
# 키워드 자동 축소
# ─────────────────────────────────────────────

# AI/신기술 관련 키워드 목록
_AI_KEYWORDS = {"AI", "인공지능", "딥러닝", "머신러닝", "빅데이터", "스마트", "지능형",
                "IoT", "디지털트윈", "디지털 트윈", "블록체인", "클라우드", "자율"}


def _is_ai_related(keyword: str) -> bool:
    """AI/신기술 관련 키워드 포함 여부"""
    for ak in _AI_KEYWORDS:
        if ak.lower() in keyword.lower():
            return True
    return False


def _shrink_keyword(keyword: str) -> list[str]:
    """
    사례 없을 때 단계별 축소 키워드 생성
    "땅꺼짐 예방을 위한 AI 계측관리 연구개발"
      → ["AI 계측관리", "계측관리", "지반계측"]
    """
    kw = keyword.strip()
    candidates = []

    # AI 키워드 제거 후 핵심 명사 추출
    ai_removed = kw
    for ak in _AI_KEYWORDS:
        ai_removed = ai_removed.replace(ak, "").strip()

    stopwords = ["을 위한", "에 대한", "을 위해", "를 위한", "에 관한",
                 "연구개발", "연구용역", "학술용역", "기초연구", "예방", "방지"]
    cleaned = ai_removed
    for sw in stopwords:
        cleaned = cleaned.replace(sw, " ")
    parts = [p.strip() for p in cleaned.split() if len(p.strip()) >= 2]

    if len(parts) >= 2:
        candidates.append(" ".join(parts[-2:]))
        candidates.append(" ".join(parts[:2]))
    if parts:
        candidates.append(parts[-1])
        if len(parts) >= 2:
            candidates.append(parts[-2])

    seen = {kw}
    result = []
    for c in candidates:
        if c and c not in seen and len(c) >= 2:
            seen.add(c)
            result.append(c)
    return result[:4]


def _build_no_result_suggestion(keyword: str, is_ai: bool) -> dict:
    """검색 결과 없을 때 — 재검색 제안 응답 생성"""
    shrunk = _shrink_keyword(keyword)
    core_kw = shrunk[0] if shrunk else keyword

    ai_hint = (
        f"\n💡 AI·인공지능은 공고명보다 RFP 조건에 명시되는 경우가 많아\n"
        f"   '공공 AI', 'AI활용', '스마트' 등으로 검색하면 더 많이 나옵니다.\n"
    ) if is_ai else ""

    return {
        "keyword":         keyword,
        "region":          "",
        "group_a":         [],
        "group_b":         [],
        "companies":       [],
        "fallback":        True,
        "ai_guidance":     True,
        "core_keyword":    core_kw,
        "shrunk_keywords": shrunk,
        "summary": (
            f"🔍 '{keyword}'로 검색된 낙찰 사례가 없습니다.\n"
            f"{ai_hint}\n"
            f"다음 중 원하시는 방식으로 다시 검색해보세요:\n\n"
            f"① 핵심 업무 기준 →  '{core_kw}' 전문업체 조회\n"
            f"   (AI·신기술 보유 여부는 RFP 제안요청서에 조건으로 명시)\n\n"
            f"② 유사 키워드 → {', '.join(shrunk[:3])}\n\n"
            + (f"③ AI 공공사업 검색 → 'AI활용', '공공AI', 'AI디지털'\n\n" if is_ai else "")
            + f"어떻게 진행할까요?"
        ),
    }


# ─────────────────────────────────────────────
# 낙찰이력에서 업체 추출
# ─────────────────────────────────────────────

async def _get_awardees_from_history(
    keyword: str,
    biz_type: str,
    months_back: int,
    region: Optional[str],
) -> list[dict]:
    """
    낙찰이력 PPSSrch로 해당 키워드 수행 업체 추출
    반환: [{업체명, 사업자번호, 주소, 낙찰횟수, 최근낙찰일, 평균낙찰금액, ...}, ...]
    """
    from tools.award import _award_search_monthly

    pp_ops = {
        "물품": "getScsbidListSttusThngPPSSrch",
        "공사": "getScsbidListSttusCnstwkPPSSrch",
        "용역": "getScsbidListSttusServcPPSSrch",
        "외자": "getScsbidListSttusFrgcptPPSSrch",
    }
    op = pp_ops.get(biz_type, "getScsbidListSttusServcPPSSrch")

    raw_items = await _award_search_monthly(op, keyword, None, months_back)

    # 업체별 집계
    company_map: dict[str, dict] = {}
    for item in raw_items:
        corp = (item.get("bidwinnrNm") or item.get("scsbidCorpNm") or
                item.get("sucsfbidCrpNm") or "")
        biz_no = (item.get("bidwinnrBizno") or item.get("bizno") or "")
        addr = item.get("bidwinnrAdrs") or item.get("adrs") or ""
        open_dt = (item.get("rlOpengDt") or item.get("opengDt") or "")[:10]
        amt = parse_amount(item.get("sucsfbidAmt") or item.get("scsbidAmt") or 0)
        inst = item.get("ntceInsttNm") or item.get("dminsttNm") or ""
        rate = item.get("sucsfbidRate") or item.get("scsbidRate") or ""

        if not corp:
            continue
        key = biz_no or corp

        if key not in company_map:
            company_map[key] = {
                "업체명":      corp,
                "사업자번호":  biz_no,
                "주소":        addr,
                "낙찰횟수":    0,
                "최근낙찰일":  "",
                "낙찰금액합계": 0,
                "낙찰금액목록": [],
                "발주기관목록": [],
                "낙찰률":      rate,
                "is_local":    (region in addr) if (region and addr) else (not region),
                "_출처":       "낙찰이력",
            }
        c = company_map[key]
        c["낙찰횟수"] += 1
        c["낙찰금액합계"] += amt
        if amt:
            c["낙찰금액목록"].append(amt)
        if inst and inst not in c["발주기관목록"]:
            c["발주기관목록"].append(inst)
        if open_dt > c["최근낙찰일"]:
            c["최근낙찰일"] = open_dt
            if addr:
                c["주소"] = addr  # 최근 주소로 갱신
            if rate:
                c["낙찰률"] = rate

    companies = list(company_map.values())

    # 지역 필터 정제
    for c in companies:
        addr = c["주소"]
        c["is_local"] = (region in addr) if (region and addr) else (not region)

    return companies


async def _fetch_debarred_all() -> list[dict]:
    """부정당제재 전체 목록 (최근 3년)"""
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
    """부정당제재 상태 → 🔴/🟡/🟢"""
    from datetime import datetime
    now = datetime.now()

    matched = [i for i in debarred_items if i.get("bizno", "") == biz_no]
    if not matched:
        return {"badge": "🟢", "label": "이상없음", "detail": ""}

    active, past = [], []
    for item in matched:
        end_str = (item.get("rgltEndDt") or item.get("sanctEndDt") or "")[:8]
        try:
            end_dt = datetime.strptime(end_str, "%Y%m%d") if end_str else None
        except ValueError:
            end_dt = None
        (active if (end_dt and end_dt >= now) else past).append(item)

    if active:
        latest = sorted(active, key=lambda x: x.get("rgltEndDt") or "")[-1]
        end_str = (latest.get("rgltEndDt") or latest.get("sanctEndDt") or "")[:8]
        reason = (latest.get("rgltRsn") or latest.get("sanctRsn") or "사유 미상")[:40]
        return {"badge": "🔴", "label": "계약불가",
                "detail": f"제재종료: {end_str} / {reason}"}
    else:
        latest = sorted(past, key=lambda x: x.get("rgltEndDt") or "")[-1]
        end_str = (latest.get("rgltEndDt") or latest.get("sanctEndDt") or "")[:8]
        return {"badge": "🟡", "label": "이력있음(해제)",
                "detail": f"최근제재종료: {end_str}"}


def _score_company(c: dict) -> int:
    """
    낙찰이력 기반 점수 (최대 10점)
    - 전문성 5점: 낙찰 횟수 (1건=1점, 최대5점)
    - 최근성 3점: 최근 2년 내 낙찰
    - 경쟁력 2점: 낙찰률 90% 이상
    """
    from datetime import datetime, timedelta
    score = 0

    # 전문성: 낙찰 횟수
    score += min(c.get("낙찰횟수", 0), 5)

    # 최근성
    latest = c.get("최근낙찰일", "")
    if latest:
        try:
            dt = datetime.strptime(latest[:10], "%Y-%m-%d")
            if dt >= datetime.now() - timedelta(days=365 * 2):
                score += 3
        except ValueError:
            pass

    # 경쟁력: 낙찰률
    try:
        rate = float(c.get("낙찰률", 0) or 0)
        if rate >= 90:
            score += 2
        elif rate >= 80:
            score += 1
    except (ValueError, TypeError):
        pass

    return score


# ─────────────────────────────────────────────
# 메인 도구
# ─────────────────────────────────────────────

async def search_companies(
    service_keyword: str,
    biz_type: str = "용역",
    region: Optional[str] = "부산",
    prefer_local_economy: bool = True,
    top_n: int = 10,
    months_back: int = 48,
) -> dict:
    """
    낙찰이력 기반 업체 추천 (PPSSrch → 실제 전문업체 추출)

    직접 사례 없으면 키워드 자동 축소 → 유사 사례로 fallback.

    그룹 A (기본: 부산): 지역 소재 업체
    그룹 B: 타 지역 전문업체
    region=None: 전국 단일 랭킹

    Args:
        service_keyword:  용역 키워드 (예: '사전타당성조사', '계측관리', '도시철도')
        biz_type:         업무구분 (용역|물품|공사)
        region:           지역 우선 (기본 '부산', None=전국)
        prefer_local_economy: 지역 그룹화 여부
        top_n:            최종 추천 수 (기본 10)
        months_back:      소급 개월 수 (기본 48개월)
    """
    # 부정당제재 목록은 병렬로 미리 받아둠
    debarred_task = asyncio.create_task(_fetch_debarred_all())

    # ── 1단계: 직접 키워드 검색 ──
    companies = await _get_awardees_from_history(
        service_keyword, biz_type, months_back, region
    )

    fallback_keyword = None
    fallback_note = ""

    # ── 2단계: 0건 처리 ──
    if not companies:
        # AI/신기술 키워드 감지 → 자동 fallback 전에 제안 응답 반환
        if _is_ai_related(service_keyword):
            return _build_no_result_suggestion(service_keyword, is_ai=True)

        # 일반 키워드 — 자동 축소 fallback (최대 24개월로 속도 제한)
        fallback_months = min(months_back, 24)
        shrunk = _shrink_keyword(service_keyword)
        for alt_kw in shrunk:
            companies = await _get_awardees_from_history(
                alt_kw, biz_type, fallback_months, region
            )
            if companies:
                fallback_keyword = alt_kw
                fallback_note = (
                    f"⚠️ '{service_keyword}' 직접 사례 없음 — "
                    f"유사 키워드 '{alt_kw}' 기준 참고 사례 {len(companies)}건"
                )
                break

    debarred_all = await debarred_task

    if not companies:
        return _build_no_result_suggestion(service_keyword, is_ai=_is_ai_related(service_keyword))

    # ── 점수 계산 ──
    for c in companies:
        c["점수"] = _score_company(c)
        avg_amt = (c["낙찰금액합계"] // c["낙찰횟수"]) if c["낙찰횟수"] else 0
        c["평균낙찰금액"] = format_amount(avg_amt)
        c["발주기관"] = ", ".join(c["발주기관목록"][:3])
        # 출력용 정제
        c.pop("낙찰금액합계", None)
        c.pop("낙찰금액목록", None)
        c.pop("발주기관목록", None)
        c.pop("_출처", None)

    # ── 그룹 분리 ──
    if region and prefer_local_economy:
        half = max(1, top_n // 2)
        group_a = sorted([c for c in companies if c["is_local"]],      key=lambda x: -x["점수"])[:half]
        group_b = sorted([c for c in companies if not c["is_local"]], key=lambda x: -x["점수"])[:(top_n - len(group_a))]
    else:
        group_a = sorted(companies, key=lambda x: -x["점수"])[:top_n]
        group_b = []

    final = group_a + group_b

    # ── 부정당제재 확인 ──
    for c in final:
        c["제재상태"] = _classify_debarment(c["사업자번호"], debarred_all)

    # ── 요약 ──
    red_count = sum(1 for c in final if c["제재상태"]["badge"] == "🔴")
    region_label = region or "전국"

    if region and prefer_local_economy:
        group_lines = (
            f"🏙️  그룹 A ({region} 소재): {len(group_a)}개\n"
            f"🌐  그룹 B (타 지역 전문): {len(group_b)}개"
        )
    else:
        group_lines = f"🌐  전국 상위 {len(group_a)}개사"

    summary = (
        f"{'📋 ' + fallback_note + chr(10) + chr(10) if fallback_note else ''}"
        f"📅 최근 {months_back}개월 나라장터 낙찰이력 기준\n"
        f"🔍 지역: {region_label} | 키워드: '{fallback_keyword or service_keyword}'\n\n"
        f"{group_lines}\n"
        f"합계: {len(final)}개"
        + (f" | 🔴 계약불가: {red_count}개" if red_count else "")
        + "\n\n✅ 감사 리스크 검토 완료 — 부정당제재 자동 확인\n"
        f"⚠️ 🔴 업체는 계약 전 담당부서 협의 필수 / 🟡 이력 확인 권고"
    )

    return {
        "keyword":         service_keyword,
        "matched_keyword": fallback_keyword or service_keyword,
        "region":          region_label,
        "fallback":        bool(fallback_keyword),
        "group_a":         group_a,
        "group_b":         group_b,
        "companies":       final,
        "summary":         summary,
    }


# 하위 호환성
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
            "message": f"API 조회 실패: {e}",
            "items": [],
        }

    if corp_name:
        items = [i for i in items if corp_name in (i.get("prcrmntCorpNm") or i.get("corpNm") or "")]
    if biz_reg_no:
        items = [i for i in items if biz_reg_no == i.get("bizno", "")]

    if not items:
        return {"is_debarred": False, "message": "부정당제재 이력 없음 (3년 기준)", "items": []}
    return {
        "is_debarred": True,
        "message": f"⚠️ 부정당제재 이력 {len(items)}건! 계약 전 담당부서 협의 필수.",
        "items": items,
    }
