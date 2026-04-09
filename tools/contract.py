"""
계약정보서비스 도구 모음
- search_contracts: 계약 목록 검색
- analyze_price_benchmark: 유사사업 낙찰가 분석 → 적정 기초금액 제안
- get_contract_detail: 통합계약번호로 상세 조회
"""
from typing import Optional
from statistics import mean, median
from config import ENDPOINTS
from tools.api_client import fetch, parse_amount, format_amount


async def search_contracts(
    keyword: Optional[str] = None,
    inst_name: Optional[str] = None,
    contract_method: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    biz_type: str = "용역",  # 물품 | 공사 | 용역 | 외자
    min_amount: Optional[int] = None,
    max_amount: Optional[int] = None,
    page_size: int = 50,
) -> dict:
    """
    나라장터 계약정보 검색 (나라장터 상세페이지 링크 포함)

    Args:
        keyword: 계약건명 키워드 (예: "청소", "경비")
        inst_name: 계약기관명 (예: "부산", "부산광역시")
        contract_method: 계약방법 (예: "수의계약", "일반경쟁")
        start_date: 조회 시작일 (YYYYMMDDHHMM)
        end_date: 조회 종료일 (YYYYMMDDHHMM)
        biz_type: 업무구분 (물품|공사|용역|외자)
        min_amount: 최소 계약금액 (원)
        max_amount: 최대 계약금액 (원)
        page_size: 최대 결과 수

    Returns:
        {"totalCount": int, "items": [...]} — 각 item에 나라장터 상세 링크 포함
    """
    # 업무구분별 오퍼레이션 매핑
    ops = {
        "물품": "getCntrctInfoListThng",
        "공사": "getCntrctInfoListCnstwk",
        "용역": "getCntrctInfoListServc",
        "외자": "getCntrctInfoListFrgcpt",
    }
    operation = ops.get(biz_type, "getCntrctInfoListServc")

    params: dict = {"numOfRows": page_size}

    if start_date and end_date:
        params["inqryDiv"] = "1"
        params["inqryBgnDt"] = start_date
        params["inqryEndDt"] = end_date
    else:
        # 기본: 최근 3개월 (1년은 너무 느림)
        from datetime import datetime, timedelta
        now = datetime.now()
        params["inqryDiv"] = "1"
        params["inqryBgnDt"] = (now - timedelta(days=90)).strftime("%Y%m%d0000")
        params["inqryEndDt"] = now.strftime("%Y%m%d2359")

    result = await fetch(ENDPOINTS["contract"], operation, params)
    items = result["items"]

    # 클라이언트 사이드 필터링 (API가 텍스트 검색 미지원)
    if keyword:
        kw = keyword.lower()
        items = [
            i for i in items
            if kw in (i.get("cntrctNm") or "").lower()
            or kw in (i.get("prdctClsfcNoNm") or "").lower()
        ]
    if inst_name:
        items = [
            i for i in items
            if inst_name in (i.get("cntrctInsttNm") or "")
        ]
    if contract_method:
        items = [
            i for i in items
            if contract_method in (i.get("cntrctMthdNm") or "")
        ]

    # 금액 필터링
    if min_amount is not None or max_amount is not None:
        filtered = []
        for i in items:
            amt = parse_amount(i.get("thtmCntrctAmt") or i.get("totCntrctAmt") or 0)
            if min_amount is not None and amt < min_amount:
                continue
            if max_amount is not None and amt > max_amount:
                continue
            filtered.append(i)
        items = filtered

    # 각 항목에 나라장터 상세 링크 추가
    for item in items:
        # API가 직접 제공하는 상세 URL 사용 (cntrctDtlInfoUrl)
        detail_url = item.get("cntrctDtlInfoUrl", "")
        if not detail_url:
            # 폴백: 통합계약번호로 직접 URL 조합
            unty_no = item.get("untyCntrctNo", "")
            if unty_no:
                detail_url = f"https://www.g2b.go.kr/link/FIUA027_01/single/?ctrtNo={unty_no}"
        item["나라장터상세URL"] = detail_url

        # 연결된 공고번호가 있으면 입찰공고 링크도 추가
        ntce_no = item.get("ntceNo", "")
        if ntce_no:
            item["원공고URL"] = f"https://www.g2b.go.kr/link/PNPE027_01/single/?bidPbancNo={ntce_no}&bidPbancOrd=000"

    return {
        "totalCount": result["totalCount"],
        "matchedCount": len(items),
        "items": items,
    }


async def _fetch_bid_by_keyword(
    keyword: str,
    biz_type: str,
    inst_region: Optional[str],
    months_back: int = 24,
) -> list:
    """
    입찰공고 PPSSrch + 월별 분할 쿼리 (연간 쿼리는 API 내부 타임아웃으로 빈 응답)
    → 예산금액(asignBdgtAmt)을 계약금액 대리지표로 사용
    """
    from datetime import datetime, timedelta
    import calendar

    ops = {
        "물품": "getBidPblancListInfoThngPPSSrch",
        "공사": "getBidPblancListInfoCnstwkPPSSrch",
        "용역": "getBidPblancListInfoServcPPSSrch",
    }
    op = ops.get(biz_type, "getBidPblancListInfoServcPPSSrch")

    now = datetime.now()
    all_items = []
    seen_ids = set()

    # 1개월씩 최대 months_back개월 소급 (단, 최대 30회 API 호출 제한)
    for m in range(min(months_back, 30)):
        # m개월 전 달
        target = now.replace(day=1) - timedelta(days=1)  # 이전 달 말일
        for _ in range(m):
            target = target.replace(day=1) - timedelta(days=1)
        year = target.year
        month = target.month
        last_day = calendar.monthrange(year, month)[1]

        start_dt = f"{year:04d}{month:02d}010000"
        end_dt   = f"{year:04d}{month:02d}{last_day:02d}2359"

        params: dict = {
            "numOfRows": 100,
            "bidNtceNm": keyword,
            "inqryDiv": "1",
            "inqryBgnDt": start_dt,
            "inqryEndDt": end_dt,
        }
        try:
            result = await fetch(ENDPOINTS["bid"], op, params)
            items = result.get("items", [])
            for item in items:
                uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
                if uid and uid not in seen_ids:
                    seen_ids.add(uid)
                    all_items.append(item)
        except Exception:
            continue  # 해당 월 오류 시 스킵

    if inst_region:
        all_items = [i for i in all_items if inst_region in (i.get("ntceInsttNm") or "")]

    return all_items


async def analyze_price_benchmark(
    keyword: str,
    biz_type: str = "용역",
    inst_region: Optional[str] = None,
    years: int = 3,
) -> dict:
    """
    유사 사업 예산·낙찰가 분석 → 적정 기초금액 제안

    입찰공고의 서버사이드 키워드 검색(bidNtceNm)을 사용하므로
    '사전타당성조사', '기본계획' 같은 긴 키워드도 정확하게 검색됩니다.

    Args:
        keyword: 사업 유형 키워드 (예: "청소", "사전타당성조사", "기본계획")
        biz_type: 업무구분 (물품|공사|용역)
        inst_region: 기관 지역 필터 (예: "부산") — 생략 시 전국
        years: 분석 기간 (1~3년, 기본 3년)
    """
    months_back = min(years, 3) * 12  # 1년=12개월, 최대 3년=36개월

    all_items = await _fetch_bid_by_keyword(
        keyword, biz_type, inst_region, months_back=months_back
    )

    if not all_items:
        return {
            "keyword": keyword,
            "sample_count": 0,
            "recommendation": (
                f"'{keyword}' 관련 입찰공고 데이터가 없습니다.\n"
                "• 키워드를 짧게 줄여보세요 (예: '사전타당성' → '타당성')\n"
                "• 업무구분(biz_type)이 맞는지 확인하세요"
            ),
        }

    # 금액 파싱 (asignBdgtAmt: 배정예산, presmptPrce: 추정가격)
    amounts = []
    for item in all_items:
        amt = parse_amount(item.get("asignBdgtAmt") or item.get("presmptPrce") or 0)
        if amt > 1_000_000:  # 100만원 이상만 (이상치 제거)
            amounts.append(amt)

    if not amounts:
        return {
            "keyword": keyword,
            "sample_count": len(all_items),
            "recommendation": "공고는 있지만 예산금액 데이터가 없습니다.",
        }

    avg = int(mean(amounts))
    med = int(median(amounts))
    mn = min(amounts)
    mx = max(amounts)

    lower = int(med * 0.8)
    upper = int(med * 1.2)

    # 대표 사례 5개 (중앙값에 가까운 순)
    sorted_items = sorted(
        all_items,
        key=lambda x: abs(parse_amount(x.get("asignBdgtAmt") or x.get("presmptPrce") or 0) - med)
    )
    samples = []
    for item in sorted_items[:5]:
        amt = parse_amount(item.get("asignBdgtAmt") or item.get("presmptPrce") or 0)
        # 공고 상세 URL (API 제공 또는 직접 조합)
        detail_url = item.get("bidNtceUrl") or item.get("bidNtceDtlUrl") or ""
        samples.append({
            "공고명": item.get("bidNtceNm", ""),
            "발주기관": item.get("ntceInsttNm", ""),
            "계약방법": item.get("cntrctMthdNm", ""),
            "예산금액": format_amount(amt),
            "공고일": (item.get("bidNtceDt") or "")[:10],
            "공고상세URL": detail_url,
        })

    from datetime import datetime, timedelta
    now = datetime.now()
    year_end   = now.year
    year_start = (now - timedelta(days=365 * years)).year
    region_str = f" ({inst_region} 기준)" if inst_region else " (전국 기준)"

    recommendation = (
        f"📅 최근 {years}년({year_start}~{year_end})간의 데이터를 기준으로 분석한 결과입니다."
        f" 더 넓은 범위가 필요하시면 years 값을 늘려 요청해 주세요.\n\n"
        f"📊 '{keyword}' 유사사업{region_str} {len(amounts)}건 분석 결과\n"
        f"   데이터 출처: 나라장터 입찰공고 예산금액\n\n"
        f"• 평균 예산금액: {format_amount(avg)}\n"
        f"• 중앙값:        {format_amount(med)}\n"
        f"• 최소 ~ 최대:   {format_amount(mn)} ~ {format_amount(mx)}\n\n"
        f"💡 추천 기초금액 범위: {format_amount(lower)} ~ {format_amount(upper)}\n"
        f"   (중앙값 ±20%, 실제 사업 규모·조건에 따라 조정 필요)\n\n"
        f"⚠️ 이 금액은 입찰 예산금액 기준입니다. 실제 낙찰가는 이보다 낮을 수 있으며,\n"
        f"   최종 기초금액은 원가계산 또는 시장조사로 확정하세요."
    )

    return {
        "keyword": keyword,
        "sample_count": len(amounts),
        "avg_amount": format_amount(avg),
        "median_amount": format_amount(med),
        "min_amount": format_amount(mn),
        "max_amount": format_amount(mx),
        "recommended_range": {
            "lower": format_amount(lower),
            "upper": format_amount(upper),
        },
        "recommendation": recommendation,
        "representative_samples": samples,
    }


async def check_voluntary_contract(
    amount: int,
    biz_type: str = "용역",
    special_condition: Optional[str] = None,
) -> dict:
    """
    수의계약 가능 여부 기초 검토
    (지방계약법 시행령 제25조 기준)

    Args:
        amount: 계약 예정 금액 (원)
        biz_type: 업무구분 (물품|공사|용역)
        special_condition: 특수 조건 (예: "긴급", "특허", "소액")

    Returns:
        {
          "is_possible": bool,
          "legal_basis": str,
          "conditions": [...],
          "caution": str,
        }
    """
    # 지방계약법 시행령 제25조 소액 기준 (2024년 기준)
    thresholds = {
        "물품": 50_000_000,   # 5천만원 이하
        "용역": 50_000_000,   # 5천만원 이하
        "공사": 200_000_000,  # 2억원 이하
    }

    threshold = thresholds.get(biz_type, 50_000_000)
    fmt_amount = format_amount(amount)
    fmt_threshold = format_amount(threshold)

    if amount <= threshold:
        return {
            "is_possible": True,
            "legal_basis": "지방계약법 시행령 제25조 제1항 제5호 (소액 수의계약)",
            "amount_check": f"{fmt_amount} ≤ {biz_type} 수의계약 한도 {fmt_threshold} ✅",
            "conditions": [
                f"{biz_type} 계약 예정금액 {fmt_threshold} 이하",
                "2인 이상 견적서 징구 필요 (단, 5백만원 이하는 1인 가능)",
                "동일인과의 연간 누적 수의계약 한도 초과 여부 확인 필요",
            ],
            "caution": (
                "⚠️ 주의사항\n"
                "• 정당한 이유 없이 경쟁입찰을 피하기 위해 사업을 분할하면 안 됩니다.\n"
                "• 연간 동일업체 누적 금액이 한도를 초과하지 않도록 관리하세요.\n"
                "• 최종 판단은 반드시 법제처 법령 해석 및 소관 부서와 협의하세요."
            ),
        }
    else:
        conditions = []
        possible = False

        if special_condition == "긴급":
            possible = True
            conditions = [
                "재해·재난 등 긴급한 필요 (시행령 제25조 제1항 제3호)",
                "긴급성 사유를 구체적으로 기안에 명시해야 함",
            ]
        elif special_condition == "특허":
            possible = True
            conditions = [
                "특허·실용신안 등록된 물품/공법 (시행령 제25조 제1항 제2호)",
                "특허권자 확인 및 유일 공급자 증빙 필요",
            ]

        if possible:
            basis = "지방계약법 시행령 제25조 (특수 사유에 의한 수의계약)"
        else:
            basis = "경쟁입찰 실시 필요 (금액 초과)"

        return {
            "is_possible": possible,
            "legal_basis": basis,
            "amount_check": f"{fmt_amount} > {biz_type} 수의계약 한도 {fmt_threshold} ❌ (금액 초과)",
            "conditions": conditions if conditions else [
                f"경쟁입찰 실시 필요",
                "일반경쟁 또는 제한경쟁 입찰 절차를 따르세요",
                "특수 사유가 있다면 special_condition 파라미터를 사용하세요",
            ],
            "caution": (
                "⚠️ 이 금액은 소액 수의계약 한도를 초과합니다.\n"
                "특수한 사유(긴급, 특허, 단일브랜드 등)가 없다면 경쟁입찰을 실시해야 합니다.\n"
                "법적 근거 없는 수의계약은 감사 지적 사항이 될 수 있습니다."
            ),
        }
