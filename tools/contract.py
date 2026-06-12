"""
계약정보서비스 도구 모음
- search_contracts: 계약 목록 검색
- analyze_price_benchmark: 유사사업 낙찰가 분석 → 적정 기초금액 제안
- get_contract_detail: 통합계약번호로 상세 조회
- fetch_voluntary_contracts: 수의계약 데이터 조회 (vendor.py 통합용)
"""
import asyncio
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
    from datetime import datetime, timedelta

    # 업무구분별 오퍼레이션 매핑
    ops = {
        "물품": "getCntrctInfoListThng",
        "공사": "getCntrctInfoListCnstwk",
        "용역": "getCntrctInfoListServc",
        "외자": "getCntrctInfoListFrgcpt",
    }
    # 서버사이드 검색 오퍼레이션 (계약명·기관명 검색 지원, 조회기간 1개월 제한)
    pps_ops = {
        "물품": "getCntrctInfoListThngPPSSrch",
        "공사": "getCntrctInfoListCnstwkPPSSrch",
        "용역": "getCntrctInfoListServcPPSSrch",
        "외자": "getCntrctInfoListFrgcptPPSSrch",
    }

    now = datetime.now()
    if not (start_date and end_date):
        # 기본: 최근 3개월
        start_date = (now - timedelta(days=90)).strftime("%Y%m%d0000")
        end_date = now.strftime("%Y%m%d2359")

    if keyword or inst_name:
        # ── 서버사이드 검색 (PPSSrch) — 월 단위 분할 병렬 조회 ──
        operation = pps_ops.get(biz_type, "getCntrctInfoListServcPPSSrch")
        bgn = datetime.strptime(start_date[:8], "%Y%m%d")
        end = datetime.strptime(end_date[:8], "%Y%m%d")
        windows: list[tuple[str, str]] = []
        cur = bgn
        while cur <= end:
            nxt = min(cur + timedelta(days=29), end)
            windows.append((cur.strftime("%Y%m%d"), nxt.strftime("%Y%m%d")))
            cur = nxt + timedelta(days=1)

        async def _fetch_window(w_bgn: str, w_end: str) -> dict:
            params: dict = {
                "numOfRows": 999,
                "inqryDiv": "1",
                "inqryBgnDate": w_bgn,
                "inqryEndDate": w_end,
            }
            if keyword:
                params["cntrctNm"] = keyword
            if inst_name:
                params["insttNm"] = inst_name
            return await fetch(ENDPOINTS["contract"], operation, params)

        results = await asyncio.gather(*[_fetch_window(*w) for w in windows])

        total_count = sum(r["totalCount"] for r in results)
        items = []
        seen_ids: set[str] = set()
        for r in results:
            for item in r["items"]:
                uid = item.get("untyCntrctNo") or item.get("cntrctNo") or ""
                if not uid or uid not in seen_ids:
                    seen_ids.add(uid)
                    items.append(item)
    else:
        # ── 날짜 기반 목록 조회 ──
        operation = ops.get(biz_type, "getCntrctInfoListServc")
        params = {
            "numOfRows": page_size,
            "inqryDiv": "1",
            "inqryBgnDt": start_date,
            "inqryEndDt": end_date,
        }
        result = await fetch(ENDPOINTS["contract"], operation, params)
        items = result["items"]
        total_count = result["totalCount"]

    # 계약방법 필드명 정규화 (PPSSrch: cntrctCnclsMthdNm / 목록조회: cntrctMthdNm)
    for item in items:
        if not item.get("cntrctMthdNm") and item.get("cntrctCnclsMthdNm"):
            item["cntrctMthdNm"] = item["cntrctCnclsMthdNm"]

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
        "totalCount": total_count,
        "matchedCount": len(items),
        "items": items[:max(page_size, 20)],
    }


async def _fetch_bid_by_keyword(
    keyword: str,
    biz_type: str,
    inst_region: Optional[str],
    months_back: int = 24,
) -> list:
    """
    입찰공고 PPSSrch + 월별 분할 병렬 쿼리 (연간 쿼리는 API 내부 타임아웃으로 빈 응답)
    → 예산금액(asignBdgtAmt)을 계약금액 대리지표로 사용
    """
    from tools.api_client import ApiKeyError, month_ranges

    ops = {
        "물품": "getBidPblancListInfoThngPPSSrch",
        "공사": "getBidPblancListInfoCnstwkPPSSrch",
        "용역": "getBidPblancListInfoServcPPSSrch",
    }
    op = ops.get(biz_type, "getBidPblancListInfoServcPPSSrch")

    async def _fetch_month(year: int, month: int, last_day: int) -> list:
        params: dict = {
            "numOfRows": 100,
            "bidNtceNm": keyword,
            "inqryDiv": "1",
            "inqryBgnDt": f"{year:04d}{month:02d}010000",
            "inqryEndDt": f"{year:04d}{month:02d}{last_day:02d}2359",
        }
        try:
            result = await fetch(ENDPOINTS["bid"], op, params)
            return result.get("items", [])
        except ApiKeyError:
            raise
        except Exception:
            return []  # 해당 월 오류 시 스킵

    results = await asyncio.gather(
        *[_fetch_month(*r) for r in month_ranges(months_back, cap=36)]
    )

    all_items = []
    seen_ids = set()
    for items in results:
        for item in items:
            uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                all_items.append(item)

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
    수의계약 가능 여부 실시간 검토
    - 법제처 실시간 법령 조문을 근거로 '법령상 가능 여부' 판단
    - 부산광역시 조례상 적정성 병행 검토
    - API 실패 시 기준 규정값으로 안전하게 폴백

    Args:
        amount:            계약 예정 금액 (원)
        biz_type:          업무구분 (물품|공사|용역)
        special_condition: 특수 조건 (예: "긴급", "특허", "소액")
    """
    from tools.legal import SOURCE_BADGE

    # ── 기준값 (지방계약법 시행령 제25조, 2024년 기준) ──
    # 공사 한도는 종합공사 기준 — 전문공사·그 밖의 공사는 한도가 다르므로 주의 문구로 안내
    THRESHOLDS = {
        "물품": 50_000_000,
        "용역": 50_000_000,
        "공사": 200_000_000,
    }
    threshold = THRESHOLDS.get(biz_type, 50_000_000)
    fmt_amount    = format_amount(amount)
    fmt_threshold = format_amount(threshold)

    # ── 법제처 실시간 조회 (비동기 병렬) ──
    law_cite   = ""
    ordin_cite = ""
    law_link   = "https://www.law.go.kr"
    ordin_link = ""

    ordin_articles: list[str] = []

    try:
        from tools.law_client import law_search, search_busan_ordinance, law_content
        # '지방계약'으로 검색 → 지방계약법 + 시행령이 상위에 노출됨
        law_task   = law_search(query="지방계약", target="law",   display=5)
        ordin_task = search_busan_ordinance(query="수의계약", display=5)
        law_res, ordin_res = await asyncio.gather(law_task, ordin_task, return_exceptions=True)

        if not isinstance(law_res, Exception) and law_res.get("items"):
            # 시행령 우선 (제25조 수의계약 조항이 시행령에 있음)
            items = law_res["items"]
            령 = next((i for i in items if "시행령" in i.get("name", "")), None)
            top = 령 or items[0]
            law_cite  = top.get("name", "")
            law_link  = top.get("link", law_link)

        if not isinstance(ordin_res, Exception) and ordin_res.get("items"):
            top_ordin = ordin_res["items"][0]
            ordin_cite = top_ordin.get("name", "")
            ordin_link = top_ordin.get("link", "")
            # 조례 본문 조회 → 수의계약 관련 조문 발췌
            ordin_serial = str(top_ordin.get("id", ""))
            if ordin_serial:
                try:
                    content = await law_content(ordin_serial, "ordin")
                    search_kw = ["수의계약", "계약", str(threshold // 10000) + "만원"]
                    for art in content.get("articles", []):
                        if any(kw in art.get("text", "") for kw in search_kw):
                            ordin_articles.append(art["text"][:300])
                        if len(ordin_articles) >= 3:
                            break
                except Exception:
                    pass
    except Exception:
        pass  # API 장애 시 기준값으로 진행

    law_basis_text = (
        f"지방계약법 시행령 제25조 제1항 제5호 (소액 수의계약)"
        + (f"\n   🔗 {law_link}" if law_link else "")
        + (f"\n   📗 {law_cite}" if law_cite else "")
    )

    if ordin_articles:
        ordin_art_text = "\n".join(f"   • {a}" for a in ordin_articles)
        busan_ordin_text = (
            f"📙 【{ordin_cite or '부산시 조례'}】 수의계약 관련 조문:\n"
            + ordin_art_text
            + (f"\n   🔗 {ordin_link}" if ordin_link else "")
        )
    else:
        busan_ordin_text = (
            "부산광역시 계약 조례 — 지역업체 우선 구매 조항 확인 필요"
            + (f"\n   📙 {ordin_cite}" if ordin_cite else "")
            + (f"\n   🔗 {ordin_link}" if ordin_link else "")
        )

    # ── 판정 ──
    if amount <= threshold:
        result = {
            "is_possible": True,
            "legal_basis": law_basis_text,
            "amount_check": f"{fmt_amount} ≤ {biz_type} 수의계약 한도 {fmt_threshold} ✅ (법령상 가능)",
            "busan_ordinance_check": busan_ordin_text,
            "conditions": [
                f"{biz_type} 계약 예정금액 {fmt_threshold} 이하 — 법령상 수의계약 허용",
                "2인 이상 견적서 징구 필요 (단, 5백만원 이하는 1인 가능)",
                "동일업체 연간 누적 수의계약 한도 초과 여부 확인 필요",
                "부산시 계약 조례 상 지역업체 우선 구매 적용 여부 검토",
            ],
            "caution": (
                "⚠️ 주의사항\n"
                "• 정당한 이유 없이 경쟁입찰 회피 목적의 사업 분할 금지\n"
                "• 연간 동일업체 누적 금액이 한도를 초과하지 않도록 관리하세요.\n"
                "• 부산시 조례상 지역업체 우선 구매 대상인 경우 지역업체 견적 우선 징구\n"
                "• 공사 한도는 종합공사 기준입니다 — 전문공사·그 밖의 공사는 한도가 다르며,\n"
                "  여성기업·장애인기업 등과의 계약은 별도 특례 한도가 적용될 수 있습니다.\n"
                "• 한도 금액은 시행령 개정으로 변동될 수 있으니 시행령 제25조 원문을 확인하세요.\n"
                "• 최종 판단은 법제처 법령 해석 및 소관 부서와 협의하세요."
            ),
        }

    else:
        possible = False
        conditions = []

        if special_condition == "긴급":
            possible = True
            conditions = [
                "재해·재난 등 긴급한 필요 (시행령 제25조 제1항 제3호)",
                "긴급성 사유를 구체적으로 기안에 명시 필요",
                "사후 감사 대비 긴급성 증빙 서류 보존",
            ]
        elif special_condition == "특허":
            possible = True
            conditions = [
                "특허·실용신안 등록된 물품/공법 (시행령 제25조 제1항 제2호)",
                "특허권자 확인 및 유일 공급자 증빙 서류 필요",
            ]

        result = {
            "is_possible": possible,
            "legal_basis": (
                "지방계약법 시행령 제25조 (특수 사유 수의계약)" if possible
                else "경쟁입찰 실시 필요 — 금액 한도 초과 (법령상 불가)"
            ) + (f"\n   🔗 {law_link}" if law_link else ""),
            "amount_check": f"{fmt_amount} > {biz_type} 수의계약 한도 {fmt_threshold} ❌ (금액 초과)",
            "busan_ordinance_check": busan_ordin_text,
            "conditions": conditions if conditions else [
                "경쟁입찰 실시 필요 (일반경쟁 또는 제한경쟁)",
                "특수 사유(긴급·특허·단일브랜드)가 있다면 special_condition 파라미터를 사용하세요",
            ],
            "caution": (
                "⚠️ 이 금액은 소액 수의계약 한도를 초과합니다.\n"
                "특수한 사유가 없다면 경쟁입찰을 실시해야 합니다.\n"
                "법적 근거 없는 수의계약은 감사 지적 대상이 될 수 있습니다."
            ),
        }

    result["source"] = SOURCE_BADGE.strip()
    return result


# ──────────────────────────────────────────────────────────────
# 수의계약 데이터 조회 (vendor.py 통합 랭킹용)
# ──────────────────────────────────────────────────────────────

async def _fetch_voluntary_monthly(
    biz_type: str,
    months_back: int,
    keyword: Optional[str] = None,
    region: Optional[str] = None,
) -> list[dict]:
    """
    getCntrctInfoListServc 월별 병렬 조회 후 수의계약만 반환.
    cntrctMthdNm 필드에 '수의' 포함 여부로 필터링.
    """
    from tools.api_client import ApiKeyError, month_ranges

    ops = {
        "물품": "getCntrctInfoListThng",
        "공사": "getCntrctInfoListCnstwk",
        "용역": "getCntrctInfoListServc",
        "외자": "getCntrctInfoListFrgcpt",
    }
    op = ops.get(biz_type, "getCntrctInfoListServc")

    async def _fetch_one(year: int, month: int, last_day: int) -> list[dict]:
        params: dict = {
            "numOfRows": 100,
            "inqryDiv": "1",
            "inqryBgnDt": f"{year:04d}{month:02d}010000",
            "inqryEndDt": f"{year:04d}{month:02d}{last_day:02d}2359",
        }
        try:
            result = await fetch(ENDPOINTS["contract"], op, params)
            items = result.get("items", [])
            return [i for i in items if "수의" in (i.get("cntrctMthdNm") or "")]
        except ApiKeyError:
            raise
        except Exception:
            return []

    results = await asyncio.gather(
        *[_fetch_one(*r) for r in month_ranges(months_back)]
    )

    all_items: list[dict] = []
    seen_ids: set[str] = set()
    for items in results:
        for item in items:
            uid = item.get("untyCntrctNo") or item.get("cntrctNo") or ""
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                all_items.append(item)
            elif not uid:
                all_items.append(item)

    # 기관/키워드 필터
    if region:
        all_items = [i for i in all_items if region in (i.get("cntrctInsttNm") or "")]
    if keyword:
        kw = keyword.lower()
        all_items = [
            i for i in all_items
            if kw in (i.get("cntrctNm") or "").lower()
            or kw in (i.get("prdctClsfcNoNm") or "").lower()
        ]

    return all_items


async def fetch_voluntary_contracts(
    keyword: Optional[str] = None,
    biz_type: str = "용역",
    months_back: int = 24,
    region: Optional[str] = None,
) -> list[dict]:
    """
    수의계약 데이터 조회 → 업체별 집계 반환 (vendor.py 통합 랭킹용)

    Args:
        keyword:     계약건명 키워드 (생략 시 전체)
        biz_type:    업무구분 (물품|공사|용역|외자)
        months_back: 소급 개월 수 (기본 24, 최대 48)
        region:      기관명 지역 필터 (예: '부산')

    Returns:
        [{업체명, 사업자번호, 주소, 수의계약횟수, 수의금액합계, 최근계약일,
          발주기관목록, is_local}, ...]
    """
    items = await _fetch_voluntary_monthly(biz_type, months_back, keyword, region)

    company_map: dict[str, dict] = {}
    for item in items:
        # 계약상대자명 — API 필드명 다중 시도 (나라장터 응답 규격 편차)
        corp = (
            item.get("cntrctorNm")
            or item.get("cntrctCntrctorNm")
            or item.get("sucsfbidCrpNm")
            or item.get("bidwinnrNm")
            or ""
        )
        biz_no = (
            item.get("cntrctorBizno")
            or item.get("cntrctBizno")
            or item.get("bizno")
            or item.get("bidwinnrBizno")
            or ""
        )
        addr = item.get("cntrctorAdrs") or item.get("adrs") or ""
        amt = parse_amount(
            item.get("thtmCntrctAmt") or item.get("totCntrctAmt") or 0
        )
        date = (item.get("cntrctCnclsDate") or "")[:10]
        inst = item.get("cntrctInsttNm") or ""

        if not corp:
            continue

        key = biz_no or corp
        if key not in company_map:
            company_map[key] = {
                "업체명":       corp,
                "사업자번호":   biz_no,
                "주소":         addr,
                "수의계약횟수": 0,
                "수의금액합계": 0,
                "최근계약일":   "",
                "발주기관목록": [],
                "is_local":    (region in addr) if (region and addr) else (not region),
            }
        c = company_map[key]
        c["수의계약횟수"] += 1
        c["수의금액합계"] += amt
        if inst and inst not in c["발주기관목록"]:
            c["발주기관목록"].append(inst)
        if date > c["최근계약일"]:
            c["최근계약일"] = date
            if addr:
                c["주소"] = addr

    return list(company_map.values())
