"""
입찰공고정보서비스 도구 모음
- search_bid_announcements: 키워드 서버사이드 검색 (PPSSrch + 월별 분할 + 자동 키워드 변형)
- search_bid_by_date: 날짜 기반 검색 (기존 방식, 빠름)
"""
import re
import calendar
import asyncio
from typing import Optional
from config import ENDPOINTS
from tools.api_client import fetch, parse_amount, format_amount


# ─────────────────────────────────────────────
# 키워드 자동 변형
# ─────────────────────────────────────────────

def _generate_keyword_variants(keyword: str) -> list[str]:
    """
    나라장터 공고명 표기 불일치 대응 — 자동 변형 키워드 생성

    나라장터는 기관마다 표기가 제각각이라 동일 사업도 검색어가 다르면 누락됩니다.

    변형 규칙:
      - 공백 제거: '주차 수급' → '주차수급'
      - 장 삽입/삭제: '주차수급' ↔ '주차장수급' (수급 앞 '장' 누락 빈번)
      - 동의어: 유지보수↔유지관리, 소프트웨어↔SW, 실태조사↔수급실태조사
      - 기관 약칭: 부산교통공사↔부산도시철도, 부산시↔부산광역시
    """
    kw = keyword.strip()
    variants: list[str] = [kw]

    # 1. 공백 제거
    no_space = kw.replace(" ", "")
    if no_space != kw:
        variants.append(no_space)

    # 2. "수급" 앞 "장" 삽입/삭제
    #    한글 자음+수급 → 장수급 (주차수급 → 주차장수급)
    if re.search(r'[가-힣]수급', kw) and "장수급" not in kw:
        variants.append(kw.replace("수급", "장수급"))
    if "장수급" in kw:
        variants.append(kw.replace("장수급", "수급"))

    # 3. 동의어 쌍 (a ↔ b)
    synonym_pairs = [
        ("유지보수",  "유지관리"),
        ("소프트웨어", "SW"),
        ("S/W",      "소프트웨어"),
        ("실태조사",  "수급실태조사"),
        ("수급실태조사", "실태조사"),
        ("기본계획수립", "기본계획"),
        ("안전점검",  "정기점검"),
        ("도시철도",  "도시철도운영"),
    ]
    for a, b in synonym_pairs:
        if a in kw and b not in kw:
            variants.append(kw.replace(a, b))
        elif b in kw and a not in kw:
            variants.append(kw.replace(b, a))

    # 4. "용역" 접미사 추가 (없는 경우만)
    #    '주차장수급' → '주차장수급 용역' 은 너무 공격적 → 생략
    #    단, 키워드가 명사형으로 끝나는 경우만
    if (not kw.endswith("용역") and not kw.endswith("사업")
            and not kw.endswith("공사") and len(kw) >= 4):
        variants.append(kw + " 용역")

    # 중복 제거, 순서 유지, 최대 6개
    seen: set[str] = set()
    result: list[str] = []
    for v in variants:
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result[:6]


# ─────────────────────────────────────────────
# 내부 헬퍼
# ─────────────────────────────────────────────

def _build_bid_item(item: dict) -> dict:
    """API 응답 → 정돈된 딕셔너리"""
    amt_raw = parse_amount(item.get("asignBdgtAmt") or item.get("presmptPrce") or 0)

    attachments = []
    for seq in range(1, 6):
        url  = item.get(f"ntceSpecDocUrl{seq}", "")
        name = item.get(f"ntceSpecFileNm{seq}", "")
        if url and name:
            attachments.append({"파일명": name, "다운로드URL": url})

    bid_no  = item.get("bidNtceNo", "")
    bid_ord = item.get("bidNtceOrd", "000")

    # 상세 URL: API 제공값 우선, 없으면 공고번호로 직접 구성
    detail_url = (
        item.get("bidNtceUrl")
        or item.get("bidNtceDtlUrl")
        or (f"https://www.g2b.go.kr/link/PNPE027_01/single/"
            f"?bidPbancNo={bid_no}&bidPbancOrd={bid_ord}" if bid_no else "")
    )

    return {
        "공고번호":      bid_no,
        "공고차수":      bid_ord,
        "공고명":        item.get("bidNtceNm", ""),
        "발주기관":      item.get("ntceInsttNm", ""),
        "수요기관":      item.get("dminsttNm", ""),
        "계약방법":      item.get("cntrctMthdNm", ""),
        "낙찰방법":      item.get("sucsfbidMthdNm", ""),
        "공고일":        (item.get("bidNtceDt") or "")[:10],
        "입찰마감":      (item.get("bidClseDt") or "")[:16],
        "예산금액":      format_amount(amt_raw),
        "예산금액_원":   amt_raw,
        "공고상세URL":   detail_url,
        "낙찰결과URL":   (f"https://www.g2b.go.kr/link/PNPE027_01/single/"
                          f"?bidPbancNo={bid_no}&bidPbancOrd={bid_ord}"
                          f"#bidResult" if bid_no else ""),
        "첨부파일":      attachments,
        "매칭키워드":    item.get("_matched_keyword", ""),
    }


async def _search_single_keyword(
    keyword: str,
    op: str,
    months_back: int,
) -> list[dict]:
    """단일 키워드 + 월별 분할 PPSSrch (내부 헬퍼)"""
    from datetime import datetime, timedelta

    now = datetime.now()
    all_items: list[dict] = []
    seen_ids: set[str] = set()

    for m in range(min(months_back, 30)):
        target = now.replace(day=1) - timedelta(days=1)
        for _ in range(m):
            target = target.replace(day=1) - timedelta(days=1)
        year, month = target.year, target.month
        last_day = calendar.monthrange(year, month)[1]

        params = {
            "numOfRows": 100,
            "bidNtceNm": keyword,
            "inqryDiv":  "1",
            "inqryBgnDt": f"{year:04d}{month:02d}010000",
            "inqryEndDt": f"{year:04d}{month:02d}{last_day:02d}2359",
        }
        try:
            result = await fetch(ENDPOINTS["bid"], op, params)
            for item in result.get("items", []):
                uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
                if uid and uid not in seen_ids:
                    seen_ids.add(uid)
                    item["_matched_keyword"] = keyword
                    all_items.append(item)
        except Exception:
            continue

    return all_items


# ─────────────────────────────────────────────
# 공개 도구
# ─────────────────────────────────────────────

async def search_bid_announcements(
    keyword: Optional[str] = None,
    inst_name: Optional[str] = None,
    biz_type: str = "용역",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_amount: Optional[int] = None,
    max_amount: Optional[int] = None,
    months_back: int = 24,
    page_size: int = 50,
) -> dict:
    """
    나라장터 입찰공고 검색

    키워드가 있으면 PPSSrch(서버사이드 검색) + 월별 분할 + 자동 키워드 변형을 사용합니다.
    나라장터는 기관마다 표기가 달라 동일 사업도 '주차수급'/'주차장수급'처럼 다르게 기재됩니다.
    자동 변형 덕분에 하나의 키워드로 관련 공고를 폭넓게 수집합니다.

    키워드가 없으면 날짜 기반 고속 검색을 사용합니다.

    Args:
        keyword: 공고명 키워드 (예: '도시철도', '주차수급', '기본계획')
                 자동으로 변형 키워드 생성 (주차수급→주차장수급 등)
        inst_name: 발주기관명 필터 (예: '부산', '서울')
        biz_type: 업무구분 (물품|공사|용역|외자)
        start_date: 날짜 기반 검색 시작일 (YYYYMMDDHHMM) — keyword 없을 때 사용
        end_date: 날짜 기반 검색 종료일 (YYYYMMDDHHMM)
        min_amount: 최소 예산금액 (원)
        max_amount: 최대 예산금액 (원)
        months_back: PPSSrch 소급 개월 수 (기본 24개월)
        page_size: 날짜 기반 검색 결과 수 (PPSSrch는 월 100건 고정)
    """
    if keyword:
        return await _search_by_keyword(
            keyword=keyword,
            inst_name=inst_name,
            biz_type=biz_type,
            min_amount=min_amount,
            max_amount=max_amount,
            months_back=months_back,
        )
    else:
        return await _search_by_date(
            inst_name=inst_name,
            biz_type=biz_type,
            start_date=start_date,
            end_date=end_date,
            min_amount=min_amount,
            max_amount=max_amount,
            page_size=page_size,
        )


async def _search_by_keyword(
    keyword: str,
    inst_name: Optional[str],
    biz_type: str,
    min_amount: Optional[int],
    max_amount: Optional[int],
    months_back: int,
) -> dict:
    """
    PPSSrch 기반 키워드 검색
    — 자동 변형 키워드를 병렬로 검색 후 중복 제거하여 합산
    """
    ops = {
        "물품": "getBidPblancListInfoThngPPSSrch",
        "공사": "getBidPblancListInfoCnstwkPPSSrch",
        "용역": "getBidPblancListInfoServcPPSSrch",
        "외자": "getBidPblancListInfoFrgcptPPSSrch",
    }
    op = ops.get(biz_type, "getBidPblancListInfoServcPPSSrch")

    # 자동 변형 키워드 생성
    variants = _generate_keyword_variants(keyword)

    # 모든 변형을 병렬 검색
    tasks = [_search_single_keyword(v, op, months_back) for v in variants]
    per_variant = await asyncio.gather(*tasks, return_exceptions=True)

    # 결과 합산 — bidNtceNo+bidNtceOrd 기준 중복 제거
    all_items_map: dict[str, dict] = {}
    variant_stats: list[str] = []
    for variant, result in zip(variants, per_variant):
        if isinstance(result, list) and result:
            new_count = 0
            for item in result:
                uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
                if uid and uid not in all_items_map:
                    all_items_map[uid] = item
                    new_count += 1
            variant_stats.append(f"'{variant}':{new_count}건")
        elif isinstance(result, list):
            variant_stats.append(f"'{variant}':0건")

    all_items = list(all_items_map.values())

    # 기관명 필터
    if inst_name:
        all_items = [
            i for i in all_items
            if inst_name in (i.get("ntceInsttNm") or "")
            or inst_name in (i.get("dminsttNm") or "")
        ]

    # 금액 필터
    if min_amount is not None or max_amount is not None:
        filtered = []
        for i in all_items:
            amt = parse_amount(i.get("asignBdgtAmt") or i.get("presmptPrce") or 0)
            if min_amount is not None and amt < min_amount: continue
            if max_amount is not None and amt > max_amount: continue
            filtered.append(i)
        all_items = filtered

    # 날짜 내림차순 정렬
    all_items.sort(key=lambda x: (x.get("bidNtceDt") or ""), reverse=True)

    simplified = [_build_bid_item(i) for i in all_items]

    keyword_summary = " | ".join(variant_stats) if variant_stats else "검색 결과 없음"
    return {
        "totalCount":   len(simplified),
        "matchedCount": len(simplified),
        "items":        simplified,
        "keyword_variants_tried": variants,
        "variant_stats": keyword_summary,
    }


async def _search_by_date(
    inst_name: Optional[str],
    biz_type: str,
    start_date: Optional[str],
    end_date: Optional[str],
    min_amount: Optional[int],
    max_amount: Optional[int],
    page_size: int,
) -> dict:
    """날짜 기반 고속 검색 (기본 최근 3개월)"""
    from datetime import datetime, timedelta

    ops = {
        "물품": "getBidPblancListInfoThng",
        "공사": "getBidPblancListInfoCnstwk",
        "용역": "getBidPblancListInfoServc",
        "외자": "getBidPblancListInfoFrgcpt",
    }
    op = ops.get(biz_type, "getBidPblancListInfoServc")

    now = datetime.now()
    if not start_date:
        start_date = (now - timedelta(days=90)).strftime("%Y%m%d0000")
    if not end_date:
        end_date = now.strftime("%Y%m%d2359")

    params = {
        "numOfRows": page_size,
        "inqryDiv":  "1",
        "inqryBgnDt": start_date,
        "inqryEndDt": end_date,
    }
    result = await fetch(ENDPOINTS["bid"], op, params)
    items = result["items"]

    if inst_name:
        items = [i for i in items if inst_name in (i.get("ntceInsttNm") or "")]
    if min_amount is not None or max_amount is not None:
        filtered = []
        for i in items:
            amt = parse_amount(i.get("asignBdgtAmt") or i.get("presmptPrce") or 0)
            if min_amount is not None and amt < min_amount: continue
            if max_amount is not None and amt > max_amount: continue
            filtered.append(i)
        items = filtered

    simplified = [_build_bid_item(i) for i in items]
    return {
        "totalCount":   result["totalCount"],
        "matchedCount": len(simplified),
        "items":        simplified,
    }
