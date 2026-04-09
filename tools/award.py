"""
낙찰정보서비스 + 계약과정통합공개서비스 도구 모음

- get_bid_award_result  : 입찰 낙찰결과 조회 (키워드 or 공고번호)
- get_contract_process  : 공고번호로 전체 계약 과정 조회 (사전규격→낙찰→계약)
"""
import asyncio
import calendar
from typing import Optional
from config import ENDPOINTS
from tools.api_client import fetch, parse_amount, format_amount


# ─────────────────────────────────────────────
# 내부 헬퍼
# ─────────────────────────────────────────────

def _build_award_item(item: dict) -> dict:
    """낙찰현황 API 응답 → 정돈된 딕셔너리

    낙찰정보서비스(ScsbidInfoService) 실제 필드명:
      bidwinnrNm     = 낙찰업체명
      bidwinnrBizno  = 사업자등록번호
      sucsfbidAmt    = 낙찰금액
      sucsfbidRate   = 낙찰률
      rlOpengDt      = 실개찰일시
      prtcptCnum     = 참가업체수
      dminsttNm      = 수요기관
    """
    bid_no  = item.get("bidNtceNo", "")
    bid_ord = item.get("bidNtceOrd", "000")

    amt_raw = parse_amount(
        item.get("sucsfbidAmt") or item.get("scsbidAmt") or
        item.get("opengAmt") or item.get("presmptPrce") or 0
    )
    corp_name = (
        item.get("bidwinnrNm")
        or item.get("scsbidCorpNm")
        or item.get("opengCorpNm")
        or item.get("sucsfbidCrpNm")
        or ""
    )
    biz_no = (
        item.get("bidwinnrBizno")
        or item.get("bizno")
        or item.get("scsbidBizno")
        or ""
    )
    open_dt = (
        item.get("rlOpengDt") or item.get("opengDt") or item.get("scsbidDt") or ""
    )[:16]
    prtc = (
        item.get("prtcptCnum") or item.get("prtcmpntNum") or
        item.get("bidPrtcmpntNum") or ""
    )
    rate = (
        item.get("sucsfbidRate") or item.get("scsbidRate") or ""
    )
    inst = (
        item.get("ntceInsttNm") or item.get("dminsttNm") or ""
    )

    return {
        "공고번호":    bid_no,
        "공고차수":    bid_ord,
        "공고명":      item.get("bidNtceNm", ""),
        "발주기관":    inst,
        "낙찰업체":    corp_name,
        "사업자번호":  biz_no,
        "낙찰금액":    format_amount(amt_raw),
        "낙찰금액_원": amt_raw,
        "낙찰률":      rate,
        "개찰일시":    open_dt,
        "참가업체수":  prtc,
        "입찰공고URL": (
            f"https://www.g2b.go.kr/link/PNPE027_01/single/"
            f"?bidPbancNo={bid_no}&bidPbancOrd={bid_ord}" if bid_no else ""
        ),
    }


async def _award_search_monthly(
    op: str,
    keyword: Optional[str],
    bid_no: Optional[str],
    months_back: int,
) -> list[dict]:
    """월별 분할 낙찰현황 조회"""
    from datetime import datetime, timedelta

    now = datetime.now()
    all_items: list[dict] = []
    seen_ids: set[str] = set()

    for m in range(min(months_back, 48)):
        target = now.replace(day=1) - timedelta(days=1)
        for _ in range(m):
            target = target.replace(day=1) - timedelta(days=1)
        year, month = target.year, target.month
        last_day = calendar.monthrange(year, month)[1]

        params: dict = {
            "numOfRows": 100,
            "inqryDiv":  "1",
            "inqryBgnDt": f"{year:04d}{month:02d}010000",
            "inqryEndDt": f"{year:04d}{month:02d}{last_day:02d}2359",
        }
        if keyword:
            params["bidNtceNm"] = keyword
        if bid_no:
            params["bidNtceNo"] = bid_no

        try:
            result = await fetch(ENDPOINTS["award"], op, params)
            for item in result.get("items", []):
                uid = item.get("bidNtceNo", "") + item.get("bidNtceOrd", "")
                if uid and uid not in seen_ids:
                    seen_ids.add(uid)
                    all_items.append(item)
        except Exception:
            continue

    return all_items


# ─────────────────────────────────────────────
# 공개 도구 1 — 낙찰결과 조회
# ─────────────────────────────────────────────

async def get_bid_award_result(
    keyword: Optional[str] = None,
    bid_no: Optional[str] = None,
    biz_type: str = "용역",
    inst_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    months_back: int = 36,
) -> dict:
    """
    나라장터 낙찰결과 조회 (낙찰정보서비스)

    키워드나 공고번호로 낙찰 업체명, 낙찰금액, 낙찰률, 참가업체수를 확인합니다.
    '이 공고 누가 낙찰받았어?' / '2023년 주차수급 용역 낙찰업체가 어디야?' 에 답합니다.

    Args:
        keyword:     공고명 키워드 (예: '주차수급', '환경미화')
        bid_no:      입찰공고번호 (예: '20230311048') — 특정 공고 조회 시 사용
        biz_type:    업무구분 (물품|공사|용역|외자)
        inst_name:   발주기관 필터 (예: '부산', '연제구')
        start_date:  조회 시작일 (YYYYMMDDHHMM)
        end_date:    조회 종료일 (YYYYMMDDHHMM)
        months_back: PPSSrch 소급 개월 수 (기본 36)
    """
    # 업무 구분별 오퍼레이션
    pp_ops = {
        "물품": "getScsbidListSttusThngPPSSrch",
        "공사": "getScsbidListSttusCnstwkPPSSrch",
        "용역": "getScsbidListSttusServcPPSSrch",
        "외자": "getScsbidListSttusFrgcptPPSSrch",
    }
    plain_op = "getScsbidListSttusServc"  # 공고번호 직접 조회용

    if bid_no and not keyword:
        # 공고번호 직접 조회 (날짜 범위 기반)
        from datetime import datetime, timedelta

        now = datetime.now()
        if not start_date:
            start_date = (now - timedelta(days=365 * 4)).strftime("%Y%m%d0000")
        if not end_date:
            end_date = now.strftime("%Y%m%d2359")

        params = {
            "numOfRows": 10,
            "inqryDiv":  "1",
            "bidNtceNo": bid_no,
            "inqryBgnDt": start_date,
            "inqryEndDt": end_date,
        }
        try:
            result = await fetch(ENDPOINTS["award"], plain_op, params)
            items = result.get("items", [])
        except Exception as e:
            return {"error": str(e), "items": []}
    else:
        # 키워드 PPSSrch (월별 분할)
        op = pp_ops.get(biz_type, "getScsbidListSttusServcPPSSrch")
        items = await _award_search_monthly(op, keyword, bid_no, months_back)

    # 기관 필터
    if inst_name:
        items = [i for i in items if inst_name in (i.get("ntceInsttNm") or "")]

    items.sort(key=lambda x: (x.get("opengDt") or x.get("scsbidDt") or ""), reverse=True)
    simplified = [_build_award_item(i) for i in items]

    return {
        "totalCount": len(simplified),
        "items": simplified,
    }


# ─────────────────────────────────────────────
# 공개 도구 2 — 계약 전체 과정 조회
# ─────────────────────────────────────────────

async def get_contract_process(
    bid_no: str,
    bid_ord: str = "000",
    biz_type: str = "용역",
) -> dict:
    """
    계약 전체 과정 조회 (계약과정통합공개서비스)

    입찰공고번호로 사전규격→입찰공고→개찰→낙찰→계약 전 단계를 한 번에 조회합니다.
    '이 공고의 계약 과정 전체 보여줘' / '최종 계약금액이 얼마야?' 에 답합니다.

    Args:
        bid_no:   입찰공고번호 (예: '20230311048') — 필수
        bid_ord:  입찰공고차수 (기본값 '000')
        biz_type: 업무구분 (물품|공사|용역|외자)
    """
    ops = {
        "물품": "getCntrctProcssIntgOpenThng",
        "공사": "getCntrctProcssIntgOpenCnstwk",
        "용역": "getCntrctProcssIntgOpenServc",
        "외자": "getCntrctProcssIntgOpenFrgcpt",
    }
    op = ops.get(biz_type, "getCntrctProcssIntgOpenServc")

    params = {
        "numOfRows": 1,
        "inqryDiv":  "1",
        "bidNtceNo": bid_no,
        "bidNtceOrd": bid_ord,
    }

    try:
        result = await fetch(ENDPOINTS["process"], op, params)
    except Exception as e:
        return {"error": str(e), "stages": []}

    items = result.get("items", [])
    if not items:
        return {
            "bid_no": bid_no,
            "message": f"공고번호 {bid_no} 에 해당하는 계약과정 정보가 없습니다.",
            "stages": [],
        }

    # 계약과정 통합공개는 단건 반환이 일반적
    raw = items[0]

    def _s(key: str) -> str:
        v = raw.get(key) or ""
        return v[:16] if v else ""

    def _parse_bracket_list(field_val: str) -> list[list[str]]:
        """
        '[seq^field1^field2^...]' 형식 문자열 파싱
        예: '[1^(주)다음이앤지^6218614571^59065900^88.322^16^2023-03-15 11:00:00]'
        """
        import re
        result = []
        for block in re.findall(r'\[([^\]]+)\]', field_val or ""):
            result.append(block.split('^'))
        return result

    stages = []

    # ── 1. 발주계획 (orderPlanNo 있으면)
    if raw.get("orderPlanNo") or raw.get("orderBizNm"):
        stages.append({
            "단계": "발주계획",
            "계획번호": raw.get("orderPlanNo", ""),
            "사업명": raw.get("orderBizNm", ""),
            "발주기관": raw.get("orderInsttNm", ""),
            "계획년월": raw.get("orderYm", ""),
            "조달방법": raw.get("prcrmntMethdNm", ""),
        })

    # ── 2. 사전규격 공개
    if raw.get("bfSpecRgstNo") or raw.get("bfSpecBizNm"):
        stages.append({
            "단계": "사전규격 공개",
            "등록번호": raw.get("bfSpecRgstNo", ""),
            "사업명": raw.get("bfSpecBizNm", ""),
            "공개기관": raw.get("bfSpecNtceInsttNm", ""),
            "의견등록마감": (raw.get("opninRgstClseDt") or "")[:16],
        })

    # ── 3. 입찰공고
    if raw.get("bidNtceNo") or raw.get("bidNtceNm"):
        stages.append({
            "단계": "입찰공고",
            "공고번호": raw.get("bidNtceNo", ""),
            "공고명": raw.get("bidNtceNm", ""),
            "공고일시": _s("bidNtceDt"),
            "입찰방법": raw.get("bidMthdNm", ""),
            "수요기관": raw.get("bidDminsttNm", ""),
        })

    # ── 4. 낙찰 (bidwinrInfoList 파싱)
    # 형식: [seq^업체명^사업자번호^대표자^낙찰금액^낙찰률^참가업체수^낙찰일시]
    winner_raw = raw.get("bidwinrInfoList", "")
    if winner_raw:
        winners = _parse_bracket_list(winner_raw)
        for w in winners:
            # 최소 6개 필드 필요
            if len(w) < 6:
                continue
            corp_nm  = w[1] if len(w) > 1 else ""
            biz_no   = w[2] if len(w) > 2 else ""
            amt_val  = parse_amount(w[4]) if len(w) > 4 else 0
            rate_val = w[5] if len(w) > 5 else ""
            prtc     = w[6] if len(w) > 6 else ""
            dt_val   = w[7][:16] if len(w) > 7 else ""
            stages.append({
                "단계": "낙찰",
                "낙찰일시": dt_val,
                "낙찰업체": corp_nm,
                "사업자번호": biz_no,
                "낙찰금액": format_amount(amt_val),
                "낙찰률(%)": rate_val,
                "참가업체수": prtc,
            })
    elif raw.get("scsbidCorpNm") or raw.get("sucsfbidCrpNm"):
        # fallback: 단일 필드
        award_amt = parse_amount(raw.get("scsbidAmt") or 0)
        stages.append({
            "단계": "낙찰",
            "낙찰일시": _s("scsbidDt"),
            "낙찰업체": raw.get("scsbidCorpNm") or raw.get("sucsfbidCrpNm") or "",
            "사업자번호": raw.get("bizno") or "",
            "낙찰금액": format_amount(award_amt),
            "낙찰률(%)": raw.get("scsbidRate") or "",
        })

    # ── 5. 계약체결 (cntrctInfoList 파싱)
    # 형식: [seq^계약번호^공고명^발주기관^수요기관^계약방법^계약금액^계약일자]
    cntrct_raw = raw.get("cntrctInfoList", "")
    if cntrct_raw:
        contracts = _parse_bracket_list(cntrct_raw)
        for c in contracts:
            if len(c) < 7:
                continue
            cntrct_no  = c[1] if len(c) > 1 else ""
            inst_nm    = c[3] if len(c) > 3 else ""
            mthd_nm    = c[5] if len(c) > 5 else ""
            amt_val    = parse_amount(c[6]) if len(c) > 6 else 0
            cntrct_dt  = c[7][:10] if len(c) > 7 else ""
            stages.append({
                "단계": "계약체결",
                "계약일자": cntrct_dt,
                "계약번호": cntrct_no,
                "계약기관": inst_nm,
                "계약방법": mthd_nm,
                "계약금액": format_amount(amt_val),
            })
    elif raw.get("cntrctCnclsDate") or raw.get("cntrctAmt"):
        # fallback
        cntrct_amt = parse_amount(raw.get("cntrctAmt") or raw.get("thtmCntrctAmt") or 0)
        stages.append({
            "단계": "계약체결",
            "계약일자": _s("cntrctCnclsDate"),
            "계약기관": raw.get("cntrctInsttNm") or raw.get("orderInsttNm") or "",
            "계약방법": raw.get("cntrctCnclsMthdNm") or "",
            "계약금액": format_amount(cntrct_amt),
        })

    return {
        "bid_no":  bid_no,
        "bid_ord": bid_ord,
        "공고명":  raw.get("bidNtceNm", ""),
        "발주기관": raw.get("orderInsttNm") or raw.get("bfSpecNtceInsttNm") or "",
        "stages":  stages,
    }
