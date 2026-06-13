"""
부산계약MCP — 지능형 조달 컨설팅 에이전트
나라장터 OpenAPI 기반 MCP 서버
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
import asyncio
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

from tools.contract import (
    search_contracts,
    analyze_price_benchmark,
    check_voluntary_contract,
)
from tools.bid import search_bid_announcements
from tools.vendor import search_companies, search_busan_companies, check_debarred_vendors
from tools.award import get_bid_award_result, get_contract_process
from tools.legal import search_legal_info, get_audit_guard, map_law_terms
from tools.special_vendors import search_special_vendors
from tools.busan_scraper import search_busan_local, rank_companies_busan_local

app = Server("busan-contract-mcp")


# ──────────────────────────────────────────
# 도구 목록 정의
# ──────────────────────────────────────────
@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="search_contracts",
            description=(
                "나라장터 계약 이력을 검색합니다. "
                "키워드, 기관명, 계약방법, 날짜 범위로 필터링 가능합니다. "
                "유사 사업의 계약 사례를 찾을 때 사용하세요."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "계약건명 키워드 (예: '청소', '경비', 'IT유지보수')"},
                    "inst_name": {"type": "string", "description": "계약기관명 (예: '부산광역시', '부산시설공단')"},
                    "contract_method": {"type": "string", "description": "계약방법 (예: '수의계약', '일반경쟁', '제한경쟁')"},
                    "start_date": {"type": "string", "description": "조회 시작일 (YYYYMMDDHHMM, 예: '202301010000')"},
                    "end_date": {"type": "string", "description": "조회 종료일 (YYYYMMDDHHMM, 예: '202312312359')"},
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역", "외자"],
                        "description": "업무구분 (기본값: 용역)",
                        "default": "용역",
                    },
                    "min_amount": {"type": "integer", "description": "최소 계약금액 (원, 예: 10000000)"},
                    "max_amount": {"type": "integer", "description": "최대 계약금액 (원, 예: 50000000)"},
                    "page_size": {"type": "integer", "description": "최대 결과 수 (기본값: 50)", "default": 50},
                },
            },
        ),
        types.Tool(
            name="analyze_price_benchmark",
            description=(
                "유사 사업의 낙찰가를 분석하여 적정 기초금액을 제안합니다. "
                "'이 사업 얼마에 해야 해?'라는 질문에 답합니다. "
                "기본값은 최근 3년(years=3) 데이터이며, 응답 첫 줄에 항상 분석 기간(예: '최근 3년(2023~2026) 기준')을 명시합니다. "
                "더 넓은 범위가 필요하면 years 파라미터를 늘릴 수 있습니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "사업 유형 키워드 (예: '환경미화', '시설관리', '소프트웨어개발')"},
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역", "외자"],
                        "description": "업무구분",
                        "default": "용역",
                    },
                    "inst_region": {"type": "string", "description": "기관 지역 필터 (예: '부산', '경남')"},
                    "years": {"type": "integer", "description": "분석 기간 (최대 3년)", "default": 3},
                },
                "required": ["keyword"],
            },
        ),
        types.Tool(
            name="check_voluntary_contract",
            description=(
                "수의계약 가능 여부를 검토합니다. "
                "지방계약법 시행령 제25조 기준으로 금액과 조건을 판단하고 "
                "법적 근거와 주의사항을 제공합니다. "
                "감사 예방에 활용하세요."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "amount": {"type": "integer", "description": "계약 예정 금액 (원 단위, 예: 30000000)"},
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역"],
                        "description": "업무구분",
                        "default": "용역",
                    },
                    "special_condition": {
                        "type": "string",
                        "enum": ["긴급", "특허", "소액"],
                        "description": "특수 사유 (해당 없으면 생략)",
                    },
                },
                "required": ["amount"],
            },
        ),
        types.Tool(
            name="search_bid_announcements",
            description=(
                "최근 나라장터 입찰공고를 검색합니다. "
                "유사 사업의 공고 조건, 예산 규모, 계약 방식을 참고할 때 사용합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "공고명 키워드"},
                    "inst_name": {"type": "string", "description": "발주기관명"},
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역", "외자"],
                        "default": "용역",
                    },
                    "start_date": {"type": "string", "description": "조회 시작일 (YYYYMMDDHHMM)"},
                    "end_date": {"type": "string", "description": "조회 종료일 (YYYYMMDDHHMM)"},
                    "min_amount": {"type": "integer", "description": "최소 예산금액 (원)"},
                    "max_amount": {"type": "integer", "description": "최대 예산금액 (원, 예: 50000000 → 5천만원 이하)"},
                    "months_back": {"type": "integer", "description": "키워드 검색 시 소급 개월 수 (기본 24)", "default": 24},
                    "page_size": {"type": "integer", "default": 50},
                },
            },
        ),
        types.Tool(
            name="check_debarred_vendors",
            description=(
                "부정당제재 업체를 조회합니다. "
                "계약 체결 전 반드시 확인해야 하는 업체 제재 이력을 검색합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "corp_name": {"type": "string", "description": "업체명 (부분 일치)"},
                    "biz_reg_no": {"type": "string", "description": "사업자등록번호 (10자리, 하이픈 없이)"},
                },
            },
        ),
        types.Tool(
            name="search_companies",
            description=(
                "나라장터 등록 업체를 검색하고 전문성·부정당제재·우대사항 점수로 추천합니다. "
                "'이 용역 할 수 있는 업체 뽑아줘' 질문에 답합니다. "
                "기본값은 부산 우선(그룹A) + 타 지역 우수(그룹B) 분리 추천입니다. "
                "region을 변경하면 서울·경기 등 다른 지역 우선 추천도 가능하고, "
                "region을 생략하면 전국 단일 랭킹으로 조회합니다. "
                "여성기업·장애인기업·사회적기업 등 우대 업체를 자동으로 상단 배치하며 "
                "부정당제재 여부(🔴/🟡/🟢)를 자동으로 함께 확인합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "service_keyword": {
                        "type": "string",
                        "description": "용역/물품 종류 키워드 (예: '사전타당성조사', '청소', '소프트웨어', '시설관리')",
                    },
                    "biz_type": {
                        "type": "string",
                        "enum": ["용역", "물품", "공사"],
                        "default": "용역",
                    },
                    "region": {
                        "type": "string",
                        "description": "지역 우선 필터 (기본: '부산'). 전국 단일 랭킹 원할 시 null 전달. 예: '서울', '경기', '인천'",
                        "default": "부산",
                    },
                    "prefer_local_economy": {
                        "type": "boolean",
                        "description": "지역 우선 그룹화 여부 (region 지정 시 유효, 기본: true)",
                        "default": True,
                    },
                    "months_back": {
                        "type": "integer",
                        "description": "낙찰이력 소급 개월 수 (기본 48)",
                        "default": 48,
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "최종 추천 업체 수 (기본 10, region 지정 시 절반씩 그룹A/B)",
                        "default": 10,
                    },
                },
                "required": ["service_keyword"],
            },
        ),
        types.Tool(
            name="get_bid_award_result",
            description=(
                "나라장터 입찰공고의 낙찰결과를 조회합니다. "
                "'이 공고 누가 낙찰받았어?' / '2023년 주차수급 용역 낙찰업체가 어디야?' 에 답합니다. "
                "공고번호(bid_no) 또는 키워드로 검색하며, 낙찰업체명·낙찰금액·낙찰률·참가업체수를 반환합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "공고명 키워드 (예: '주차수급', '환경미화')"},
                    "bid_no": {"type": "string", "description": "입찰공고번호 (예: '20230311048') — 특정 공고 직접 조회 시 사용"},
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역", "외자"],
                        "default": "용역",
                    },
                    "inst_name": {"type": "string", "description": "발주기관 필터 (예: '부산', '연제구')"},
                    "start_date": {"type": "string", "description": "조회 시작일 (YYYYMMDDHHMM)"},
                    "end_date": {"type": "string", "description": "조회 종료일 (YYYYMMDDHHMM)"},
                    "months_back": {"type": "integer", "description": "소급 개월 수 (기본 36)", "default": 36},
                },
            },
        ),
        types.Tool(
            name="get_contract_process",
            description=(
                "입찰공고번호로 계약 전체 과정을 조회합니다. "
                "사전규격 공개 → 입찰공고 → 개찰 → 낙찰 → 계약체결 까지 모든 단계를 한 번에 보여줍니다. "
                "'이 공고 계약 과정 전체 보여줘' / '최종 계약금액이 얼마야?' / '누가 낙찰받았어?' 에 답합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "bid_no": {"type": "string", "description": "입찰공고번호 (예: '20230311048') — 필수"},
                    "bid_ord": {"type": "string", "description": "입찰공고차수 (기본값: '000')", "default": "000"},
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역", "외자"],
                        "default": "용역",
                    },
                },
                "required": ["bid_no"],
            },
        ),
        # ── 법령 정보 도구 ──────────────────────────
        types.Tool(
            name="search_legal_info",
            description=(
                "법제처 국가법령정보센터에서 법령·행정규칙·부산시 조례를 실시간으로 동시 조회합니다. "
                "'수의계약 범위가 어떻게 돼?', '지방계약법에 뭐라고 써 있어?' 질문에 답합니다. "
                "부산광역시 조례를 우선적으로 포함합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "검색어 (예: '수의계약 범위', '용역 계약 한도', '지역제한 입찰')",
                    },
                    "include_admin_rule": {
                        "type": "boolean",
                        "description": "행정규칙(조달청 고시 등) 포함 여부 (기본 true)",
                        "default": True,
                    },
                    "include_busan_ordinance": {
                        "type": "boolean",
                        "description": "부산시 자치법규 포함 여부 (기본 true)",
                        "default": True,
                    },
                    "display": {
                        "type": "integer",
                        "description": "출처별 최대 결과 수 (기본 5)",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_audit_guard",
            description=(
                "계약 과정에서 감사 지적을 피할 수 있도록 관련 법령해석례·판례를 실시간 조회하고 "
                "감사원 사전컨설팅 신청 경로를 안내합니다. "
                "'이런 수의계약 했다가 감사 걸리면?' / '비슷한 사례 판례 있어?' 질문에 답합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "situation": {
                        "type": "string",
                        "description": "계약 상황 설명 (예: '긴급 수의계약 후 감사 지적 사례', '소액 수의계약 분할 발주')",
                    },
                    "display": {
                        "type": "integer",
                        "description": "출처별 최대 결과 수 (기본 5)",
                        "default": 5,
                    },
                },
                "required": ["situation"],
            },
        ),
        types.Tool(
            name="search_special_vendors",
            description=(
                "장애인기업·여성기업의 나라장터 낙찰 순위를 조회합니다. "
                "'부산 장애인기업 낙찰 많은 순으로 알려줘' / "
                "'여성기업 중 청소용역 많이 낙찰받은 업체는?' 질문에 답합니다. "
                "장애인기업종합지원센터 API + 여성기업 목록 엑셀을 나라장터 낙찰이력과 교차 분석합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "service_keyword": {
                        "type": "string",
                        "description": "공고명 키워드 (생략 시 전체 광범위 검색, 예: '청소', '경비', '시설관리')",
                    },
                    "vendor_type": {
                        "type": "string",
                        "enum": ["장애인", "여성", "all"],
                        "description": "인증 유형 (기본: all = 장애인기업+여성기업 통합)",
                        "default": "all",
                    },
                    "region": {
                        "type": "string",
                        "description": "지역 필터 (기본: '부산')",
                        "default": "부산",
                    },
                    "biz_type": {
                        "type": "string",
                        "enum": ["물품", "공사", "용역", "all"],
                        "description": "업무구분 (기본: 'all' = 물품+공사+용역 동시 검색)",
                        "default": "all",
                    },
                    "months_back": {
                        "type": "integer",
                        "description": "소급 개월 수 (기본: 24개월)",
                        "default": 24,
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "결과 최대 수 (기본: 20)",
                        "default": 20,
                    },
                },
            },
        ),
        types.Tool(
            name="search_busan_contracts",
            description=(
                "부산시청 계약정보공개시스템에서 수집한 로컬 DB를 검색합니다. "
                "공사·용역·물품 수의계약 포함 (kind 파라미터로 구분). "
                "청소용역·경비용역 등 소액 수의계약 조회 시 kind='용역'으로 지정하세요. "
                "'부산시 청소 수의계약 업체 알려줘' / '부산시 직접 발주한 소액계약 뭐가 있어?' 에 답합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword":    {"type": "string",  "description": "계약건명 키워드 (예: '청소', '경비')"},
                    "company":    {"type": "string",  "description": "업체명 키워드"},
                    "kind":       {
                        "type": "string",
                        "enum": ["공사", "용역", "물품"],
                        "description": "계약종류 필터 (공사|용역|물품, 생략 시 전체). 청소용역 조회 시 '용역' 사용",
                    },
                    "min_amount": {"type": "integer", "description": "최소 금액 (원)"},
                    "max_amount": {"type": "integer", "description": "최대 금액 (원)"},
                    "years":      {"type": "integer", "description": "최근 N년 이내 (기본 5)", "default": 5},
                    "top_n":      {"type": "integer", "description": "최대 결과 수 (기본 20)", "default": 20},
                },
            },
        ),
        types.Tool(
            name="rank_busan_vendors",
            description=(
                "부산시청 로컬 DB 기반으로 수의계약 업체 순위를 냅니다. "
                "'부산시 청소 수의계약 가장 많이 받은 업체 순위' 질문에 답합니다. "
                "나라장터 API 데이터와 합쳐서 종합 순위를 낼 수 있습니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {"type": "string",  "description": "계약건명 키워드 (생략 시 전체)"},
                    "years":   {"type": "integer", "description": "최근 N년 (기본 5)", "default": 5},
                    "top_n":   {"type": "integer", "description": "상위 N개 업체 (기본 10)", "default": 10},
                },
            },
        ),
        types.Tool(
            name="map_law_terms",
            description=(
                "일상 표현을 법률 용어로 변환하고 관련 법령 조항을 안내합니다. "
                "'동네 업체랑 수의계약 해도 돼?', '긴급이라서 그냥 해도 되나?' 같은 "
                "구어체 질문을 전문 법률 용어로 매핑합니다. "
                "신규 임용 주무관이나 법령에 익숙하지 않은 경우 특히 유용합니다."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "plain_text": {
                        "type": "string",
                        "description": "사용자 입력 구어체 표현 (예: '동네 업체랑 수의계약', '쪼개서 발주')",
                    },
                    "fetch_articles": {
                        "type": "boolean",
                        "description": "관련 법령 실제 조회 여부 (기본 true)",
                        "default": True,
                    },
                },
                "required": ["plain_text"],
            },
        ),
    ]


# ──────────────────────────────────────────
# 도구 실행 핸들러
# ──────────────────────────────────────────
@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        if name == "search_contracts":
            result = await search_contracts(**arguments)
            text = _format_contract_list(result)

        elif name == "analyze_price_benchmark":
            result = await analyze_price_benchmark(**arguments)
            text = result.get("recommendation", json.dumps(result, ensure_ascii=False, indent=2))

        elif name == "check_voluntary_contract":
            result = await check_voluntary_contract(**arguments)
            text = _format_voluntary_check(result)

        elif name == "search_bid_announcements":
            result = await search_bid_announcements(**arguments)
            text = _format_bid_list(result)

        elif name == "check_debarred_vendors":
            result = await check_debarred_vendors(**arguments)
            text = result.get("message", "") + "\n" + json.dumps(result.get("items", []), ensure_ascii=False, indent=2)

        elif name in ("search_companies", "search_busan_companies"):
            if name == "search_busan_companies":
                arguments.setdefault("region", "부산")
            # 구버전 클라이언트가 캐시한 스키마의 page_size 방어
            arguments.pop("page_size", None)
            result = await search_companies(**arguments)
            text = _format_company_list(result)

        elif name == "get_bid_award_result":
            result = await get_bid_award_result(**arguments)
            text = _format_award_list(result)

        elif name == "get_contract_process":
            result = await get_contract_process(**arguments)
            text = _format_contract_process(result)

        elif name == "search_special_vendors":
            result = await search_special_vendors(**arguments)
            text = _format_special_vendors(result)

        elif name == "search_legal_info":
            result = await search_legal_info(**arguments)
            text = _format_legal_info(result)

        elif name == "get_audit_guard":
            result = await get_audit_guard(**arguments)
            text = result.get("audit_guidance", json.dumps(result, ensure_ascii=False, indent=2))

        elif name == "map_law_terms":
            result = await map_law_terms(**arguments)
            text = result.get("guidance", json.dumps(result, ensure_ascii=False, indent=2))

        elif name == "search_busan_contracts":
            result = search_busan_local(**arguments)
            text = _format_busan_contracts(result)

        elif name == "rank_busan_vendors":
            result = rank_companies_busan_local(**arguments)
            text = _format_busan_ranking(result)

        else:
            text = f"알 수 없는 도구: {name}"

    except Exception as e:
        text = f"❌ 오류 발생: {type(e).__name__}: {e}"

    return [types.TextContent(type="text", text=text)]


# ──────────────────────────────────────────
# 출력 포매터
# ──────────────────────────────────────────
def _format_contract_list(result: dict) -> str:
    items = result.get("items", [])
    total = result.get("totalCount", 0)
    matched = result.get("matchedCount", len(items))

    if not items:
        return "조회된 계약 정보가 없습니다."

    lines = [f"전체 {total:,}건 중 {matched}건 표시\n"]
    for i, item in enumerate(items[:20], 1):
        from tools.api_client import parse_amount, format_amount
        amt = format_amount(parse_amount(item.get("thtmCntrctAmt", 0)))
        detail_url = item.get("나라장터상세URL", "")
        orig_url = item.get("원공고URL", "")

        lines.append(
            f"{i}. [{item.get('cntrctCnclsDate', '')}] {item.get('cntrctNm', '(무제)')}\n"
            f"   기관: {item.get('cntrctInsttNm', '')} | 방법: {item.get('cntrctMthdNm', '')} | 금액: {amt}"
        )
        if detail_url:
            lines.append(f"   📄 계약상세: {detail_url}")
        if orig_url:
            lines.append(f"   📋 원공고:   {orig_url}")
        lines.append("")

    if matched > 20:
        lines.append(f"... 외 {matched - 20}건 더 있음")
    return "\n".join(lines)


def _format_bid_list(result: dict) -> str:
    items = result.get("items", [])
    total = result.get("totalCount", 0)
    matched = result.get("matchedCount", len(items))

    if not items:
        tried = result.get("keyword_variants_tried", [])
        stats = result.get("variant_stats", "")
        if tried:
            return (
                f"조회된 입찰공고가 없습니다.\n"
                f"시도한 키워드: {' / '.join(tried)}\n"
                f"결과: {stats}\n"
                f"→ 다른 키워드를 시도해보세요."
            )
        return "조회된 입찰공고가 없습니다."

    stats_line = result.get("variant_stats", "")
    lines = [
        f"전체 {total:,}건 중 {matched}건 표시"
        + (f"  (키워드 변형: {stats_line})" if stats_line else ""),
        ""
    ]
    for i, item in enumerate(items[:20], 1):
        lines.append(
            f"{i}. [{item.get('공고일', '')}] {item.get('공고명', '(무제)')}\n"
            f"   기관: {item.get('발주기관', '')} | 방법: {item.get('계약방법', '')} | 예산: {item.get('예산금액', '')}\n"
            f"   마감: {item.get('입찰마감', '')}"
        )
        if item.get("공고상세URL"):
            lines.append(f"   🔗 공고상세: {item['공고상세URL']}")
        if item.get("낙찰결과URL") and item.get("공고상세URL") != item.get("낙찰결과URL"):
            lines.append(f"   🏆 낙찰결과: {item['낙찰결과URL']}")

        kw = item.get("매칭키워드", "")
        if kw:
            lines.append(f"   🔍 매칭키워드: '{kw}'")

        attachments = item.get("첨부파일", [])
        if attachments:
            lines.append(f"   📎 첨부파일 ({len(attachments)}개):")
            for att in attachments[:3]:
                lines.append(f"      └ {att['파일명']}")
                lines.append(f"        ⬇ {att['다운로드URL']}")
        lines.append("")

    if matched > 20:
        lines.append(f"... 외 {matched - 20}건 더 있음")
    return "\n".join(lines)


def _format_voluntary_check(result: dict) -> str:
    possible = result.get("is_possible", False)
    icon = "✅ 수의계약 가능" if possible else "❌ 수의계약 불가"
    lines = [
        f"{icon}",
        f"",
        f"📋 법령상 가능 여부: {result.get('legal_basis', '')}",
        f"💰 금액 검토: {result.get('amount_check', '')}",
    ]
    busan = result.get("busan_ordinance_check", "")
    if busan:
        lines += ["", f"📙 부산시 조례상 적정성: {busan}"]
    lines += [
        "",
        "📌 조건 및 절차:",
    ]
    for cond in result.get("conditions", []):
        lines.append(f"  • {cond}")
    lines.append("")
    lines.append(result.get("caution", ""))
    source = result.get("source", "")
    if source:
        lines += ["", "---", source]
    return "\n".join(lines)


def _format_special_vendors(result: dict) -> str:
    if "error" in result:
        return f"❌ {result['error']}"

    items = result.get("items", [])
    summary = result.get("summary", "")
    lines = [summary, ""]

    CERT_ICON = {"장애인기업": "♿", "여성기업": "👩"}
    for rank, item in enumerate(items, 1):
        icon = CERT_ICON.get(item.get("인증유형", ""), "🏢")
        lines.append(
            f"{rank}. {icon} [{item.get('인증유형','')}] {item.get('업체명','')}\n"
            f"   낙찰: {item.get('낙찰횟수',0)}회 | 평균: {item.get('평균낙찰금액','')} | 최근: {item.get('최근낙찰일','')}\n"
            f"   업종: {item.get('주업종','')}\n"
            f"   주소: {item.get('주소','')}\n"
            f"   발주기관: {item.get('발주기관','')}"
        )
        if item.get("대표공고명"):
            lines.append(f"   대표공고: {item['대표공고명']}")
        lines.append("")

    if not items:
        lines.append("낙찰 이력이 확인된 인증 업체가 없습니다.")
        lines.append("키워드를 바꾸거나 months_back을 늘려보세요.")

    return "\n".join(lines)


def _format_legal_info(result: dict) -> str:
    query = result.get("query", "")
    lines = [f"🔍 '{query}' 법령정보 통합 조회\n"]

    law_items = result.get("law", [])
    if law_items and not (len(law_items) == 1 and law_items[0].get("error")):
        lines.append("📗 현행법령:")
        for item in law_items[:5]:
            eff = item.get("effective_date", "")
            lines.append(f"  • [{item.get('kind','')}] {item.get('name','')} (시행: {eff})")
            if item.get("link"):
                lines.append(f"    🔗 {item['link']}")

    admrul_items = result.get("admrul", [])
    if admrul_items and not (len(admrul_items) == 1 and admrul_items[0].get("error")):
        lines.append("\n📘 행정규칙:")
        for item in admrul_items[:5]:
            lines.append(f"  • [{item.get('kind','')}] {item.get('name','')}")
            if item.get("link"):
                lines.append(f"    🔗 {item['link']}")

    ordin_items = result.get("busan_ordin", [])
    if ordin_items and not (len(ordin_items) == 1 and ordin_items[0].get("error")):
        lines.append("\n📙 부산시 자치법규:")
        for item in ordin_items[:5]:
            region = item.get("region", "")
            lines.append(f"  • [{item.get('kind','')}] {item.get('name','')} ({region})")
            if item.get("link"):
                lines.append(f"    🔗 {item['link']}")

    if not any([law_items, admrul_items, ordin_items]):
        lines.append("관련 법령·조례를 찾지 못했습니다. 검색어를 바꿔보세요.")

    lines.append("\n---\n📌 **법제처 국가법령정보센터 실시간 데이터 기준입니다**")
    return "\n".join(lines)


def _format_company_list(result: dict) -> str:
    # AI 관련 용역 안내 응답
    if result.get("ai_guidance"):
        return result.get("summary", "")

    group_a = result.get("group_a", [])
    group_b = result.get("group_b", [])

    if not group_a and not group_b:
        return result.get("summary", "조회된 업체가 없습니다.")

    lines = [result.get("summary", ""), ""]

    def _fmt_vendor(rank: int, c: dict) -> str:
        badge    = c.get("제재상태", {}).get("badge", "🟢")
        label    = c.get("제재상태", {}).get("label", "이상없음")
        detail   = c.get("제재상태", {}).get("detail", "")
        score    = c.get("점수", 0)
        cnt      = c.get("낙찰횟수", 0)
        suyee    = c.get("수의계약횟수", 0)
        avg      = c.get("평균낙찰금액", "")
        latest   = c.get("최근낙찰일", "")
        rate     = c.get("낙찰률", "")
        inst     = c.get("발주기관", "")

        suyee_tag = f" (수의 {suyee}건 포함)" if suyee else ""
        row = (
            f"{rank}. {badge} {c.get('업체명', '')}{suyee_tag}  (점수:{score} | 낙찰{cnt}회)\n"
            f"   제재: {label}" + (f" — {detail}" if detail else "") + "\n"
            f"   주소: {c.get('주소', '')}\n"
            f"   평균낙찰가: {avg}" + (f" | 낙찰률: {rate}%" if rate else "")
            + (f" | 최근: {latest}" if latest else "") + "\n"
            f"   수행기관: {inst}"
        )
        return row

    region_label = result.get("region", "부산")
    if group_a:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if group_b:
            lines.append(f"🏙️  그룹 A — {region_label} 소재 업체 (지역경제 우선)")
        else:
            lines.append(f"🌐  전국 추천 업체 ({region_label})")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        for i, c in enumerate(group_a, 1):
            lines.append(_fmt_vendor(i, c))
            lines.append("")

    if group_b:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("🌐  그룹 B — 타 지역 우수 업체 (전문성 상위)")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        offset = len(group_a)
        for i, c in enumerate(group_b, offset + 1):
            lines.append(_fmt_vendor(i, c))
            lines.append("")

    lines.append("📋 신호등: 🔴=현재 계약불가  🟡=이력있음(해제)  🟢=이상없음  ⚪=확인불가(사업자번호 없음)")
    lines.append("   ★=업종 전문성 높음  [계약이력]=계약 낙찰 이력으로 발굴된 업체")
    lines.append("")
    lines.append("※ 본 데이터는 나라장터에 등록된 입찰 및 전자 수의계약 체결 내역을 통합 분석한 결과입니다.")
    return "\n".join(lines)


def _format_award_list(result: dict) -> str:
    if "error" in result:
        return f"❌ API 오류: {result['error']}\n→ 신규 API 키 활성화 대기 중일 수 있습니다. 잠시 후 재시도해주세요."

    items = result.get("items", [])
    if not items:
        return "조회된 낙찰결과가 없습니다."

    lines = [f"낙찰결과 {len(items)}건\n"]
    for i, item in enumerate(items[:30], 1):
        rate_str = f" ({item['낙찰률']}%)" if item.get("낙찰률") else ""
        prtc_str = f" / 참가 {item['참가업체수']}개사" if item.get("참가업체수") else ""
        lines.append(
            f"{i}. [{item.get('개찰일시', '')}] {item.get('공고명', '(무제)')}\n"
            f"   기관: {item.get('발주기관', '')} | 공고번호: {item.get('공고번호', '')}\n"
            f"   🏆 낙찰업체: {item.get('낙찰업체', '미확인')}\n"
            f"   💰 낙찰금액: {item.get('낙찰금액', '')}{rate_str}{prtc_str}"
        )
        if item.get("입찰공고URL"):
            lines.append(f"   🔗 공고: {item['입찰공고URL']}")
        lines.append("")

    if len(items) > 30:
        lines.append(f"... 외 {len(items) - 30}건 더 있음")
    return "\n".join(lines)


def _format_contract_process(result: dict) -> str:
    if "error" in result:
        return f"❌ API 오류: {result['error']}\n→ 신규 API 키 활성화 대기 중일 수 있습니다. 잠시 후 재시도해주세요."

    if not result.get("stages"):
        return result.get("message", "계약과정 정보가 없습니다.")

    lines = [
        f"📋 계약과정 통합공개 — 공고번호 {result.get('bid_no', '')}",
        f"   공고명: {result.get('공고명', '')}",
        f"   발주기관: {result.get('발주기관', '')}",
        "",
    ]

    stage_icons = {
        "사전규격 공개": "📝",
        "입찰공고":     "📢",
        "개찰":         "🔓",
        "낙찰":         "🏆",
        "계약체결":     "✍️",
    }

    for stage in result.get("stages", []):
        name = stage.get("단계", "")
        icon = stage_icons.get(name, "▶")
        lines.append(f"{icon} [{stage.get('일시', '일시 미확인')}] {name}")

        # 단계별 추가 정보 출력
        skip = {"단계", "일시"}
        for k, v in stage.items():
            if k in skip or not v:
                continue
            lines.append(f"   {k}: {v}")
        lines.append("")

    return "\n".join(lines)


def _format_busan_contracts(result: dict) -> str:
    items = result.get("items", [])
    notice = result.get("notice", "")
    if notice:
        return notice

    lines = [
        f"[부산시 계약정보 DB]  수집일: {result.get('db_date', '')}  |  "
        f"DB 총 {result.get('total_db', 0):,}건 중 {result.get('matched', 0)}건 매칭\n"
    ]
    for i, item in enumerate(items, 1):
        amt = item.get("계약금액") or f"{item.get('계약금액_원', 0):,}원"
        lines.append(
            f"{i}. [{item.get('계약일자', '')}] {item.get('계약건명', '(무제)')}\n"
            f"   업체: {item.get('업체명', '')} | 금액: {amt} | 방법: {item.get('계약방법', '')}\n"
            f"   부서: {item.get('담당부서', '')} | 기간: {item.get('계약기간', '')}"
        )
        if item.get("상세URL"):
            lines.append(f"   🔗 {item['상세URL']}")
        lines.append("")

    if not items:
        lines.append("조건에 맞는 계약이 없습니다.")
    lines.append("출처: 부산시청 계약정보공개시스템 (로컬 DB)")
    return "\n".join(lines)


def _format_busan_ranking(result: dict) -> str:
    ranking = result.get("ranking", [])
    lines = [
        f"[부산시 수의계약 업체 순위]  수집일: {result.get('db_date', '')}",
        f"키워드: {result.get('keyword', '전체')} | 최근 {result.get('years', 5)}년\n",
    ]
    for i, r in enumerate(ranking, 1):
        lines.append(
            f"{i}. {r['업체명']}\n"
            f"   계약횟수: {r['계약횟수']}건 | 합계: {r.get('합계금액_표시', '')} | 최근: {r.get('최근계약', '')}"
        )
        if r.get("계약목록"):
            preview = r["계약목록"][:2]
            lines.append(f"   대표계약: {' / '.join(preview)}")
        lines.append("")

    if not ranking:
        lines.append("순위 데이터가 없습니다. 먼저 스크래퍼를 실행하세요.")
    lines.append("출처: 부산시청 계약정보공개시스템 (로컬 DB)")
    return "\n".join(lines)


# ──────────────────────────────────────────
# 엔트리포인트
# ──────────────────────────────────────────
def _run_http():
    from contextlib import asynccontextmanager
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route
    from starlette.responses import JSONResponse
    import uvicorn

    async def server_card(request):
        return JSONResponse({
            "name": "busan-contract-mcp",
            "version": "1.0.0",
            "description": "나라장터 OpenAPI 기반 지능형 조달 컨설팅 MCP 서버",
        })

    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    session_manager = StreamableHTTPSessionManager(
        app=app,
        event_store=None,
        json_response=False,
        stateless=True,
    )

    async def handle_mcp(scope, receive, send):
        await session_manager.handle_request(scope, receive, send)

    @asynccontextmanager
    async def lifespan(_app):
        async with session_manager.run():
            yield

    starlette_app = Starlette(
        lifespan=lifespan,
        routes=[
            Route("/.well-known/mcp/server-card.json", endpoint=server_card),
            Mount("/mcp", app=handle_mcp),
            Mount("/sse", app=handle_mcp),
            Mount("/", app=handle_mcp),
        ],
    )

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(starlette_app, host="0.0.0.0", port=port)


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    if os.environ.get("MCP_TRANSPORT") == "http":
        _run_http()
    else:
        asyncio.run(main())
