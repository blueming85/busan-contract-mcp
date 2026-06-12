"""
법령 정보 핵심 도구 3종

search_legal_info  - 법령 + 부산시 조례 동시 조회
get_audit_guard    - 감사 지적 예방 (해석례·판례·사전컨설팅)
map_law_terms      - 일상어 → 법률 용어 변환 + 관련 조항 안내
"""
import asyncio
from typing import Optional
from tools.law_client import (
    law_search,
    search_busan_ordinance,
    search_multi_targets,
    law_content,
)

SOURCE_BADGE = "\n\n---\n📌 **법제처 국가법령정보센터 실시간 데이터 기준입니다**"

# ──────────────────────────────────────────
# 법령명 검색 쿼리 매핑
# (법제처 API의 law target은 법령명 기준 검색)
# ──────────────────────────────────────────
_LAW_QUERY_MAP: dict[str, list[str]] = {
    # 계약 관련 → 지방계약법 계열
    "수의계약":    ["지방계약", "국가계약"],
    "입찰":        ["지방계약", "국가계약", "조달"],
    "계약":        ["지방계약", "국가계약"],
    "경쟁입찰":    ["지방계약", "국가계약"],
    "낙찰":        ["지방계약", "국가계약"],
    "제한경쟁":    ["지방계약"],
    "지명경쟁":    ["지방계약"],
    "협상계약":    ["지방계약"],
    "적격심사":    ["지방계약", "국가계약"],
    "예정가격":    ["지방계약"],
    "원가":        ["지방계약"],
    "하자":        ["지방계약", "건설산업"],
    "하자보수":    ["지방계약", "건설산업"],
    "물품":        ["물품관리", "지방계약"],
    "공사":        ["건설산업", "지방계약"],
    "용역":        ["지방계약"],
    "감사":        ["감사원", "지방자치단체"],
    "조달":        ["조달사업", "국가계약"],
    "예산":        ["지방재정", "국가재정"],
    "보조금":      ["보조금", "지방재정"],
    # 설계·엔지니어링 → 엔지니어링산업진흥법 추가
    "사전타당성":  ["엔지니어링산업진흥법", "지방계약"],
    "타당성":      ["엔지니어링산업진흥법", "지방계약"],
    "기본계획":    ["엔지니어링산업진흥법", "지방계약"],
    "실시설계":    ["엔지니어링산업진흥법", "건설기술"],
    "설계":        ["엔지니어링산업진흥법", "건설기술", "지방계약"],
    "엔지니어링":  ["엔지니어링산업진흥법"],
    "기술용역":    ["엔지니어링산업진흥법", "지방계약"],
    "기술평가":    ["엔지니어링산업진흥법"],
    # 사회적 경제 주체 → 각 지원법
    "여성기업":    ["여성기업지원"],
    "장애인기업":  ["장애인기업활동"],
    "사회적기업":  ["사회적기업", "사회적경제"],
    "중소기업":    ["중소기업제품", "중소기업기본"],
    "지역업체":    ["지방계약", "중소기업제품"],
    "지역제품":    ["중소기업제품"],
}

def _derive_law_queries(query: str) -> list[str]:
    """사용자 쿼리에서 법령명 검색 키워드 파생"""
    derived = []
    q = query.lower()
    for keyword, law_names in _LAW_QUERY_MAP.items():
        if keyword in q:
            derived.extend(law_names)
    if not derived:
        # 매핑 없으면 쿼리 앞 2~3 어절을 그대로 사용
        derived = [query.split()[0]] if query.split() else [query]
    # 중복 제거
    seen = set()
    return [x for x in derived if not (x in seen or seen.add(x))]  # type: ignore[func-returns-value]


# ──────────────────────────────────────────
# 부산 조례 특화 검색어 매핑
# (일반 search_busan_ordinance 쿼리 보강용)
# ──────────────────────────────────────────
_BUSAN_ORDIN_KEYWORD_MAP: dict[str, str] = {
    "여성기업":    "여성기업 구매촉진",
    "장애인기업":  "장애인기업 구매촉진",
    "사회적기업":  "사회적경제",
    "사회적경제":  "사회적경제",
    "지역업체":    "지역제품 구매촉진",
    "지역제품":    "지역제품 구매촉진",
    "중소기업":    "중소기업제품 구매촉진",
    "수의계약":    "계약",
    "사전타당성":  "엔지니어링",
    "설계":        "건설",
    "하자":        "건설",
}


def _busan_ordin_query(query: str) -> str:
    """사용자 쿼리 → 부산 조례 검색에 최적화된 쿼리 반환"""
    for keyword, ordin_q in _BUSAN_ORDIN_KEYWORD_MAP.items():
        if keyword in query:
            return ordin_q
    return query


async def _fetch_busan_ordin_excerpts(
    query: str,
    ordin_items: list,
) -> list[dict]:
    """
    부산시 자치법규 검색 결과에서 상위 2개 조례의 실제 조문을 가져와
    쿼리와 관련된 조문을 발췌합니다.

    Returns:
        [{"name": ..., "link": ..., "relevant_articles": [...], "has_content": bool}, ...]
    """
    if not ordin_items:
        return []

    keywords = [w for w in query.split() if len(w) > 1]

    async def _fetch_one(item: dict) -> dict:
        serial = str(item.get("id", ""))
        name   = item.get("name", "")
        link   = item.get("link", "")
        if not serial:
            return {"name": name, "link": link, "relevant_articles": [], "has_content": False}
        try:
            content = await law_content(serial, "ordin")
        except Exception:
            return {"name": name, "link": link, "relevant_articles": [], "has_content": False}

        articles = content.get("articles", [])
        relevant = []
        for art in articles:
            art_text = art.get("text", "")
            # 쿼리 키워드 또는 관련 단어가 포함된 조문만 선택
            if any(kw in art_text for kw in keywords):
                relevant.append(art_text)

        # 관련 조문 없으면 앞 3조문 제공 (조례 전체 구조 파악용)
        if not relevant:
            relevant = [a["text"] for a in articles[:3]]

        return {
            "name":              name,
            "link":              link,
            "relevant_articles": relevant[:4],
            "has_content":       bool(articles),
        }

    tasks = [_fetch_one(item) for item in ordin_items[:2]]
    return list(await asyncio.gather(*tasks, return_exceptions=False))


# ──────────────────────────────────────────
# 일상어 → 법률 용어 매핑 사전
# ──────────────────────────────────────────
_TERM_MAP: dict[str, list[str]] = {
    # 계약 방식
    "그냥 계약":      ["수의계약", "소액 수의계약"],
    "직접 계약":      ["수의계약"],
    "동네 업체":      ["지역제한 입찰", "지역업체 우선 구매", "소재지 제한"],
    "동네 회사":      ["지역제한 입찰", "지역업체 우선 구매"],
    "아는 업체":      ["수의계약", "유일한 공급자"],
    "지인 업체":      ["수의계약", "이해충돌"],
    "나눠서 발주":    ["분할 발주", "분리 발주", "일괄 입찰"],
    "쪼개기":         ["분할 발주", "계약 분할"],
    "긴급":           ["긴급 수의계약", "재해·재난 수의계약"],
    "급해서":         ["긴급 수의계약"],
    "유일한 제품":    ["단일 규격", "특허 수의계약", "유일 공급자"],
    "특허 제품":      ["특허 수의계약"],
    # 입찰 방식
    "공개 입찰":      ["일반경쟁입찰"],
    "제한 입찰":      ["제한경쟁입찰"],
    "지명 입찰":      ["지명경쟁입찰"],
    "협상":           ["협상에 의한 계약", "협상계약"],
    # 금액
    "소액":           ["소액 수의계약", "소액 수의계약 한도"],
    "5천만원":        ["소액 수의계약 한도 (용역·물품)", "지방계약법 시행령 제25조"],
    "2억":            ["소액 수의계약 한도 (공사)", "지방계약법 시행령 제25조"],
    # 업체 자격
    "부산 업체":      ["부산 소재 업체", "지역업체 우선 구매", "지역제한 입찰"],
    "여성기업":       ["여성기업 우대", "여성기업지원에 관한 법률"],
    "장애인기업":     ["장애인기업 우대", "장애인기업활동 촉진법"],
    "사회적기업":     ["사회적기업 우선 구매", "사회적기업 육성법"],
    "중소기업":       ["중소기업 우선 구매", "중소기업제품 구매촉진법"],
    # 감사 관련
    "감사":           ["감사원 사전컨설팅", "감사 지적", "부당 특혜"],
    "적발":           ["감사 지적", "위법 계약"],
    "걸리면":         ["감사 지적", "제재 처분"],
    "책임":           ["계약 담당자 책임", "변상 책임"],
    # 하자
    "하자":           ["하자담보책임", "하자보수보증금"],
    "AS":             ["하자담보책임", "유지보수 계약"],
    # 기타
    "원산지":         ["원산지 확인", "외자 구매"],
    "해외 제품":      ["외자 구매", "국제입찰"],
    "주차장":         ["주차장 관리 위탁", "주차수요 조사 용역"],
    "청소":           ["환경미화 용역", "청소 위탁 계약"],
    "경비":           ["시설경비 용역", "청원경찰"],
    "IT":             ["정보시스템 구축", "소프트웨어 개발 용역"],
    "전산":           ["정보시스템 유지보수", "소프트웨어 유지관리"],
}


def _find_terms(plain: str) -> list[str]:
    """일상어에서 법률 용어 후보 추출"""
    found = []
    lower = plain.lower()
    for keyword, terms in _TERM_MAP.items():
        if keyword.lower() in lower or any(t.lower() in lower for t in terms):
            found.extend(terms)
    # 중복 제거·유지
    seen = set()
    unique = []
    for t in found:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


# ──────────────────────────────────────────
# 도구 1: search_legal_info
# ──────────────────────────────────────────
async def search_legal_info(
    query: str,
    include_admin_rule: bool = True,
    include_busan_ordinance: bool = True,
    display: int = 5,
) -> dict:
    """
    법령 + 행정규칙 + 부산시 조례를 동시에 조회하여 통합 결과를 반환합니다.

    Args:
        query:                  검색어 (예: "수의계약 범위", "용역 계약 한도")
        include_admin_rule:     행정규칙(조달청 고시 등) 포함 여부 (기본 True)
        include_busan_ordinance: 부산시 조례 포함 여부 (기본 True)
        display:                각 출처별 최대 결과 수 (기본 5)

    Returns:
        {"law": [...], "admrul": [...], "busan_ordin": [...], "summary": str}
    """
    # 법령명 검색은 쿼리 매핑 필요 (API가 법령명 기준으로만 검색)
    law_queries = _derive_law_queries(query)
    # 여러 법령명 쿼리를 병렬로 검색해서 합산
    law_coros = [law_search(query=lq, target="law", display=display) for lq in law_queries[:3]]

    tasks = {}
    if include_admin_rule:
        tasks["admrul"] = law_search(query=query, target="admrul", display=display)
    if include_busan_ordinance:
        # 쿼리 키워드에 맞는 부산 조례 검색어 사용
        busan_q = _busan_ordin_query(query)
        tasks["busan_ordin"] = search_busan_ordinance(query=busan_q, display=display)

    # 법령 + 나머지 병렬 실행
    extra_keys = list(tasks.keys())
    all_coros  = law_coros + list(tasks.values())
    all_results = await asyncio.gather(*all_coros, return_exceptions=True)

    # 법령 결과 합산 (중복 제거)
    law_items: list = []
    seen_law_ids: set = set()
    for res in all_results[:len(law_coros)]:
        if isinstance(res, Exception):
            continue
        for item in res.get("items", []):
            uid = item.get("id", item.get("name", ""))
            if uid and uid not in seen_law_ids:
                seen_law_ids.add(uid)
                law_items.append(item)

    output: dict = {k: [] for k in ["law", "admrul", "busan_ordin"]}
    output["law"] = law_items[:display]
    for k, res in zip(extra_keys, all_results[len(law_coros):]):
        if isinstance(res, Exception):
            output[k] = [{"error": str(res)}]
        else:
            output[k] = res.get("items", [])

    # 부산 조례 실제 조문 발췌 (추가 API 호출)
    busan_ordin_excerpts: list = []
    if include_busan_ordinance and output.get("busan_ordin"):
        busan_ordin_excerpts = await _fetch_busan_ordin_excerpts(query, output["busan_ordin"])
    output["busan_ordin_excerpts"] = busan_ordin_excerpts

    # 요약 텍스트
    law_names    = [i.get("name", "") for i in output["law"][:3]]
    admrul_names = [i.get("name", "") for i in output["admrul"][:3]]
    ordin_names  = [i.get("name", "") for i in output["busan_ordin"][:3]]

    summary_lines = [f"🔍 '{query}' 법령정보 통합 조회 결과\n"]
    if law_names:
        summary_lines.append("📗 현행법령: " + " / ".join(law_names))
    if admrul_names:
        summary_lines.append("📘 행정규칙: " + " / ".join(admrul_names))
    if ordin_names:
        summary_lines.append("📙 부산시 자치법규: " + " / ".join(ordin_names))
    if not any([law_names, admrul_names, ordin_names]):
        summary_lines.append("관련 법령·조례를 찾지 못했습니다. 검색어를 바꿔보세요.")

    # 부산 조례 조문 발췌 요약 포함
    if busan_ordin_excerpts:
        summary_lines.append("\n📋 부산시 조례 주요 조문:")
        for exc in busan_ordin_excerpts:
            if not exc.get("has_content"):
                continue
            summary_lines.append(f"\n  ▶ 【{exc['name']}】")
            for art_text in exc.get("relevant_articles", [])[:3]:
                # 조문 텍스트 요약 (너무 길면 자름)
                snippet = art_text[:250].replace("\n", " ")
                summary_lines.append(f"    • {snippet}")
            if exc.get("link"):
                summary_lines.append(f"    🔗 {exc['link']}")

    output["summary"] = "\n".join(summary_lines) + SOURCE_BADGE
    output["query"] = query
    return output


# ──────────────────────────────────────────
# 도구 2: get_audit_guard
# ──────────────────────────────────────────
async def get_audit_guard(
    situation: str,
    display: int = 5,
) -> dict:
    """
    계약 과정에서 감사 지적을 피할 수 있도록 관련 법령해석례·판례를 조회합니다.

    감사원 사전컨설팅은 별도 공식 API가 없으므로, 법제처 법령해석례에서
    유사 사례를 검색하고 실무 주의사항을 함께 제공합니다.

    Args:
        situation: 계약 상황 설명 (예: "긴급 수의계약 후 감사 지적 사례")
        display:   각 출처별 최대 결과 수

    Returns:
        {"interpretations": [...], "precedents": [...], "audit_guidance": str}
    """
    # 핵심 키워드만 추출 (짧을수록 검색 잘 됨)
    core_terms = _find_terms(situation)
    search_q = core_terms[0] if core_terms else situation.split()[0] if situation.split() else situation

    interp_task  = law_search(query=search_q, target="lsitItrprt", display=display)
    prec_task    = law_search(query=search_q, target="prec",       display=display)

    interp_res, prec_res = await asyncio.gather(interp_task, prec_task, return_exceptions=True)

    interpretations = interp_res.get("items", []) if not isinstance(interp_res, Exception) else []
    precedents      = prec_res.get("items",  []) if not isinstance(prec_res,  Exception) else []

    # 감사 실무 가이드라인 (법령 기반 고정 안내)
    audit_lines = [
        f"⚖️ '{situation}' — 감사 지적 예방 가이드\n",
        "【법령해석례 조회 결과】",
    ]
    if interpretations:
        for i, item in enumerate(interpretations[:3], 1):
            audit_lines.append(
                f"  {i}. [{item.get('org','?')} {item.get('date','')}] {item.get('name','')}"
            )
            if item.get("link"):
                audit_lines.append(f"     🔗 {item['link']}")
    else:
        audit_lines.append("  관련 법령해석례가 없습니다.")

    audit_lines.append("\n【판례 조회 결과】")
    if precedents:
        for i, item in enumerate(precedents[:3], 1):
            audit_lines.append(
                f"  {i}. [{item.get('court','?')} {item.get('date','')}] {item.get('name','')}"
                + (f" ({item.get('case_no','')})" if item.get("case_no") else "")
            )
            if item.get("link"):
                audit_lines.append(f"     🔗 {item['link']}")
    else:
        audit_lines.append("  관련 판례가 없습니다.")

    audit_lines.append(
        "\n【감사원 사전컨설팅 안내】\n"
        "  감사원 사전컨설팅은 공식 오픈API가 없습니다.\n"
        "  아래 경로로 직접 신청하시면 사전에 면책 근거를 확보할 수 있습니다:\n"
        "  🏛️  감사원 사전컨설팅 신청: https://www.bai.go.kr (사전컨설팅 메뉴)\n"
        "  📋  주요 면책 요건: ① 성실·선의의 의도 ② 법령 해석 불분명 ③ 이해관계 없음\n"
        "  💡  부산시 감사위원회 컨설팅도 병행 활용을 권장합니다."
    )

    return {
        "interpretations": interpretations,
        "precedents":      precedents,
        "audit_guidance":  "\n".join(audit_lines) + SOURCE_BADGE,
        "situation":       situation,
    }


# ──────────────────────────────────────────
# 도구 3: map_law_terms
# ──────────────────────────────────────────
async def map_law_terms(
    plain_text: str,
    fetch_articles: bool = True,
) -> dict:
    """
    일상 표현을 법률 용어로 매핑하고 관련 법령 조항을 안내합니다.

    Args:
        plain_text:    사용자 입력 (예: "동네 업체랑 수의계약 해도 돼?")
        fetch_articles: 매핑된 용어로 실제 법령 조회 여부 (기본 True)

    Returns:
        {"matched_terms": [...], "law_items": [...], "guidance": str}
    """
    matched = _find_terms(plain_text)

    # 매칭이 없으면 원문 그대로 검색
    search_queries = matched[:3] if matched else [plain_text]

    law_items = []
    if fetch_articles and search_queries:
        # 첫 번째 대표 용어로 현행법령 + 부산 조례 병렬 검색
        q = search_queries[0]
        law_res, ordin_res = await asyncio.gather(
            law_search(query=q, target="law",   display=3),
            search_busan_ordinance(query=q,     display=3),
            return_exceptions=True,
        )
        if not isinstance(law_res,   Exception):
            law_items.extend(law_res.get("items", []))
        if not isinstance(ordin_res, Exception):
            law_items.extend(ordin_res.get("items", []))

    # 안내 텍스트 구성
    guide_lines = [f"💬 입력: \"{plain_text}\"\n"]

    if matched:
        guide_lines.append("📌 법률 용어 매핑:")
        for term in matched[:8]:
            guide_lines.append(f"  → {term}")
    else:
        guide_lines.append("⚠️ 특정 법률 용어로 자동 매핑되지 않았습니다.")
        guide_lines.append("  아래 법령 검색 결과를 참고해 주세요.")

    if law_items:
        guide_lines.append("\n📗 관련 법령·조례:")
        for item in law_items[:5]:
            src  = item.get("source_type", "")
            name = item.get("name", "")
            link = item.get("link", "")
            region = f" ({item['region']})" if item.get("region") else ""
            guide_lines.append(f"  [{src}{region}] {name}")
            if link:
                guide_lines.append(f"   🔗 {link}")

    guide_lines.append(
        "\n💡 더 자세한 조문이 필요하면 'search_legal_info' 도구를 사용하거나\n"
        "   법제처 국가법령정보센터(www.law.go.kr)에서 직접 확인하세요."
    )

    return {
        "input":         plain_text,
        "matched_terms": matched,
        "law_items":     law_items,
        "guidance":      "\n".join(guide_lines) + SOURCE_BADGE,
    }
