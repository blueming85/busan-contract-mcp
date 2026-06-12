"""
법제처 오픈API 비동기 클라이언트 (open.law.go.kr)

검증된 실제 응답 구조 기반 (2026년 4월 테스트):
  target   root_key       items_key   검색 가능
  law      LawSearch      law         O (section=all 필수)
  admrul   AdmRulSearch   admrul      O
  ordin    OrdinSearch    law         O (자치법규도 'law' 키)
  prec     PrecSearch     prec        O
  lsitItrprt              —           API 미지원(빈 응답)
"""
import asyncio
import json
import urllib.request
import urllib.parse
from typing import Optional

try:
    import xmltodict
    _HAS_XMLTODICT = True
except ImportError:
    _HAS_XMLTODICT = False

LAW_SEARCH_URL  = "https://www.law.go.kr/DRF/lawSearch.do"
LAW_SERVICE_URL = "https://www.law.go.kr/DRF/lawService.do"
LAW_INFO_BASE   = "https://www.law.go.kr"

# AI가 읽을 조문 최대 글자 수
MAX_ARTICLE_CHARS = 4000

# target → (root_key, items_key)
_TARGET_KEYS = {
    "law":        ("LawSearch",    "law"),
    "admrul":     ("AdmRulSearch", "admrul"),
    "ordin":      ("OrdinSearch",  "law"),   # 자치법규도 items_key="law"
    "prec":       ("PrecSearch",   "prec"),
}

SOURCE_LABELS = {
    "law":    "현행법령",
    "admrul": "행정규칙",
    "ordin":  "자치법규",
    "prec":   "판례",
}


# ──────────────────────────────────────────
# 인증키
# ──────────────────────────────────────────
def _oc() -> str:
    from config import LAW_API_OC
    if not LAW_API_OC:
        raise RuntimeError(".env 파일에 LAW_API_OC를 설정해주세요.")
    return LAW_API_OC


# ──────────────────────────────────────────
# 공개 링크 생성 (API 키 미노출)
# ──────────────────────────────────────────
def _public_link(target: str, item: dict) -> str:
    """target + 일련번호로 법제처 공개 URL 생성 (API 키 불포함)"""
    if target == "law":
        seq = item.get("법령일련번호", "")
        if seq:
            return f"https://www.law.go.kr/lsInfoP.do?lsiSeq={seq}"
    elif target == "admrul":
        seq = item.get("행정규칙일련번호", "")
        if seq:
            return f"https://www.law.go.kr/admRulInfoP.do?admRulSeq={seq}"
    elif target == "ordin":
        seq = item.get("자치법규일련번호", "")
        if seq:
            return f"https://www.law.go.kr/ordinInfoP.do?ordinSeq={seq}"
    elif target == "prec":
        seq = item.get("판례일련번호", "")
        if seq:
            return f"https://www.law.go.kr/precInfoP.do?precSeq={seq}"
    return ""


# ──────────────────────────────────────────
# 응답 정규화
# ──────────────────────────────────────────
def _to_list(val) -> list:
    if val is None:
        return []
    if isinstance(val, dict):
        return [val]
    return list(val)


def _parse_raw(raw: bytes) -> dict:
    text = raw.decode("utf-8")
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if _HAS_XMLTODICT:
            return xmltodict.parse(text)
        raise RuntimeError(
            "API가 XML을 반환했습니다. 'pip install xmltodict'를 실행해주세요."
        )


def _norm_search(data: dict, target: str) -> dict:
    """API 응답 → 통일된 JSON 구조"""
    root_key, items_key = _TARGET_KEYS.get(target, ("LawSearch", target))
    root  = data.get(root_key, {})
    total = int(root.get("totalCnt", 0) or 0)
    raw_items = _to_list(root.get(items_key))
    items = []
    src = SOURCE_LABELS.get(target, target)

    if target == "law":
        for r in raw_items:
            items.append({
                "source_type":    src,
                "id":             r.get("법령일련번호", r.get("id", "")),
                "law_id":         r.get("법령ID", ""),
                "name":           r.get("법령명한글", ""),
                "abbr":           r.get("법령약칭명", ""),
                "kind":           r.get("법령구분명", ""),
                "ministry":       r.get("소관부처명", ""),
                "effective_date": r.get("시행일자", ""),
                "link":           _public_link("law", r),
            })

    elif target == "admrul":
        for r in raw_items:
            items.append({
                "source_type":    src,
                "id":             r.get("행정규칙일련번호", r.get("id", "")),
                "admrul_id":      r.get("행정규칙ID", ""),
                "name":           r.get("행정규칙명", ""),
                "kind":           r.get("행정규칙종류", ""),
                "ministry":       r.get("소관부처명", ""),
                "effective_date": r.get("시행일자", r.get("발령일자", "")),
                "link":           _public_link("admrul", r),
            })

    elif target == "ordin":
        for r in raw_items:
            items.append({
                "source_type":    src,
                "id":             r.get("자치법규일련번호", r.get("id", "")),
                "ordin_id":       r.get("자치법규ID", ""),
                "name":           r.get("자치법규명", ""),
                "kind":           r.get("자치법규종류", ""),
                "region":         r.get("지자체기관명", ""),
                "effective_date": r.get("시행일자", ""),
                "link":           _public_link("ordin", r),
            })

    elif target == "prec":
        for r in raw_items:
            items.append({
                "source_type": src,
                "id":          r.get("판례일련번호", r.get("id", "")),
                "name":        r.get("사건명", ""),
                "court":       r.get("법원명", ""),
                "date":        r.get("선고일자", ""),
                "case_no":     r.get("사건번호", ""),
                "kind":        r.get("사건종류명", ""),
                "link":        _public_link("prec", r),
            })

    return {
        "totalCount":  total,
        "items":       items,
        "source_type": src,
    }


# ──────────────────────────────────────────
# 법령 본문 정규화
# ──────────────────────────────────────────
def _norm_content(data: dict, target: str) -> dict:
    result = {"name": "", "articles": [], "full_text": "", "summary": ""}

    if target == "law":
        law  = data.get("법령", {})
        info = law.get("기본정보", {})
        result["name"]           = info.get("법령명_한글", "")
        result["effective_date"] = info.get("시행일자", "")

        조문단위 = _to_list(law.get("조문", {}).get("조문단위"))
        articles = []
        for art in 조문단위[:60]:
            no      = art.get("조문번호", "")
            title   = art.get("조문제목", "")
            content = art.get("조문내용", "") or ""
            항_lines = []
            for 항 in _to_list(art.get("항")):
                항_lines.append(f"  ①{항.get('항번호','')} {항.get('항내용','')}")
                for 호 in _to_list(항.get("호")):
                    항_lines.append(f"    {호.get('호번호','')} {호.get('호내용','')}")
            text = (
                f"제{no}조({title}) {content}"
                + ("\n" + "\n".join(항_lines) if 항_lines else "")
            ).strip()
            articles.append({"no": no, "title": title, "text": text})

        result["articles"] = articles
        full = "\n\n".join(a["text"] for a in articles)
        result["full_text"] = full
        result["summary"]   = _truncate(full)

    elif target == "ordin":
        ordin = data.get("자치법규", {})
        info  = ordin.get("기본정보", {})
        result["name"]           = info.get("자치법규명", "")
        result["effective_date"] = info.get("시행일자", "")

        조문단위 = _to_list(ordin.get("조문", {}).get("조문단위"))
        articles = []
        for art in 조문단위[:60]:
            no      = art.get("조문번호", "")
            title   = art.get("조문제목", "")
            content = art.get("조문내용", "") or ""
            text = f"제{no}조({title}) {content}".strip()
            articles.append({"no": no, "title": title, "text": text})

        result["articles"] = articles
        full = "\n\n".join(a["text"] for a in articles)
        result["full_text"] = full
        result["summary"]   = _truncate(full)

    else:
        text = json.dumps(data, ensure_ascii=False)
        result["full_text"] = text
        result["summary"]   = _truncate(text)

    return result


def _truncate(text: str) -> str:
    if len(text) <= MAX_ARTICLE_CHARS:
        return text
    return text[:MAX_ARTICLE_CHARS] + "\n…(이하 생략 — 전문은 법제처 국가법령정보센터 참조)"


# ──────────────────────────────────────────
# 동기 HTTP 호출
# ──────────────────────────────────────────
def _get_raw(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "BusanContractMCP/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def _sync_search(oc: str, target: str, query: str, page: int, display: int) -> dict:
    params: dict = {
        "OC": oc, "target": target, "type": "JSON",
        "query": query, "page": page, "display": display,
    }
    # law 검색은 section=all 없이는 0건 반환
    if target == "law":
        params["section"] = "all"

    url  = LAW_SEARCH_URL + "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    raw  = _get_raw(url)
    if not raw.strip():
        return {"totalCount": 0, "items": [], "source_type": SOURCE_LABELS.get(target, target)}
    data = _parse_raw(raw)
    return _norm_search(data, target)


def _sync_content(oc: str, target: str, law_serial: str) -> dict:
    """법령 본문 조회 — MST 파라미터(일련번호) 사용"""
    params = {
        "OC": oc, "target": target,
        "MST": law_serial, "type": "JSON",
    }
    url  = LAW_SERVICE_URL + "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    raw  = _get_raw(url)
    data = _parse_raw(raw)
    return _norm_content(data, target)


# ──────────────────────────────────────────
# 비동기 공개 API
# ──────────────────────────────────────────
async def law_search(
    query: str,
    target: str = "law",
    page: int = 1,
    display: int = 10,
) -> dict:
    """
    법제처 법령 검색 (비동기)

    Args:
        query:   검색어 (예: "수의계약", "지방계약법 시행령")
        target:  law / admrul / ordin / prec
        page:    페이지 번호
        display: 결과 수 (최대 100)
    """
    oc = _oc()
    return await asyncio.to_thread(_sync_search, oc, target, query, page, display)


async def law_content(law_serial: str, target: str = "law") -> dict:
    """
    법령 본문 조회 (비동기)

    Args:
        law_serial: 법령일련번호 (law_search 결과의 id 필드)
        target:     law / ordin / admrul
    """
    oc = _oc()
    return await asyncio.to_thread(_sync_content, oc, target, law_serial)


async def search_busan_ordinance(query: str, display: int = 5) -> dict:
    """부산광역시 자치법규만 검색 (편의 함수)"""
    result = await law_search(query=f"부산 {query}", target="ordin", display=display * 2)
    busan_items = [i for i in result["items"] if "부산" in i.get("region", "")]
    result["items"] = busan_items[:display] if busan_items else result["items"][:display]
    return result


async def search_multi_targets(query: str, targets: list, display: int = 5) -> list:
    """여러 검색 대상을 병렬 조회 후 합산"""
    coros = [law_search(query=query, target=t, display=display) for t in targets]
    results = await asyncio.gather(*coros, return_exceptions=True)
    merged = []
    for res in results:
        if not isinstance(res, Exception):
            merged.extend(res.get("items", []))
    return merged
