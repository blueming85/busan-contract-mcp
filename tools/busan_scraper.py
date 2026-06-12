"""
부산시청 계약정보공개시스템 스크래퍼
대상: https://www.busan.go.kr/depart/abpcontract?schCtrtkindcd=1 (수의계약)

사용법:
  - 단독 실행 (5년치 초기 적재):
      python -m tools.busan_scraper --years 5
  - MCP 도구로 호출:
      from tools.busan_scraper import search_busan_local, run_scraper

데이터 저장: data/busan_contracts.json
"""

import gzip
import json
import logging
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 경로 / 상수
# ─────────────────────────────────────────────
DATA_FILE = Path(__file__).parent.parent / "data" / "busan_contracts.json"

BASE_URL = "https://www.busan.go.kr"
LIST_URL = f"{BASE_URL}/depart/abpcontract"   # ?schCtrtkindcd=N&curPage=N

# 계약종류 코드 (부산시청 계약정보공개시스템)
KIND_NAMES = {1: "공사", 2: "용역", 3: "물품"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ─────────────────────────────────────────────
# 드라이버 초기화
# ─────────────────────────────────────────────

def _make_driver():
    """headless Chrome WebDriver 생성"""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--user-agent={HEADERS['User-Agent']}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


# ─────────────────────────────────────────────
# 페이지 파싱
# ─────────────────────────────────────────────

def _parse_amount(text: str) -> int:
    """'1,234,567' → 1234567"""
    try:
        return int(re.sub(r"[^\d]", "", text))
    except Exception:
        return 0


def _parse_list_page(html: str) -> list[dict]:
    """
    목록 페이지 HTML → 계약 항목 리스트
    부산시 계약정보 테이블 구조:
    th: 번호 | 계약건명 | 계약방법 | 계약상대자 | 계약금액 | 계약기간 | 담당부서
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # 테이블 찾기 (여러 선택자 fallback)
    table = (
        soup.select_one("table.boardList")   # 부산시 실제 클래스
        or soup.select_one("table.board_list")
        or soup.select_one("table.list_table")
        or soup.select_one(".board_wrap table")
        or soup.select_one(".cont_wrap table")
        or soup.select_one("table")
    )
    if not table:
        return rows

    # thead에서 컬럼명 추출
    headers = []
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]

    tbody = table.find("tbody")
    if not tbody:
        return rows

    for tr in tbody.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells or len(cells) < 3:
            continue

        # 컬럼명이 있으면 매핑, 없으면 위치 기반 추정
        row = _map_row(headers, cells, tr)
        if row:
            rows.append(row)

    return rows


def _map_row(headers: list[str], cells: list[str], tr) -> Optional[dict]:
    """헤더-셀 매핑 → 정규화된 dict"""
    from bs4 import Tag

    # 빈 행 / 헤더 반복 행 제거
    if not any(cells):
        return None
    if cells[0] in ("번호", "No", "순번"):
        return None

    # 위치 기반 매핑 (부산시 계약정보 표준 컬럼 순서)
    # 번호 | 계약건명 | 계약방법 | 계약상대자 | 계약금액 | 계약기간 | 담당부서
    mapping = {
        "계약건명": None,
        "계약방법": None,
        "업체명":   None,   # 계약상대자
        "계약금액": None,
        "계약기간": None,
        "담당부서": None,
        "계약일자": None,
    }

    col_aliases = {
        "계약건명": ["계약건명", "사업명", "계약명"],
        "계약방법": ["계약방법", "방법"],
        "업체명":   ["계약상대자", "업체명", "상대자", "업체"],
        "계약금액": ["계약금액", "금액", "계약액"],
        "계약기간": ["계약기간", "기간"],
        "담당부서": ["담당부서", "부서", "소관부서"],
        "계약일자": ["계약일자", "계약일", "체결일"],
    }

    if headers:
        for field, aliases in col_aliases.items():
            for alias in aliases:
                for i, h in enumerate(headers):
                    if alias in h and i < len(cells):
                        mapping[field] = cells[i]
                        break
                if mapping[field] is not None:
                    break
    else:
        # 헤더 없을 때 위치로 추정 (번호, 건명, 방법, 상대자, 금액, 기간, 부서)
        pos = {0: "번호", 1: "계약건명", 2: "계약방법", 3: "업체명",
               4: "계약금액", 5: "계약기간", 6: "담당부서"}
        for idx, field in pos.items():
            if idx < len(cells) and field != "번호":
                mapping[field] = cells[idx]

    # 계약건명이 없으면 유효하지 않은 행
    if not mapping.get("계약건명"):
        return None

    # 상세 링크 추출
    detail_url = ""
    a_tag = tr.find("a", href=True)
    if a_tag and isinstance(a_tag, Tag):
        href = a_tag.get("href", "")
        if href and not href.startswith("javascript"):
            from urllib.parse import urljoin
            detail_url = urljoin(BASE_URL, str(href))

    # 금액 정수 변환
    raw_amt = mapping.get("계약금액") or ""
    amount_int = _parse_amount(raw_amt)

    # 날짜 추출 (계약기간에서 시작일 뽑기)
    contract_date = mapping.get("계약일자") or ""
    if not contract_date and mapping.get("계약기간"):
        m = re.search(r"(\d{4}[-./]\d{1,2}[-./]\d{1,2})", mapping["계약기간"])
        if m:
            contract_date = m.group(1).replace(".", "-").replace("/", "-")

    return {
        "계약건명":   mapping.get("계약건명", ""),
        "계약방법":   mapping.get("계약방법", ""),
        "업체명":     mapping.get("업체명", ""),
        "계약금액":   raw_amt,
        "계약금액_원": amount_int,
        "계약기간":   mapping.get("계약기간", ""),
        "계약일자":   contract_date,
        "담당부서":   mapping.get("담당부서", ""),
        "상세URL":    detail_url,
        "수집일시":   datetime.now().strftime("%Y-%m-%d"),
        "출처":       "부산시청 계약정보공개시스템",
    }


def _get_total_pages(html: str) -> int:
    """페이지네이션에서 마지막 페이지 번호 추출"""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    max_page = 1

    # 부산시 실제 구조: <a href="?curPage=N&schCtrtkindcd=1"> 형태의 일반 링크
    # href에 curPage가 있는 모든 <a> 태그를 긁어서 최대값 추출
    for a in soup.find_all("a", href=True):
        href = str(a.get("href", ""))
        m = re.search(r"curPage=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))

    # onclick 패턴 fallback
    for a in soup.find_all("a", onclick=True):
        onclick = str(a.get("onclick", ""))
        m = re.search(r"[Pp]age\D*(\d+)", onclick)
        if m:
            max_page = max(max_page, int(m.group(1)))

    return max(max_page, 1)


# ─────────────────────────────────────────────
# 날짜 필터
# ─────────────────────────────────────────────

def _is_within_years(item: dict, years: int) -> bool:
    """계약일자가 years년 이내인지 확인"""
    date_str = item.get("계약일자", "") or item.get("계약기간", "")
    if not date_str:
        return True  # 날짜 불명은 포함
    m = re.search(r"(\d{4})", date_str)
    if not m:
        return True
    year = int(m.group(1))
    cutoff = datetime.now().year - years
    return year >= cutoff


# ─────────────────────────────────────────────
# 로컬 DB 로드 / 저장
# ─────────────────────────────────────────────

DATA_FILE_GZ = DATA_FILE.with_name(DATA_FILE.name + ".gz")

_db_cache: Optional[dict] = None
_db_cache_key: Optional[tuple] = None


def _load_db() -> dict:
    """data/busan_contracts.json(.gz) 로드 — 파일 mtime 기반 캐시 (15MB 반복 파싱 방지)"""
    global _db_cache, _db_cache_key

    if DATA_FILE.exists():
        src = DATA_FILE
    elif DATA_FILE_GZ.exists():
        src = DATA_FILE_GZ
    else:
        return {"last_updated": "", "total_count": 0, "items": []}

    cache_key = (str(src), src.stat().st_mtime)
    if _db_cache is not None and _db_cache_key == cache_key:
        return _db_cache

    try:
        opener = gzip.open if src.suffix == ".gz" else open
        with opener(src, "rt", encoding="utf-8") as f:
            db = json.load(f)
    except Exception:
        return {"last_updated": "", "total_count": 0, "items": []}

    _db_cache, _db_cache_key = db, cache_key
    return db


def _save_db(db: dict):
    global _db_cache, _db_cache_key
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    db["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db["total_count"] = len(db["items"])
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    # 배포용 압축본도 함께 갱신 (git에는 .gz만 커밋)
    with gzip.open(DATA_FILE_GZ, "wt", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)
    _db_cache, _db_cache_key = None, None


def _dedupe_key(item: dict) -> str:
    """중복 제거 키: 계약종류 + 계약건명 + 업체명 + 금액"""
    return f"{item.get('계약종류','')}|{item.get('계약건명','')}|{item.get('업체명','')}|{item.get('계약금액_원',0)}"


# ─────────────────────────────────────────────
# 스크래퍼 메인
# ─────────────────────────────────────────────

def run_scraper(
    years: int = 5,
    max_pages: int = 0,
    verbose: bool = True,
    kind_codes: Optional[list] = None,
) -> dict:
    """
    부산시 수의계약 목록 스크래핑

    Args:
        years:      소급 연수 (기본 5년). 0 = 전체
        max_pages:  최대 페이지 수 (0 = 자동 감지)
        verbose:    진행 상황 출력
        kind_codes: 계약종류 코드 목록 (기본 [1,2,3] = 공사+용역+물품)
                    1=공사, 2=용역, 3=물품

    Returns:
        {"new": int, "existing": int, "total": int}
    """
    from selenium.webdriver.support.ui import WebDriverWait

    if kind_codes is None:
        kind_codes = [1, 2, 3]

    db = _load_db()
    existing_keys = {_dedupe_key(i) for i in db["items"]}
    new_items = []

    driver = _make_driver()

    try:
        for kind in kind_codes:
            kind_name = KIND_NAMES.get(kind, str(kind))
            wait = WebDriverWait(driver, 30)

            # 1페이지 로드 → 총 페이지 수 확인
            url_p1 = f"{LIST_URL}?schCtrtkindcd={kind}&curPage=1"
            driver.get(url_p1)
            wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(random.uniform(1.5, 2.5))

            total_pages = _get_total_pages(driver.page_source)
            if max_pages and max_pages < total_pages:
                total_pages = max_pages

            if verbose:
                print(f"\n[부산스크래퍼] [{kind_name}] 총 {total_pages}페이지 수집 시작 (최근 {years}년)")

            stop_early = False
            kind_new = 0
            for page in range(1, total_pages + 1):
                if page > 1:
                    url = f"{LIST_URL}?schCtrtkindcd={kind}&curPage={page}"
                    driver.get(url)
                    wait.until(
                        lambda d: d.execute_script("return document.readyState") == "complete"
                    )
                    try:
                        WebDriverWait(driver, 5).until(
                            lambda d: d.execute_script(
                                "return typeof jQuery !== 'undefined' ? jQuery.active == 0 : true"
                            )
                        )
                    except Exception:
                        pass
                    time.sleep(random.uniform(1.0, 2.0))

                rows = _parse_list_page(driver.page_source)

                # 각 row에 계약종류 주입
                for row in rows:
                    row["계약종류"] = kind_name

                page_new = 0
                for row in rows:
                    if years > 0 and not _is_within_years(row, years):
                        stop_early = True
                        continue

                    key = _dedupe_key(row)
                    if key not in existing_keys:
                        existing_keys.add(key)
                        new_items.append(row)
                        page_new += 1
                        kind_new += 1

                if verbose:
                    print(f"  [{kind_name}] 페이지 {page:3d}/{total_pages} — 신규 {page_new}건")

                if stop_early and page_new == 0:
                    if verbose:
                        print(f"  → {years}년 이전 데이터 진입, 조기 종료")
                    break

            if verbose:
                print(f"  [{kind_name}] 소계: 신규 {kind_new}건")

    finally:
        driver.quit()

    # DB에 새 항목 앞에 추가 (최신순 유지)
    db["items"] = new_items + db["items"]
    _save_db(db)

    result = {
        "new":      len(new_items),
        "existing": len(db["items"]) - len(new_items),
        "total":    len(db["items"]),
    }
    if verbose:
        print(f"\n[완료] 신규 {result['new']}건 추가 / 누적 {result['total']}건")
    return result


# ─────────────────────────────────────────────
# MCP 도구: 로컬 DB 조회
# ─────────────────────────────────────────────

def search_busan_local(
    keyword: Optional[str] = None,
    company: Optional[str] = None,
    min_amount: Optional[int] = None,
    max_amount: Optional[int] = None,
    years: int = 5,
    top_n: int = 20,
    kind: Optional[str] = None,  # "공사" | "용역" | "물품" | None=전체
) -> dict:
    """
    로컬 부산시 계약 DB 조회 (스크래핑 후 사용)

    Args:
        keyword:    계약건명 키워드
        company:    업체명 키워드
        min_amount: 최소 금액 (원)
        max_amount: 최대 금액 (원)
        years:      최근 N년 이내 (0=전체)
        top_n:      최대 결과 수

    Returns:
        {"db_date": str, "total_db": int, "matched": int, "items": [...]}
    """
    db = _load_db()
    items = db.get("items", [])

    if not items:
        return {
            "db_date": db.get("last_updated", ""),
            "total_db": 0,
            "matched": 0,
            "items": [],
            "notice": (
                "로컬 DB가 비어있습니다. "
                "먼저 'python -m tools.busan_scraper --years 5' 를 실행해 데이터를 수집하세요."
            ),
        }

    # 필터링 (캐시된 원본을 정렬로 변형하지 않도록 복사본 사용)
    result = list(items)
    if years > 0:
        result = [i for i in result if _is_within_years(i, years)]
    if kind:
        result = [i for i in result if kind in (i.get("계약종류") or "")]
    if keyword:
        kw = keyword.lower()
        result = [i for i in result if kw in (i.get("계약건명") or "").lower()]
    if company:
        co = company.lower()
        result = [i for i in result if co in (i.get("업체명") or "").lower()]
    if min_amount is not None:
        result = [i for i in result if i.get("계약금액_원", 0) >= min_amount]
    if max_amount is not None:
        result = [i for i in result if i.get("계약금액_원", 0) <= max_amount]

    # 금액 기준 정렬
    result.sort(key=lambda x: x.get("계약금액_원", 0), reverse=True)

    return {
        "db_date":  db.get("last_updated", ""),
        "total_db": len(items),
        "matched":  len(result),
        "items":    result[:top_n],
    }


def rank_companies_busan_local(
    keyword: Optional[str] = None,
    years: int = 5,
    top_n: int = 10,
) -> dict:
    """
    로컬 DB 기반 업체별 수의계약 순위

    Args:
        keyword: 계약건명 키워드 (없으면 전체)
        years:   최근 N년
        top_n:   상위 N개 업체

    Returns:
        {"db_date": str, "ranking": [{업체명, 계약횟수, 합계금액, ...}]}
    """
    data = search_busan_local(keyword=keyword, years=years, top_n=99999)
    items = data.get("items", [])

    company_map: dict[str, dict] = {}
    for item in items:
        corp = item.get("업체명", "").strip()
        if not corp or corp in ("", "-", "—"):
            continue
        if corp not in company_map:
            company_map[corp] = {
                "업체명":   corp,
                "계약횟수": 0,
                "합계금액": 0,
                "최근계약": "",
                "계약목록": [],
            }
        c = company_map[corp]
        c["계약횟수"] += 1
        c["합계금액"] += item.get("계약금액_원", 0)
        date = item.get("계약일자", "")
        if date > c["최근계약"]:
            c["최근계약"] = date
        c["계약목록"].append(item.get("계약건명", ""))

    ranking = sorted(company_map.values(), key=lambda x: x["합계금액"], reverse=True)

    # 금액 포맷
    for r in ranking:
        amt = r["합계금액"]
        if amt >= 100_000_000:
            r["합계금액_표시"] = f"{amt/100_000_000:.1f}억원"
        elif amt >= 10_000:
            r["합계금액_표시"] = f"{amt//10_000}만원"
        else:
            r["합계금액_표시"] = f"{amt:,}원"

    return {
        "db_date": data.get("db_date", ""),
        "keyword": keyword or "전체",
        "years":   years,
        "ranking": ranking[:top_n],
    }


# ─────────────────────────────────────────────
# 단독 실행
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="부산시 수의계약 스크래퍼")
    parser.add_argument("--years",     type=int, default=5,  help="소급 연수 (기본: 5)")
    parser.add_argument("--max-pages", type=int, default=0,  help="최대 페이지 (0=전체)")
    parser.add_argument("--query",     type=str, default="", help="수집 후 즉시 조회할 키워드")
    parser.add_argument(
        "--kinds", type=str, default="1,2,3",
        help="계약종류 코드 (기본: 1,2,3 = 공사+용역+물품, 예: --kinds 2 는 용역만)"
    )
    args = parser.parse_args()

    kind_codes = [int(k.strip()) for k in args.kinds.split(",") if k.strip().isdigit()]
    run_scraper(years=args.years, max_pages=args.max_pages, kind_codes=kind_codes)

    if args.query:
        print(f"\n[조회] '{args.query}'")
        result = search_busan_local(keyword=args.query, years=args.years)
        for item in result["items"][:10]:
            print(f"  {item['업체명']:20s}  {item['계약금액']:>15s}  {item['계약건명'][:40]}")
