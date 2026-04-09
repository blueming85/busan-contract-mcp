import os
from pathlib import Path

# .env 파일 자동 로드 (python-dotenv 없이 직접 파싱)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# 공공데이터포털 서비스 키
SERVICE_KEY = os.environ.get("NARA_SERVICE_KEY", "")
if not SERVICE_KEY:
    raise RuntimeError(".env 파일에 NARA_SERVICE_KEY를 설정해주세요.")

# 나라장터 API Base URL
BASE_URL = "http://apis.data.go.kr/1230000"

# 서비스별 엔드포인트
ENDPOINTS = {
    "contract":  f"{BASE_URL}/ao/CntrctInfoService",
    "bid":       f"{BASE_URL}/ad/BidPublicInfoService",
    "user":      f"{BASE_URL}/ao/UsrInfoService02",
    "price":     f"{BASE_URL}/ao/PriceInfoService",
    "industry":  f"{BASE_URL}/ao/IndstrytyBaseLawrgltInfoService",
    # 신규 (2026년 활용신청)
    "award":     f"{BASE_URL}/as/ScsbidInfoService",           # 낙찰정보서비스
    "process":   f"{BASE_URL}/ao/CntrctProcssIntgOpenService", # 계약과정통합공개서비스
}

# 기본 페이지 크기
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 999
