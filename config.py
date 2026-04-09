import os

# 공공데이터포털 서비스 키
SERVICE_KEY = os.environ.get(
    "NARA_SERVICE_KEY",
    "7db9147f9b8e4eff41f27653c73002ec2d3d4054d863a74ae1e7fdd561a7ea62"
)

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
