from pathlib import Path

# General configuration for crawler
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
OUTPUT_DIR = Path(__file__).resolve().parents[1] / 'csv' / 'output'

REQUIRED_COLS = ("公司名稱", "電話", "Email", "官網")
KEYWORDS = ["廣告代理商", "品牌設計", "行銷公司", "保健品", "生技", "生物科技"]
CITIES = ["台北", "新北", "桃園", "台中", "台南", "高雄"]

HEADERS = {"User-Agent": USER_AGENT}

# hosts with problematic SSL; requests to these will skip certificate validation
SSL_SKIP_HOSTS = {"vsun.com.tw", "chuanyi1995.com"}

# domains to ignore when trying to extract emails
BAD_DOMAINS = {"blog.xuite.net", "pixnet.net"}
