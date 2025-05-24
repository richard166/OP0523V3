import os
from pathlib import Path

# General configuration for crawler
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'
OUTPUT_DIR = Path(__file__).resolve().parents[1] / 'csv' / 'output'

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

REQUIRED_COLS = ("公司名稱", "電話", "Email", "官網")

KEYWORDS = ["廣告代理商", "品牌設計", "行銷公司"]
CITIES = ["台北", "新北", "桃園", "台中", "台南", "高雄"]

HEADERS = {"User-Agent": USER_AGENT}
