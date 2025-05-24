import logging
import time
from typing import List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

from .. import config

BASE = "https://opendata.vip/data/company"


def _build_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.headers.update(config.HEADERS)
    return sess


def _query(keyword: str, sess: requests.Session) -> list[dict]:
    try:
        resp = sess.get(BASE, params={"keyword": keyword}, timeout=10)
        resp.raise_for_status()
        return resp.json().get("output", [])
    except Exception as e:
        logging.warning("keyword %s failed: %s", keyword, e)
        return []


def crawl(keywords: List[str] | None = None) -> pd.DataFrame:
    """Crawl company data from opendata.vip API."""
    if keywords is None:
        keywords = config.KEYWORDS
    sess = _build_session()
    rows: list[dict] = []
    for kw in tqdm(keywords, desc="Query"):
        rows += _query(kw, sess)
        time.sleep(0.3)  # polite delay
    if not rows:
        return pd.DataFrame()
    df = pd.json_normalize(rows).drop_duplicates("Business_Accounting_NO")
    df = df.rename(
        columns={
            "Business_Accounting_NO": "統編",
            "Company_Name": "公司名稱",
            "Company_Location": "地址",
            "Responsible_Name": "負責人",
            "Capital_Stock_Amount": "資本額",
        }
    )
    keep = ["統編", "公司名稱", "負責人", "地址", "資本額", "Company_Status_Desc"]
    return df.loc[:, [c for c in keep if c in df.columns]]
