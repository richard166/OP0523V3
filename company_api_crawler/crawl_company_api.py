#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
抓 opendata.vip/company
用法:
    python crawl_company_api.py 關鍵字1 關鍵字2 ...
"""
from __future__ import annotations
import argparse, json, logging, time
from pathlib import Path
from typing import List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

BASE = "https://opendata.vip/data/company"
DATA_DIR = Path(__file__).parent / "data" / "output"
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def query(keyword: str, sess: requests.Session) -> List[dict]:
    try:
        r = sess.get(BASE, params={"keyword": keyword}, timeout=10)
        r.raise_for_status()
        return r.json().get("output", [])
    except Exception as e:
        logging.warning("keyword %s 失敗：%s", keyword, e)
        return []

def crawl(keywords: List[str]) -> pd.DataFrame:
    sess = build_session()
    rows: list[dict] = []
    for kw in tqdm(keywords, desc="Query"):
        rows += query(kw, sess)
        time.sleep(0.3)       # 很輕量的 polite delay
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("keywords", nargs="+", help="關鍵字列表")
    args = parser.parse_args()

    df = crawl(args.keywords)
    if df.empty:
        logging.error("沒有撈到任何資料，請確認關鍵字")
        return

    out = DATA_DIR / f"company_{pd.Timestamp.now():%Y%m%d}.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    logging.info("共抓到 %d 筆，已存 %s", len(df), out)

if __name__ == "__main__":
    main()
