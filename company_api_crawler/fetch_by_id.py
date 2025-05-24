#!/usr/bin/env python
"""Fetch company info from opendata.vip by business numbers."""
from __future__ import annotations

import argparse
import logging
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

USER_AGENT = "OP0523V3-Crawler/1.0 (+mailto:you@example.com)"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def query(comp_id: str, sess: requests.Session) -> List[dict]:
    try:
        r = sess.get(BASE, params={"keyword": comp_id}, timeout=10)
        r.raise_for_status()
        return r.json().get("output", [])
    except Exception as e:
        logging.warning("%s failed: %s", comp_id, e)
        return []


def crawl(ids: List[str]) -> pd.DataFrame:
    sess = build_session()
    rows: list[dict] = []
    for cid in tqdm(ids, desc="Query"):
        rows += query(cid, sess)
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


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch company info by business numbers")
    p.add_argument("ids", nargs="*", help="list of business numbers")
    p.add_argument("-f", "--file", help="file containing IDs, one per line")
    args = p.parse_args()

    ids = list(args.ids)
    if args.file:
        ids += [line.strip() for line in Path(args.file).read_text().splitlines() if line.strip()]
    if not ids:
        p.error("No IDs provided")

    df = crawl(ids)
    if df.empty:
        logging.error("No data fetched")
        return

    out = DATA_DIR / f"company_ids_{pd.Timestamp.now():%Y%m%d}.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    logging.info("Fetched %d records, saved to %s", len(df), out)


if __name__ == "__main__":
    main()
