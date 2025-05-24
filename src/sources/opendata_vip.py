import logging
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .. import config

BASE = "https://www.opendata.vip"
LIST_URL = BASE + "/tool/company"
INFO_PREFIX = BASE + "/tool/companyInfo/"


def _build_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(config.HEADERS)
    return sess


def _fetch_list(keyword: str, sess: requests.Session) -> list[dict]:
    try:
        resp = sess.get(LIST_URL, params={"keyword": keyword}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logging.warning("list %s failed: %s", keyword, e)
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    rows: list[dict] = []
    tbody = soup.find("tbody")
    if not tbody:
        return rows
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue
        comp_id = tds[1].get_text(strip=True)
        name = tds[2].get_text(strip=True)
        owner = tds[4].get_text(strip=True)
        addr = tds[6].get_text(strip=True)
        a = tds[0].find("a", href=True)
        info_url = urljoin(BASE, a["href"]) if a else urljoin(BASE, f"/tool/companyInfo/{comp_id}")
        rows.append({
            "公司名稱": name,
            "統編": comp_id,
            "負責人": owner,
            "地址": addr,
            "info_url": info_url,
            "分類": keyword,
        })
    return rows


def _fetch_detail(url: str, sess: requests.Session) -> tuple[str | None, str | None, str | None]:
    try:
        resp = sess.get(url, timeout=3)
        resp.raise_for_status()
    except Exception as e:
        logging.warning("detail fetch failed: %s", e)
        return None, None, None
    soup = BeautifulSoup(resp.text, "html.parser")
    phone = email = website = None
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        key = th.get_text(strip=True)
        val = td.get_text(strip=True)
        if "電話" in key:
            phone = val or None
        elif "mail" in key.lower() or "email" in key.lower():
            email = val or None
        elif "網址" in key or "網站" in key:
            a = td.find("a", href=True)
            website = a["href"] if a else val or None
    return phone, email, website


def crawl(keyword: str) -> pd.DataFrame:
    sess = _build_session()
    rows = _fetch_list(keyword, sess)
    for row in rows:
        phone, email, website = _fetch_detail(row.pop("info_url"), sess)
        row.update({"電話": phone, "Email": email, "官網": website})
    df = pd.DataFrame(rows)
    return df

