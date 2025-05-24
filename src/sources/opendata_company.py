import logging
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from .. import utils, config

BASE = "https://opendata.vip"
LIST_URL = BASE + "/tool/company"
INFO_PREFIX = BASE + "/tool/companyInfo/"

KEYWORDS = ["保健", "保養", "生技", "健康"]


def _fetch_list(keyword: str) -> list[dict]:
    sess = utils.get_session()
    page = 1
    rows: list[dict] = []
    while True:
        try:
            resp = sess.get(LIST_URL, params={"keyword": keyword, "page": page}, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            logging.warning("opendata list %s page %s failed: %s", keyword, page, e)
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        tbody = soup.find("tbody")
        if not tbody or not tbody.find("tr"):
            break
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            comp_id = tds[0].get_text(strip=True)
            name = tds[1].get_text(strip=True)
            owner = tds[2].get_text(strip=True)
            addr = tds[3].get_text(strip=True)
            a = tr.find("a", href=True)
            info_url = urljoin(BASE, a["href"]) if a else urljoin(BASE, f"/tool/companyInfo/{comp_id}")
            rows.append({
                "統編": comp_id,
                "公司名稱": name,
                "負責人": owner,
                "地址": addr,
                "info_url": info_url,
                "分類": keyword,
            })
        page += 1
    return rows


def _fetch_detail(url: str) -> tuple[str, str, str]:
    try:
        resp = utils.make_request(url)
    except Exception as e:
        logging.warning("opendata detail fetch failed: %s", e)
        return "", "", ""
    soup = BeautifulSoup(resp.text, "html.parser")
    phone = ""
    email = ""
    website = ""
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        key = th.get_text(strip=True)
        value = td.get_text(strip=True)
        if "電話" in key:
            phone = value
        elif "mail" in key.lower() or "Email" in key:
            email = value
        elif "網址" in key or "網站" in key:
            a = td.find("a", href=True)
            website = a["href"] if a else value
    return phone, email, website


def crawl() -> pd.DataFrame:
    rows: list[dict] = []
    for kw in KEYWORDS:
        rows.extend(_fetch_list(kw))
    for row in rows:
        phone, email, website = _fetch_detail(row.pop("info_url"))
        row.update({"電話": phone, "Email": email, "官網": website})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["統編"])
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = config.OUTPUT_DIR / f"company_opendata_{datetime.now():%Y%m%d}.csv"
    try:
        df.to_csv(out, index=False)
    except Exception as e:
        logging.warning("failed to save opendata csv: %s", e)
    return df
