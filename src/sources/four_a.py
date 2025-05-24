import logging
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd

from .. import utils, config

URL = 'https://www.aaaa.org.tw/4a-member'
SAMPLE = '4a_member.html'


def crawl() -> pd.DataFrame:
    try:
        resp = utils.make_request(URL)
        html = resp.text
    except Exception as e:
        logging.warning('4A source failed: %s; using sample', e)
        html = utils.load_sample(SAMPLE).decode('utf-8')

    soup = BeautifulSoup(html, 'html.parser')
    records = []
    for div in soup.select('div.member'):
        name = div.find('h3')
        name = name.get_text(strip=True) if name else None
        phone = None
        phone_tag = div.find(string=lambda x: x and '電話' in x)
        if phone_tag:
            phone = phone_tag.split('：')[-1].strip()
        email = None
        mail_tag = div.find(string=lambda x: x and '@' in x)
        if mail_tag:
            email = mail_tag.strip()
        website = None
        a = div.find('a', href=True)
        if a:
            website = a['href']
        if name:
            records.append({
                '公司名稱': name,
                '電話': phone,
                'Email': email,
                '官網': website,
                '分類': '4A會員'
            })
    return pd.DataFrame(records)
