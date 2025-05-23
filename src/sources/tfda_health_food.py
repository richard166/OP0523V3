import logging
from pathlib import Path
import pandas as pd

from .. import utils, config

URL = 'https://consumer.fda.gov.tw/LSPN/CsvDownload?nodeId=15'
SAMPLE = 'tfda_health_food.csv'


def crawl() -> pd.DataFrame:
    csv_data = None
    try:
        resp = utils.make_request(URL)
        csv_data = resp.content
    except Exception as e:
        logging.warning('TFDA health food fetch failed: %s; using sample', e)
        try:
            csv_data = utils.load_sample(SAMPLE)
        except Exception:
            return pd.DataFrame(columns=['公司名稱', '電話', 'Email', '官網', '分類'])

    tmp_path = Path('/tmp/tfda_health_food.csv')
    tmp_path.write_bytes(csv_data)
    df = utils.read_table(tmp_path)
    try:
        tmp_path.unlink()
    except Exception:
        pass

    mapping = {c: '公司名稱' for c in df.columns if '公司名稱' in c}
    if '電話' not in df.columns:
        for col in df.columns:
            if '電話' in col:
                mapping[col] = '電話'
    if 'Email' not in df.columns:
        for col in df.columns:
            if 'mail' in col.lower():
                mapping[col] = 'Email'
    if '官網' not in df.columns:
        for col in df.columns:
            if '網站' in col or '官網' in col:
                mapping[col] = '官網'
    df = df.rename(columns=mapping)

    needed = ['公司名稱', '電話', 'Email', '官網']
    for n in needed:
        if n not in df.columns:
            df[n] = None
    df = df[needed]
    df['分類'] = '健康食品'
    return df
