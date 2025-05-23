import logging
import pandas as pd

from .. import config, utils

SAMPLE = 'google_places.csv'


def crawl() -> pd.DataFrame:
    api_key = config.GOOGLE_API_KEY
    if not api_key:
        logging.warning('GOOGLE_API_KEY missing; skip Google Places and use sample')
        try:
            return pd.read_csv(config.DATA_DIR / SAMPLE)
        except Exception:
            return pd.DataFrame(columns=['公司名稱', '電話', 'Email', '官網', '分類'])

    import googlemaps

    gmaps = googlemaps.Client(key=api_key)
    records = []
    for kw in config.KEYWORDS:
        for city in config.CITIES:
            query = f"{city}{kw}"
            try:
                res = gmaps.places(query=query, language='zh-TW')
                for item in res.get('results', []):
                    detail = gmaps.place(place_id=item['place_id'], language='zh-TW')
                    info = detail.get('result', {})
                    records.append({
                        '公司名稱': info.get('name'),
                        '電話': info.get('formatted_phone_number'),
                        'Email': None,
                        '官網': info.get('website'),
                        '分類': kw
                    })
            except Exception as e:
                logging.warning('Google Places query %s failed: %s', query, e)
    df = pd.DataFrame(records)
    if df.empty:
        try:
            df = pd.read_csv(config.DATA_DIR / SAMPLE)
        except Exception:
            pass
    return df
