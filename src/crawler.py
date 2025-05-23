import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import config, utils
from .sources import four_a, google_places, tfda_health_food, tfda_cosmetics_gmp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main() -> None:
    dfs = []
    for source in [four_a, google_places, tfda_health_food, tfda_cosmetics_gmp]:
        try:
            df = source.crawl()
            logging.info('%s entries: %d', source.__name__, len(df))
            dfs.append(df)
        except Exception as e:
            logging.warning('%s failed: %s', source.__name__, e)

    if dfs:
        all_df = pd.concat(dfs, ignore_index=True)
        all_df['電話'] = all_df['電話'].apply(utils.to_e164)
        all_df = all_df.drop_duplicates(subset=['公司名稱', '電話'])
    else:
        all_df = pd.DataFrame(columns=['公司名稱', '電話', 'Email', '官網', '分類'])

    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output = config.OUTPUT_DIR / f"taiwan_company_contacts_{datetime.now():%Y%m%d}.csv"
    all_df.to_csv(output, index=False)
    logging.info('Saved %d records to %s', len(all_df), output)


if __name__ == '__main__':
    main()
