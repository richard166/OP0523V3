import logging
import argparse
from datetime import datetime

import pandas as pd

from src import config, utils
from src.utils import setup_logger
from src.sources import opendata_company, website_email



def main() -> None:
    p = argparse.ArgumentParser(description="TW company contacts crawler")
    p.add_argument("-v", "--verbose", action="store_true", help="顯示 DEBUG 訊息")
    p.add_argument("--log", metavar="LOG", help="另存 log 檔")
    args = p.parse_args()

    setup_logger(args.verbose, args.log)

    datasets = []

    for source in [opendata_company]:

        try:
            df = source.crawl()
            logging.info('%s entries: %d', source.__name__, len(df))
            datasets.append(df)
        except Exception as e:
            logging.warning('%s failed: %s', source.__name__, e)

    if datasets:
        all_df = utils.merge_sources(datasets)
    else:
        all_df = pd.DataFrame(columns=list(config.REQUIRED_COLS) + ['分類'])

    mask = all_df['Email'].isna() & all_df['官網'].notna()
    all_df.loc[mask, 'Email'] = all_df.loc[mask, '官網'].apply(
        lambda u: website_email.extract(u) or ""
    )

    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output = config.OUTPUT_DIR / f"taiwan_company_contacts_{datetime.now():%Y%m%d}.csv"
    all_df.to_csv(output, index=False)
    logging.info('Saved %d records to %s', len(all_df), output)


if __name__ == '__main__':
    main()
