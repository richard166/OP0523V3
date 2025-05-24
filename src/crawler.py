import logging
import argparse
from datetime import datetime

import pandas as pd

from src import config, utils
from src.sources import four_a, google_places, website_email

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TW company contacts crawler")
    p.add_argument("-v", "--verbose", action="store_true", help="顯示 DEBUG 訊息")
    p.add_argument("--log", metavar="FILE", help="將 log 另存檔案")
    return p.parse_args()


def main() -> None:
    datasets = []
    for source in [four_a, google_places]:
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
    args = parse_args()
    utils.setup_logger(args.verbose, args.log)
    main()
