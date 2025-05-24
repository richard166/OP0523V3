import argparse, logging, sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from src import config, utils
from utils import setup_logger
from .sources import (
    opendata_company,
    website_email,
)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('-v', '--verbose', action='store_true',
                   help='show DEBUG messages')
    p.add_argument('--log', type=Path, help='log file path')
    return p.parse_args()


def main() -> None:
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
    args = parse_args()
    setup_logger(args.verbose, args.log)
    main()
