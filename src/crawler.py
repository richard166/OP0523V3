import logging
from datetime import datetime
import argparse

import pandas as pd

from . import config, utils
from .sources import (
    opendata_company,
    website_email,
)


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
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-ssl', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show debug logs')
    parser.add_argument('--log', help='Path to log file')
    args = parser.parse_args()

    utils.setup_logger(verbose=args.verbose, log_file=args.log)
    utils.GLOBAL_VERIFY_SSL = not args.no_ssl
    main()
