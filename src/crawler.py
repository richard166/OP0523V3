import logging
from datetime import datetime
from pathlib import Path
import argparse

import pandas as pd

from . import config, utils
from .sources import (
    four_a,
    google_places,
    tfda_health_food,
    tfda_cosmetics_gmp,
    opendata_company,
    tfda_health_gmp,
    tfda_cosmetic_gmp,
    website_email,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main() -> None:
    datasets = []
    for source in [
        four_a,
        google_places,
        tfda_health_food,
        tfda_cosmetics_gmp,
        opendata_company,
    ]:
        try:
            df = source.crawl()
            logging.info('%s entries: %d', source.__name__, len(df))
            datasets.append(df)
        except Exception as e:
            logging.warning('%s failed: %s', source.__name__, e)

    for fetcher in [tfda_health_gmp.fetch, tfda_cosmetic_gmp.fetch]:
        try:
            datasets.append(pd.DataFrame(fetcher()))
        except Exception as e:
            logging.warning('%s failed: %s', fetcher.__module__, e)

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
    args = parser.parse_args()
    utils.GLOBAL_VERIFY_SSL = not args.no_ssl
    main()
