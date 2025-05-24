import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import config, utils
    dfs = []
    # 1. 先跑四個來源
    for source in [four_a, google_places, tfda_health_food, tfda_cosmetics_gmp]:
        try:
            df = source.crawl()
            logging.info('%s entries: %d', source.__name__, len(df))
            dfs.append(df)
        except Exception as e:
            logging.warning('%s failed: %s', source.__name__, e)

    # 2. 合併
    if dfs:
        all_df = pd.concat(dfs, ignore_index=True)
    else:
        all_df = pd.DataFrame(columns=list(config.REQUIRED_COLS) + ['分類'])

    # 3. 電話轉 E.164、去重
    if '電話' in all_df.columns:
        all_df['電話'] = all_df['電話'].apply(utils.to_e164)
    all_df = all_df.drop_duplicates(subset=['公司名稱', '電話'])

    # 4. 補抓 Email（僅當目前 Email 為空且有官網）
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
