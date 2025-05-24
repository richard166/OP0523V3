import logging
import os
import pandas as pd

from ..config import DATA_DIR, REQUIRED_COLS
from .. import utils

SAMPLE = os.path.join(DATA_DIR, "tfda_cosmetic_sample.csv")
URL = "https://data.fda.gov.tw/...&format=csv"


def fetch() -> list[dict]:
    try:
        resp = utils.get_session().get(URL, timeout=20)
        resp.raise_for_status()
        content, filename = resp.content, "tfda_cosmetic.csv"
    except Exception as e:
        logging.warning("TFDA cosmetic fetch failed: %s, fallback sample", e)
        with open(SAMPLE, "rb") as f:
            content, filename = f.read(), SAMPLE
    df = utils.read_table(content, filename)
    rows = [
        {col: r.get(col, "") for col in REQUIRED_COLS}
        for _, r in df.iterrows()
    ]
    return rows
