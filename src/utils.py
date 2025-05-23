import logging
from pathlib import Path
from io import BytesIO

import chardet
import pandas as pd
import phonenumbers
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import src.config as config

_session = requests.Session()
_retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount('http://', _adapter)
_session.mount('https://', _adapter)


def make_request(url: str, **kwargs) -> requests.Response:
    headers = kwargs.pop('headers', config.HEADERS)
    response = _session.get(url, headers=headers, timeout=30, **kwargs)
    response.raise_for_status()
    return response


def detect_encoding(data: bytes) -> str:
    result = chardet.detect(data)
    enc = result.get('encoding')
    return enc or 'utf-8'


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == '.xlsx':
        return pd.read_excel(path)
    data = path.read_bytes()
    enc = detect_encoding(data)
    return pd.read_csv(BytesIO(data), encoding=enc)


def to_e164(phone: str) -> str | None:
    if not phone:
        return None
    try:
        pn = phonenumbers.parse(phone, 'TW')
        if phonenumbers.is_possible_number(pn):
            return phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        logging.debug('Failed to parse phone %s', phone)
    return None


def load_sample(filename: str) -> bytes:
    path = config.DATA_DIR / filename
    return path.read_bytes()
