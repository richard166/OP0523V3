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

logging.getLogger("urllib3").setLevel(logging.ERROR)

GLOBAL_VERIFY_SSL = True

_session = requests.Session()
_retry = Retry(total=1, connect=0, read=0, backoff_factor=0.3)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)


def _auto_ssl_flag(url: str) -> bool:
    from urllib.parse import urlparse

    host = urlparse(url).hostname or ""
    return host not in config.SSL_SKIP_HOSTS


def get_session() -> requests.Session:
    """Return shared HTTP session with retry configured."""
    return _session


def make_request(url: str, *, verify_ssl: bool | None = None, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", config.HEADERS)
    if verify_ssl is None:
        verify_ssl = _auto_ssl_flag(url)
    verify_ssl = verify_ssl and GLOBAL_VERIFY_SSL
    response = _session.get(
        url, headers=headers, timeout=10, verify=verify_ssl, **kwargs
    )
    response.raise_for_status()
    return response


def detect_encoding(data: bytes) -> str:
    result = chardet.detect(data)
    enc = result.get('encoding')
    return enc or 'utf-8'


def read_table(src: Path | bytes, filename: str | None = None) -> pd.DataFrame:
    """Read CSV/XLSX content from path or raw bytes."""

    if isinstance(src, (str, Path)):
        path = Path(src)
        if path.suffix.lower() == ".xlsx":
            return pd.read_excel(path)
        data = path.read_bytes()
        enc = detect_encoding(data)
        return pd.read_csv(BytesIO(data), encoding=enc)

    # src 為 bytes
    data = src
    if filename and filename.lower().endswith(".xlsx"):
        return pd.read_excel(BytesIO(data))
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

def merge_sources(datasets: list[pd.DataFrame | list]) -> pd.DataFrame:
    frames = []
    for d in datasets:
        if isinstance(d, pd.DataFrame):
            frames.append(d)
        else:
            frames.append(pd.DataFrame(d))
    if not frames:
        return pd.DataFrame(columns=list(config.REQUIRED_COLS) + ["分類"])
    df = pd.concat(frames, ignore_index=True)
    df["電話"] = df["電話"].apply(to_e164)
    df = df.drop_duplicates(subset=["公司名稱", "電話"])
    return df

