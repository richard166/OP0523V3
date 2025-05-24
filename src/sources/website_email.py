import logging
import re
import urllib.parse

from ..utils import get_session

EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}")


def extract(url: str) -> str | None:
    sess = get_session()
    pages = [
        url,
        urllib.parse.urljoin(url, "contact"),
        urllib.parse.urljoin(url, "\u806f\u7d61\u6211\u5011"),
    ]
    for p in pages:
        try:
            text = sess.get(p, timeout=8).text
            m = EMAIL_RE.search(text)
            if m:
                return m.group(0)
        except Exception:
            continue
    logging.debug("no email found: %s", url)
    return None
