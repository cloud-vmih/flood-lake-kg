"""Tai trang bao: gzip/charset, retry/backoff, lich su theo domain."""
from __future__ import annotations

import gzip
import time
import urllib.request
from dataclasses import dataclass, field

DEFAULT_USER_AGENT = (
    "SonLaFloodResearchBot/0.1 (nhom nghien cuu; chi doc, tuan thu robots)"
)
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3


@dataclass
class FetchResult:
    ok: bool
    http: int = 0
    html: str = ""
    elapsed_s: float = 0.0
    error: str = ""


def fetch(url: str, *, timeout: int = DEFAULT_TIMEOUT,
          retries: int = DEFAULT_RETRIES,
          user_agent: str = DEFAULT_USER_AGENT) -> FetchResult:
    """GET 1 URL, tu giai nen gzip, doan charset. Loi mang thi thu lai."""
    last = ""
    for attempt in range(retries):
        try:
            started = time.time()
            req = urllib.request.Request(url, headers={"User-Agent": user_agent})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                code = resp.status
                if "gzip" in resp.headers.get("Content-Encoding", ""):
                    raw = gzip.decompress(raw)
                charset = resp.headers.get_content_charset() or "utf-8"
            try:
                html = raw.decode(charset)
            except (UnicodeDecodeError, LookupError):
                html = raw.decode(charset, "ignore")
            return FetchResult(ok=True, http=code, html=html,
                               elapsed_s=round(time.time() - started, 1))
        except Exception as exc:  # noqa: BLE001 - ghi nhan de retry
            last = f"{type(exc).__name__}: {exc}"
            time.sleep(2 * (attempt + 1))
    return FetchResult(ok=False, error=last[:300])


@dataclass
class PoliteFetcher:
    """Gian request theo domain de khong doi bom server bao."""
    min_interval_s: float = 2.0
    timeout: int = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES
    user_agent: str = DEFAULT_USER_AGENT
    _last_host: str = field(default="", init=False, repr=False)
    _last_time: float = field(default=0.0, init=False, repr=False)

    def get(self, url: str) -> FetchResult:
        host = urllib.request.urlparse(url).netloc
        if host == self._last_host:
            wait = self.min_interval_s - (time.time() - self._last_time)
            if wait > 0:
                time.sleep(wait)
        result = fetch(url, timeout=self.timeout, retries=self.retries,
                       user_agent=self.user_agent)
        self._last_host = host
        self._last_time = time.time()
        return result
