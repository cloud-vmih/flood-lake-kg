"""Danh sach URL seed: doc CSV, chuan hoa, dedup giu thu tu."""
from __future__ import annotations

import csv
import urllib.parse

TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term",
                   "utm_content", "fbclid", "gclid"}


def canonicalize_url(url: str) -> str:
    """Ve dang chuan de dedup: host thuong, bo param tracking, bo '/' thua."""
    url = url.strip()
    parts = urllib.parse.urlparse(url)
    host = parts.netloc.lower()
    if host.endswith(":80"):
        host = host[:-3]
    elif host.endswith(":443"):
        host = host[:-4]
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    kept = sorted((k, v) for k, v in query if k not in TRACKING_PARAMS)
    path = parts.path.rstrip("/") or "/"
    return urllib.parse.urlunparse(
        (parts.scheme.lower(), host, path, "", urllib.parse.urlencode(kept), ""))


def load_seeds(path: str) -> list[dict[str, str]]:
    """Doc CSV seeds, tra ve tung dong kem url chuan hoa."""
    rows: list[dict[str, str]] = []
    with open(path, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            url = (row.get("url") or "").strip()
            if not url.startswith("http"):
                continue
            row["canonical_url"] = canonicalize_url(url)
            rows.append(row)
    return rows


def dedupe_urls(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Loai URL trung (theo dang chuan), giu ban dau tien."""
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for row in rows:
        key = row.get("canonical_url") or canonicalize_url(row.get("url", ""))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique
