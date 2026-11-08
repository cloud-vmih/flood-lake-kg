"""Small signed MinIO result manifests shared by Spark Silver domains."""

from __future__ import annotations

import hashlib
import hmac
import urllib.request
from collections.abc import Callable
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10.


def manifest_key(uri: str) -> str:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("Spark output manifest must use s3://bucket/key")
    return f"{parsed.netloc}/{parsed.path.strip('/')}"


def _signing_key(secret: str, date: str, region: str) -> bytes:
    def digest(key: bytes, value: str) -> bytes:
        return hmac.new(key, value.encode(), hashlib.sha256).digest()

    service = digest(digest(digest(("AWS4" + secret).encode(), date), region), "s3")
    return digest(service, "aws4_request")


def put_json_manifest(
    uri: str,
    payload: str,
    *,
    now: datetime,
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str = "us-east-1",
    opener: Callable[..., object] = urllib.request.urlopen,
) -> None:
    """Write one UTF-8 JSON object to MinIO using AWS Signature V4."""
    key = manifest_key(uri)
    target = urlsplit(endpoint)
    if target.scheme not in {"http", "https"} or not target.netloc:
        raise ValueError("MinIO endpoint must be absolute HTTP(S)")
    if not access_key or not secret_key:
        raise ValueError("MinIO manifest credentials are required")
    if now.tzinfo is None or now.utcoffset() != _UTC.utcoffset(now):
        raise ValueError("manifest timestamp must be timezone-aware UTC")
    date = now.strftime("%Y%m%d")
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    body = payload.encode("utf-8")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_uri = "/" + "/".join(quote(segment, safe="-_.~") for segment in key.split("/"))
    headers = (
        f"host:{target.netloc}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"PUT\n{canonical_uri}\n\n{headers}\n{signed_headers}\n{payload_hash}"
    scope = f"{date}/{region}/s3/aws4_request"
    to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    signature = hmac.new(
        _signing_key(secret_key, date, region), to_sign.encode(), hashlib.sha256
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    request = urllib.request.Request(
        f"{target.scheme}://{target.netloc}{canonical_uri}",
        data=body,
        method="PUT",
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        },
    )
    with opener(request, timeout=30) as response:
        if response.status not in {200, 201}:
            raise RuntimeError(f"MinIO manifest PUT failed: HTTP {response.status}")
