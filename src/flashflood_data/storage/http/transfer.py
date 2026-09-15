"""Bounded streaming and retry primitives for HTTP acquisition."""

import math
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import BinaryIO, Final

import httpx

from flashflood_data.catalog.models import RemoteAsset
from flashflood_data.storage.http.errors import (
    BoundExceeded as _BoundExceeded,
)
from flashflood_data.storage.http.errors import (
    RangeBodyMismatch as _RangeBodyMismatch,
)
from flashflood_data.storage.http.errors import (
    UnsafePartialResponse as _UnsafePartialResponse,
)
from flashflood_data.storage.http.models import ResumeState as _ResumeState

_CONTENT_RANGE: Final = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_STRONG_ETAG: Final = re.compile(r'^"[\x21\x23-\x7e\x80-\xff]*"$')


class TransferMixin:
    def _download_attempt(
        self, remote: RemoteAsset, partial: Path, size_bound: int, request_headers: dict[str, str]
    ) -> tuple[int, float | None] | None:
        resume = self._owned_resume_state(remote, partial)
        for request_number in range(2):
            partial_size = partial.stat().st_size if resume is not None else 0
            headers = dict(request_headers)
            if resume is not None:
                headers.update(
                    {
                        "Range": f"bytes={partial_size}-",
                        "If-Range": resume.validator_value,
                    }
                )
            with self.client.stream(
                remote.request_method,
                remote.uri,
                headers=headers,
                data=remote.request_form or None,
                follow_redirects=True,
            ) as response:
                if response.status_code == 429 or response.status_code >= 500:
                    return response.status_code, self._retry_after(
                        response.headers.get("Retry-After")
                    )
                response.raise_for_status()
                if response.status_code == 206:
                    if resume is None:
                        raise _UnsafePartialResponse("unsolicited_partial_response")
                    range_length = self._compatible_range_length(
                        response, partial_size, size_bound, remote, resume
                    )
                    if range_length is None:
                        self._discard_resume(remote, partial)
                        resume = None
                        if request_number == 0:
                            continue
                        raise _UnsafePartialResponse("incompatible_partial_response")
                    self._stream_response(
                        response,
                        partial,
                        mode="ab",
                        initial_size=partial_size,
                        size_bound=size_bound,
                        expected_body_bytes=range_length,
                    )
                    return None

                self._stream_full_response(response, partial, remote, size_bound)
                return None
        raise AssertionError("unreachable")

    def _stream_full_response(
        self,
        response: httpx.Response,
        partial: Path,
        remote: RemoteAsset,
        size_bound: int,
    ) -> None:
        partial.parent.mkdir(parents=True, exist_ok=True)
        try:
            handle = partial.open("wb")
        except Exception:
            self._discard_resume(remote, partial)
            raise
        with handle:
            try:
                validator = self._response_validator(response)
                if validator is None:
                    self._resume_state_path(remote).unlink(missing_ok=True)
                else:
                    self._write_resume_state(remote, validator)
            except Exception:
                handle.seek(0)
                handle.truncate(0)
                self._resume_state_path(remote).unlink(missing_ok=True)
                raise
            self._stream_chunks(
                response,
                handle,
                initial_size=0,
                size_bound=size_bound,
            )

    def _stream_response(
        self,
        response: httpx.Response,
        partial: Path,
        *,
        mode: str,
        initial_size: int,
        size_bound: int,
        expected_body_bytes: int | None = None,
    ) -> None:
        body_bytes = 0
        partial.parent.mkdir(parents=True, exist_ok=True)
        with partial.open(mode) as handle:
            body_bytes = self._stream_chunks(
                response,
                handle,
                initial_size=initial_size,
                size_bound=size_bound,
                expected_body_bytes=expected_body_bytes,
            )
        if expected_body_bytes is not None and body_bytes != expected_body_bytes:
            raise _RangeBodyMismatch

    @staticmethod
    def _stream_chunks(
        response: httpx.Response,
        handle: BinaryIO,
        *,
        initial_size: int,
        size_bound: int,
        expected_body_bytes: int | None = None,
    ) -> int:
        body_bytes = 0
        written = initial_size
        for chunk in response.iter_raw():
            if expected_body_bytes is not None and body_bytes + len(chunk) > expected_body_bytes:
                allowed = max(expected_body_bytes - body_bytes, 0)
                handle.write(chunk[:allowed])
                raise _RangeBodyMismatch
            if written + len(chunk) > size_bound:
                handle.write(chunk[: max(size_bound - written, 0)])
                raise _BoundExceeded
            handle.write(chunk)
            body_bytes += len(chunk)
            written += len(chunk)
        if expected_body_bytes is not None and body_bytes != expected_body_bytes:
            raise _RangeBodyMismatch
        return body_bytes

    @staticmethod
    def _response_validator(response: httpx.Response) -> tuple[str, str] | None:
        etag = response.headers.get("ETag")
        if etag is not None and _STRONG_ETAG.fullmatch(etag.strip()) is not None:
            return "ETag", etag
        last_modified = response.headers.get("Last-Modified")
        if last_modified is not None:
            try:
                parsed = parsedate_to_datetime(last_modified)
            except (TypeError, ValueError, OverflowError):
                return None
            if parsed.tzinfo is not None:
                return "Last-Modified", last_modified
        return None

    @staticmethod
    def _compatible_range_length(
        response: httpx.Response,
        partial_size: int,
        size_bound: int,
        remote: RemoteAsset,
        resume: _ResumeState,
    ) -> int | None:
        if response.status_code != 206:
            return None
        match = _CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", "").strip())
        if match is None:
            return None
        start, end, total = match.groups()
        if total == "*":
            return None
        start_value, end_value, total_value = int(start), int(end), int(total)
        response_validator = response.headers.get(resume.validator_header)
        if (
            start_value != partial_size
            or end_value != total_value - 1
            or total_value > size_bound
            or response_validator != resume.validator_value
        ):
            return None
        if remote.expected_size is not None and total_value != remote.expected_size:
            return None
        return end_value - start_value + 1

    @staticmethod
    def _retry_after(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            seconds = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=UTC)
                seconds = (retry_at - datetime.now(UTC)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return None
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return min(seconds, 60)
