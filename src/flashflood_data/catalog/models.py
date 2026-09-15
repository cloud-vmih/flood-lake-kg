"""Immutable value models shared by pipeline stages."""

import re
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Self
from urllib.parse import parse_qsl, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

_SENSITIVE_KEY_PARTS = (
    "secret",
    "token",
    "password",
    "authorization",
    "apikey",
    "signature",
    "credential",
    "privatekey",
)
_SENSITIVE_TEXT = re.compile(
    r"\b(?:secret|token|password|authorization|api[-_ ]?key|signature|credential)\b\s*[:=]",
    re.IGNORECASE,
)


class FrozenDict(dict[str, object]):
    """A read-compatible mapping that rejects every in-place mutation."""

    @staticmethod
    def _immutable(*_: object, **__: object) -> None:
        raise TypeError("frozen mappings cannot be modified")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __ior__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable


def _is_sensitive_key(key: object) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _is_public_endpoint_key(key: object) -> bool:
    """Recognize public OAuth endpoint names without admitting credential values."""
    return re.sub(r"[^a-z0-9]", "", str(key).lower()) in {"tokenurl"}


def _reject_credential_url(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credential-bearing URLs are not allowed")
    if any(_is_sensitive_key(key) for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        raise ValueError("credential-bearing URLs are not allowed")


def _reject_credential_text(value: str) -> None:
    if _SENSITIVE_TEXT.search(value):
        raise ValueError("credential-bearing text is not allowed")


def _reject_credential_mapping(value: Mapping[object, object]) -> None:
    for key, nested_value in value.items():
        if _is_sensitive_key(key) and not _is_public_endpoint_key(key):
            raise ValueError("credential-like mapping keys are not allowed")
        if isinstance(nested_value, Mapping):
            _reject_credential_mapping(nested_value)
        elif isinstance(nested_value, (list, tuple)):
            for item in nested_value:
                if isinstance(item, Mapping):
                    _reject_credential_mapping(item)
                elif isinstance(item, str):
                    _reject_credential_url(item)
                    _reject_credential_text(item)
        elif isinstance(nested_value, str):
            _reject_credential_url(nested_value)
            _reject_credential_text(nested_value)


def _deep_freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return FrozenDict({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_deep_freeze(item) for item in value)
    return value


class AssetStatus(StrEnum):
    """Lifecycle state of a catalogued asset."""

    DISCOVERED = "discovered"
    FETCHING = "fetching"
    FETCHED = "fetched"
    VALIDATED = "validated"
    HARMONIZED = "harmonized"
    DERIVED = "derived"
    FAILED = "failed"
    STALE = "stale"
    QUARANTINED = "quarantined"


class AssetKind(StrEnum):
    """The pipeline layer represented by an asset."""

    RAW = "raw"
    HARMONIZED = "harmonized"
    DERIVED = "derived"
    QA = "qa"


class ImmutableModel(BaseModel):
    """Base class for records that are replaced rather than mutated."""

    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    def model_post_init(self, __context: Any, /) -> None:
        """Recursively freeze containers after Pydantic has validated them."""
        for field_name in type(self).model_fields:
            object.__setattr__(self, field_name, _deep_freeze(getattr(self, field_name)))

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        """Return a validated replacement instead of Pydantic's unchecked copy."""
        values = self.model_dump(mode="python", round_trip=True)
        values.update(update or {})
        return type(self).model_validate(values)


class AssetRecord(ImmutableModel):
    """Auditable metadata for one immutable file asset."""

    asset_id: str
    source_id: str
    source_version: str
    kind: AssetKind
    source_uri: str
    storage_path: str
    media_type: str
    size_bytes: int = Field(ge=0)
    checksum_algorithm: str = "sha256"
    checksum: str
    retrieved_at: datetime
    source_valid_time: str | None = None
    license_id: str
    bbox_wgs84_json: str | None = None
    crs: str | None = None
    resolution_json: str | None = None
    pipeline_run_id: str
    status: AssetStatus
    dependency_fingerprint: str | None = None
    duplicate_of_asset_id: str | None = None
    metadata_json: str = "{}"
    error_code: str | None = None
    error_message: str | None = None

    @field_validator("source_uri")
    @classmethod
    def source_uri_cannot_include_credentials(cls, value: str) -> str:
        _reject_credential_url(value)
        return value

    @field_validator("error_message")
    @classmethod
    def error_message_cannot_include_credentials(cls, value: str | None) -> str | None:
        if value is not None:
            _reject_credential_url(value)
            _reject_credential_text(value)
        return value


class RunRecord(ImmutableModel):
    """Auditable record of an independently runnable pipeline command."""

    run_id: str
    command: str
    started_at: datetime
    ended_at: datetime | None = None
    status: str = "running"
    config_fingerprint: str


class RemoteAsset(ImmutableModel):
    """Source-declared remote payload metadata, excluding credentials."""

    asset_id: str
    source_id: str
    source_version: str
    uri: str
    target_relative_path: Path
    media_type: str
    license_id: str
    expected_size: int | None = None
    budget_size_bytes: int | None = Field(default=None, gt=0)
    expected_checksum: str | None = None
    source_valid_time: str | None = None
    request_method: str = "GET"
    request_form: Mapping[str, str] = Field(default_factory=dict)

    @field_validator("uri")
    @classmethod
    def uri_cannot_include_credentials(cls, value: str) -> str:
        _reject_credential_url(value)
        return value

    @field_validator("request_form")
    @classmethod
    def request_form_cannot_include_credentials(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        _reject_credential_mapping(value)
        return value


class SourceSpec(ImmutableModel):
    """Static configuration for one source adapter."""

    source_id: str
    adapter: str
    version: str
    license_id: str
    enabled: bool = True
    settings: Mapping[str, object] = Field(default_factory=dict)

    @field_validator("settings")
    @classmethod
    def settings_cannot_include_credentials(
        cls, value: Mapping[str, object]
    ) -> Mapping[str, object]:
        _reject_credential_mapping(value)
        return value


class SourceFile(ImmutableModel):
    """Source registry document."""

    sources: tuple[SourceSpec, ...]


class ValidationResult(ImmutableModel):
    """Machine-readable outcome of a validation stage."""

    passed: bool
    checks: Mapping[str, bool]
    metrics: Mapping[str, float | int | str] = Field(default_factory=dict)
    messages: tuple[str, ...] = Field(default_factory=tuple)
