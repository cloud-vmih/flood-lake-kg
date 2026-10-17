"""Deterministic quarantine records and Iceberg persistence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, Protocol
from urllib.parse import parse_qsl, urlsplit

from flashflood_data.shared.quality.models import QualityFinding

Layer = Literal["raw", "bronze", "silver", "gold"]
_UTC = timezone.utc  # noqa: UP017 - imported by the Python 3.10 Spark driver


class QuarantineStore(Protocol):
    def upsert_quarantine_rows(
        self,
        identifier: tuple[str, str],
        key_fields: tuple[str, ...],
        rows: Sequence[Mapping[str, Any]],
    ) -> int: ...


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def make_rejection_id(*, pipeline_run_id: str | None = None, **identity: object) -> str:
    """Hash logical rejection identity; the retry/run identifier is deliberately ignored."""
    del pipeline_run_id
    normalized = {
        key: sorted(value) if key == "rule_ids" and isinstance(value, (list, tuple, set)) else value
        for key, value in identity.items()
        if value is not None
    }
    return hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()


def _validate_input_identity(
    object_id: str | None,
    input_table: str | None,
    input_snapshot_id: int | None,
) -> None:
    has_object = object_id is not None
    has_any_snapshot = input_table is not None or input_snapshot_id is not None
    has_snapshot = input_table is not None and input_snapshot_id is not None
    if has_any_snapshot and not has_snapshot:
        raise ValueError("input_table and input_snapshot_id must be supplied together")
    if has_object == has_snapshot:
        raise ValueError("a rejection requires exactly one input identity")


@dataclass(frozen=True)
class ObjectRejection:
    pipeline_run_id: str
    layer: Layer
    target_dataset_id: str
    scope_key: str
    quality_policy_id: str
    rule_id: str
    rule_version: str
    reason_code: str
    detail: Mapping[str, object]
    source_id: str | None = None
    object_id: str | None = None
    input_table: str | None = None
    input_snapshot_id: int | None = None
    evidence_uri: str | None = None
    rejected_at: datetime = field(default_factory=lambda: datetime.now(_UTC))

    def __post_init__(self) -> None:
        _validate_input_identity(self.object_id, self.input_table, self.input_snapshot_id)


@dataclass(frozen=True)
class RowRejection:
    pipeline_run_id: str
    layer: Literal["bronze", "silver", "gold"]
    target_dataset_id: str
    source_record_key: str
    quality_policy_id: str
    findings: tuple[QualityFinding, ...]
    reason_codes: tuple[str, ...]
    object_id: str | None = None
    input_table: str | None = None
    input_snapshot_id: int | None = None
    record: Mapping[str, object] | None = None
    record_uri: str | None = None
    parser_version: str | None = None
    rejected_at: datetime = field(default_factory=lambda: datetime.now(_UTC))

    def __post_init__(self) -> None:
        _validate_input_identity(self.object_id, self.input_table, self.input_snapshot_id)
        if not self.source_record_key:
            raise ValueError("source_record_key is required")
        if self.record is None and self.record_uri is None:
            raise ValueError("a row rejection requires record or record_uri")


_SECRET_KEYS = {
    "access_key",
    "api_key",
    "authorization",
    "credential",
    "password",
    "secret",
    "secret_key",
    "signature",
    "token",
}


def _contains_secret(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if normalized in _SECRET_KEYS or any(
                normalized.endswith(f"_{suffix}") for suffix in _SECRET_KEYS
            ):
                return True
            if _contains_secret(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_secret(item) for item in value)
    return False


def _validate_evidence(value: object, uri: str | None = None) -> None:
    if _contains_secret(value):
        raise ValueError("quarantine evidence cannot contain credential fields")
    if uri:
        query_keys = {key.lower().replace("-", "_") for key, _ in parse_qsl(urlsplit(uri).query)}
        if query_keys & _SECRET_KEYS:
            raise ValueError("quarantine evidence URI cannot contain credentials")


class QuarantineRepository:
    """Store quarantine evidence using retry-stable logical identifiers."""

    def __init__(self, store: QuarantineStore, namespace: str = "quarantine") -> None:
        self.store = store
        self.namespace = namespace

    def reject_object(self, record: ObjectRejection) -> tuple[str, int]:
        _validate_evidence(record.detail, record.evidence_uri)
        rejection_id = make_rejection_id(
            pipeline_run_id=record.pipeline_run_id,
            layer=record.layer,
            target_dataset_id=record.target_dataset_id,
            source_id=record.source_id,
            object_id=record.object_id,
            input_table=record.input_table,
            input_snapshot_id=record.input_snapshot_id,
            scope_key=record.scope_key,
            quality_policy_id=record.quality_policy_id,
            rule_id=record.rule_id,
            rule_version=record.rule_version,
        )
        row = {
            "rejection_id": rejection_id,
            "pipeline_run_id": record.pipeline_run_id,
            "layer": record.layer,
            "target_dataset_id": record.target_dataset_id,
            "source_id": record.source_id,
            "object_id": record.object_id,
            "input_table": record.input_table,
            "input_snapshot_id": record.input_snapshot_id,
            "scope_key": record.scope_key,
            "quality_policy_id": record.quality_policy_id,
            "rule_id": record.rule_id,
            "rule_version": record.rule_version,
            "reason_code": record.reason_code,
            "detail_json": _canonical_json(record.detail),
            "evidence_uri": record.evidence_uri,
            "rejected_at": record.rejected_at,
        }
        snapshot_id = self.store.upsert_quarantine_rows(
            (self.namespace, "rejected_objects"), ("rejection_id",), (row,)
        )
        return rejection_id, snapshot_id

    def reject_rows(
        self, records: Sequence[RowRejection]
    ) -> tuple[tuple[str, ...], int]:
        if not records:
            raise ValueError("row rejection batch cannot be empty")
        rows: list[dict[str, object]] = []
        rejection_ids: list[str] = []
        for record in records:
            _validate_evidence(record.record or {}, record.record_uri)
            rule_ids = tuple(sorted({finding.rule_id for finding in record.findings}))
            rejection_id = make_rejection_id(
                pipeline_run_id=record.pipeline_run_id,
                layer=record.layer,
                target_dataset_id=record.target_dataset_id,
                object_id=record.object_id,
                input_table=record.input_table,
                input_snapshot_id=record.input_snapshot_id,
                source_record_key=record.source_record_key,
                quality_policy_id=record.quality_policy_id,
                rule_ids=rule_ids,
                parser_version=record.parser_version,
            )
            rejection_ids.append(rejection_id)
            rows.append(
                {
                    "rejection_id": rejection_id,
                    "pipeline_run_id": record.pipeline_run_id,
                    "layer": record.layer,
                    "target_dataset_id": record.target_dataset_id,
                    "object_id": record.object_id,
                    "input_table": record.input_table,
                    "input_snapshot_id": record.input_snapshot_id,
                    "source_record_key": record.source_record_key,
                    "quality_policy_id": record.quality_policy_id,
                    "rule_ids": list(rule_ids),
                    "reason_codes_json": _canonical_json(sorted(set(record.reason_codes))),
                    "record_json": None if record.record is None else _canonical_json(record.record),
                    "record_uri": record.record_uri,
                    "parser_version": record.parser_version,
                    "rejected_at": record.rejected_at,
                }
            )
        snapshot_id = self.store.upsert_quarantine_rows(
            (self.namespace, "rejected_rows"), ("rejection_id",), rows
        )
        return tuple(rejection_ids), snapshot_id
