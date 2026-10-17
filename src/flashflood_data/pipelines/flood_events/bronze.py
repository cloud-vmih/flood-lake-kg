"""Source-faithful parser for structured flood-event files."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _native(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def parse_events(
    path: Path, *, object_id: str, run_id: str, parser_version: str
) -> list[dict[str, object]]:
    """Preserve original workbook/CSV values and date text before Silver cleanup."""
    if path.suffix.lower() in {".xlsx", ".xls"}:
        frame = pd.read_excel(path, dtype=object)
    elif path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, dtype=object)
    else:
        raise ValueError(f"unsupported structured event file: {path.suffix}")
    if frame.empty:
        raise ValueError("event source contains no records")
    rows = []
    keys: set[str] = set()
    for position, (_, source) in enumerate(frame.iterrows(), start=1):
        attributes = {name: source[name] for name in frame.columns}
        original_id = _native(attributes.get("STT"))
        record_id = (
            str(int(original_id))
            if isinstance(original_id, (int, float)) and float(original_id).is_integer()
            else f"row-{position}"
        )
        if record_id in keys:
            raise ValueError(f"duplicate historical event source ID: {record_id}")
        keys.add(record_id)
        text = _native(attributes.get("Mô tả ngắn"))
        valid_time = _native(attributes.get("Ngày xảy ra"))
        original_fields = {
            str(name): _native(value) for name, value in attributes.items()
        }
        rows.append(
            {
                "object_id": object_id,
                "source_record_id": record_id,
                "source_document_id": None,
                "event_text": None if text is None else str(text),
                "original_fields_json": json.dumps(
                    original_fields,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "source_valid_time": None if valid_time is None else str(valid_time),
                "ingest_run_id": run_id,
                "parser_version": parser_version,
                "quality_status": "passed",
            }
        )
    return rows
