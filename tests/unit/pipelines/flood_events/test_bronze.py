"""Bronze flood-event files preserve source text before Silver cleanup."""

import json
from pathlib import Path

import pandas as pd

from flashflood_data.pipelines.flood_events.bronze import parse_events


def test_event_parser_preserves_original_columns_and_time_text(tmp_path: Path) -> None:
    path = tmp_path / "events.xlsx"
    pd.DataFrame(
        [{"STT": 3, "Năm": 2024, "Ngày xảy ra": "10/8/2024", "Mô tả ngắn": "Lũ quét"}]
    ).to_excel(path, index=False)

    rows = parse_events(path, object_id="raw-event", run_id="parse-1", parser_version="v1")

    assert rows[0]["source_record_id"] == "3"
    assert rows[0]["source_valid_time"] == "10/8/2024"
    assert json.loads(rows[0]["original_fields_json"])["Mô tả ngắn"] == "Lũ quét"
