"""Manual Bronze backfill uses the same parsing service as Airflow."""

import json

from typer.testing import CliRunner

from flashflood_data.cli.app import app


def test_bronze_backfill_dry_run_lists_registered_object_ids(monkeypatch) -> None:
    class Service:
        def discover(self, source_id, *, parser_version, force_reprocess):
            assert source_id == "hydrobasins_v1c"
            assert parser_version == "v1"
            assert not force_reprocess
            return ("raw-a", "raw-b")

        def process_object(self, *args, **kwargs):
            raise AssertionError("dry-run must not parse")

    monkeypatch.setattr(
        "flashflood_data.cli.commands.bronze.build_bronze_service", lambda: Service()
    )
    result = CliRunner().invoke(
        app, ["bronze", "backfill", "--source-id", "hydrobasins_v1c", "--dry-run"]
    )
    assert result.exit_code == 0
    assert json.loads(result.stdout)["object_ids"] == ["raw-a", "raw-b"]


def test_bronze_reconcile_command_reports_status(monkeypatch) -> None:
    expected = {
        "available_objects_without_bronze": {"hydrobasins_v1c": ["obj-1"]},
        "orphaned_bronze_rows": [],
        "duplicate_business_keys": {},
        "unreferenced_published_runs": {
            "missing_snapshot_refs": [],
            "missing_lineage_edges": [],
        },
    }

    class Service:
        def reconcile(self, source_id=None):
            return expected

    monkeypatch.setattr(
        "flashflood_data.cli.commands.bronze.build_bronze_service", lambda: Service()
    )
    result = CliRunner().invoke(app, ["bronze", "reconcile", "--source-id", "hydrobasins_v1c"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == expected
