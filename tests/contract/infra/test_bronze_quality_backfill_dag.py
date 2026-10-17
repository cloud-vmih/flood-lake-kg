import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]


def test_quality_backfill_is_manual_paused_and_serialized() -> None:
    source = (
        ROOT / "src/flashflood_data/pipelines/quality_migration/airflow.py"
    ).read_text(encoding="utf-8")
    ast.parse(source)

    assert 'dag_id="bronze_quality_backfill"' in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert "is_paused_upon_creation=True" in source
    assert 'pool="bronze_writer"' in source
    assert "dataset_id" in source
    assert 'mode="{{ dag_run.conf.get(\'mode\', \'audit\') }}"' in source
    assert "object_limit" in source
    assert "force" in source
    assert "retries=0" in source


def test_quality_backfill_entrypoint_is_thin() -> None:
    source = (ROOT / "airflow/dags/bronze_quality_backfill.py").read_text(
        encoding="utf-8"
    )
    ast.parse(source)
    assert "quality_migration.airflow" in source
    assert "pyiceberg" not in source
