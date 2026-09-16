from pathlib import Path

ROOT = Path(__file__).parents[4]
QA_DIR = ROOT / "src" / "flashflood_data" / "static" / "qa"


def test_quality_checks_are_split_by_subject() -> None:
    assert {
        "admin.py",
        "hydro.py",
        "mappings.py",
        "raster.py",
        "population.py",
        "events.py",
        "provenance.py",
        "runner.py",
    } <= {path.name for path in QA_DIR.glob("*.py")}
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= 500
        for path in QA_DIR.glob("*.py")
    )
