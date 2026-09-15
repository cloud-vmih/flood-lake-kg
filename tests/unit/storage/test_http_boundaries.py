from pathlib import Path

from flashflood_data.storage.http import HttpFetcher

ROOT = Path(__file__).parents[3]
HTTP_DIR = ROOT / "src" / "flashflood_data" / "storage" / "http"


def test_http_fetcher_keeps_public_fetch_interface() -> None:
    assert callable(HttpFetcher.fetch)
    assert callable(HttpFetcher.lock_path)


def test_http_responsibilities_are_split_into_reviewable_modules() -> None:
    required = {
        "fetcher.py",
        "resume.py",
        "transfer.py",
        "quarantine.py",
        "redaction.py",
    }
    assert required <= {path.name for path in HTTP_DIR.glob("*.py")}
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= 500
        for path in HTTP_DIR.glob("*.py")
    )
