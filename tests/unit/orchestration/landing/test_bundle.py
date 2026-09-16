from pathlib import Path
from zipfile import ZipFile

import pytest

from flashflood_data.orchestration.landing.bundle import (
    IncompleteShapefileBundle,
    build_deterministic_zip,
    shapefile_members,
)


def _write_member(path: Path, content: bytes) -> Path:
    path.write_bytes(content)
    return path


def test_deterministic_zip_is_order_independent_and_streamed(tmp_path: Path) -> None:
    shp = _write_member(tmp_path / "basin.shp", b"shape")
    dbf = _write_member(tmp_path / "basin.dbf", b"attributes")

    first = build_deterministic_zip((shp, dbf), tmp_path / "first.zip")
    second = build_deterministic_zip((dbf, shp), tmp_path / "second.zip")

    assert first.checksum == second.checksum
    assert first.members == second.members
    with ZipFile(first.path) as archive:
        assert archive.namelist() == ["basin.dbf", "basin.shp"]


def test_shapefile_members_require_core_files_and_keep_optional(tmp_path: Path) -> None:
    primary = tmp_path / "basin.shp"
    paths = tuple(
        _write_member(tmp_path / f"basin{suffix}", suffix.encode())
        for suffix in (".shp", ".shx", ".dbf", ".prj", ".cpg")
    )
    assert shapefile_members(primary, paths) == tuple(sorted(paths, key=lambda item: item.name))

    (tmp_path / "basin.dbf").unlink()
    with pytest.raises(IncompleteShapefileBundle, match=".dbf"):
        shapefile_members(primary, paths)
