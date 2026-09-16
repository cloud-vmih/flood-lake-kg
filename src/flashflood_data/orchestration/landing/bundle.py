"""Deterministic, memory-bounded source bundle construction."""

import shutil
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from flashflood_data.catalog.repository import sha256_file
from flashflood_data.orchestration.landing.models import BundleMember

_CORE_SHAPEFILE_SUFFIXES = frozenset({".shp", ".shx", ".dbf", ".prj"})
_OPTIONAL_SHAPEFILE_SUFFIXES = frozenset({".cpg", ".sbn", ".sbx", ".shp.xml"})
_COPY_CHUNK_SIZE = 8 * 1024 * 1024


class IncompleteShapefileBundle(ValueError):
    """Raised when a selected Shapefile lacks required members."""


@dataclass(frozen=True)
class BundleResult:
    path: Path
    checksum: str
    size_bytes: int
    members: tuple[BundleMember, ...]


def _logical_suffix(path: Path) -> str:
    return ".shp.xml" if path.name.lower().endswith(".shp.xml") else path.suffix.lower()


def shapefile_members(primary: Path, candidates: tuple[Path, ...]) -> tuple[Path, ...]:
    """Validate and return the complete member set for one Shapefile basename."""
    expected_stem = primary.name[: -len(".shp")]
    selected = tuple(
        sorted(
            (
                path
                for path in candidates
                if path.name == f"{expected_stem}{_logical_suffix(path)}"
                and _logical_suffix(path)
                in _CORE_SHAPEFILE_SUFFIXES | _OPTIONAL_SHAPEFILE_SUFFIXES
            ),
            key=lambda path: path.name,
        )
    )
    suffixes = {_logical_suffix(path) for path in selected if path.is_file()}
    missing = sorted(_CORE_SHAPEFILE_SUFFIXES - suffixes)
    if missing:
        raise IncompleteShapefileBundle(
            f"Shapefile {primary.name} is missing members: {', '.join(missing)}"
        )
    return selected


def build_deterministic_zip(members: tuple[Path, ...], output_path: Path) -> BundleResult:
    """Stream files into a byte-stable ZIP with normalized metadata."""
    ordered = tuple(sorted((Path(path) for path in members), key=lambda path: path.name))
    names = [path.name for path in ordered]
    if len(names) != len(set(names)):
        raise ValueError("bundle contains duplicate member names")
    if not ordered or any(not path.is_file() for path in ordered):
        raise FileNotFoundError("every bundle member must be a file")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_members: list[BundleMember] = []
    with ZipFile(output_path, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path in ordered:
            info = ZipInfo(path.name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            info.flag_bits |= 0x800
            info.compress_type = ZIP_DEFLATED
            with path.open("rb") as source, archive.open(
                info, "w", force_zip64=True
            ) as destination:
                shutil.copyfileobj(source, destination, length=_COPY_CHUNK_SIZE)
            bundle_members.append(
                BundleMember(
                    name=path.name,
                    path=path,
                    size_bytes=path.stat().st_size,
                    checksum=sha256_file(path),
                )
            )
    return BundleResult(
        path=output_path,
        checksum=sha256_file(output_path),
        size_bytes=output_path.stat().st_size,
        members=tuple(bundle_members),
    )
