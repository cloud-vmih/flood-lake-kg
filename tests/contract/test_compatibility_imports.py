def test_core_compatibility_exports_are_identical() -> None:
    from flashflood_data.config import StudyAreaConfig as LegacyConfig
    from flashflood_data.core.config import StudyAreaConfig
    from flashflood_data.core.paths import ProjectPaths
    from flashflood_data.paths import ProjectPaths as LegacyPaths

    assert LegacyConfig is StudyAreaConfig
    assert LegacyPaths is ProjectPaths


def test_catalog_compatibility_exports_are_identical() -> None:
    from flashflood_data.catalog import AssetCatalog, AssetRecord
    from flashflood_data.catalog.models import AssetRecord as CanonicalRecord
    from flashflood_data.catalog.repository import AssetCatalog as CanonicalCatalog
    from flashflood_data.models import AssetRecord as LegacyRecord

    assert AssetCatalog is CanonicalCatalog
    assert AssetRecord is CanonicalRecord
    assert LegacyRecord is CanonicalRecord


def test_storage_compatibility_exports_are_identical() -> None:
    from flashflood_data.http import DownloadFailed as LegacyDownloadFailed
    from flashflood_data.http import HttpFetcher as LegacyFetcher
    from flashflood_data.io_atomic import atomic_target as legacy_atomic_target
    from flashflood_data.storage.atomic import atomic_target
    from flashflood_data.storage.http import DownloadFailed, HttpFetcher

    assert LegacyDownloadFailed is DownloadFailed
    assert LegacyFetcher is HttpFetcher
    assert legacy_atomic_target is atomic_target
