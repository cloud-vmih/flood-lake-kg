"""Large parser key checks must spill to disk instead of growing worker RAM."""

from flashflood_data.core.disk_index import DiskUniqueIndex


def test_disk_unique_index_detects_duplicates_and_cleans_up(tmp_path) -> None:
    with DiskUniqueIndex(directory=tmp_path) as index:
        path = index.path
        assert index.add_many(["a", "b"])
        assert not index.add_many(["c", "a"])
        assert path.exists()

    assert not path.exists()
