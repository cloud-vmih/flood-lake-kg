from pathlib import Path

import pytest

from flashflood_data.io_atomic import atomic_target


def test_atomic_target_publishes_complete_write(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"

    with atomic_target(target) as partial:
        partial.write_bytes(b"complete")

    assert target.read_bytes() == b"complete"
    assert not list(tmp_path.glob("*.partial"))


def test_atomic_target_does_not_publish_failed_write(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"

    with pytest.raises(RuntimeError), atomic_target(target) as partial:
        partial.write_bytes(b"broken")
        raise RuntimeError("stop")

    assert not target.exists()
    assert not list(tmp_path.glob("*.partial"))


def test_atomic_target_nested_writers_use_independent_partials(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"

    with atomic_target(target) as outer_partial:
        outer_partial.write_bytes(b"outer")
        with atomic_target(target) as inner_partial:
            assert inner_partial != outer_partial
            inner_partial.write_bytes(b"inner")
        assert target.read_bytes() == b"inner"

    assert target.read_bytes() == b"outer"
    assert not list(tmp_path.glob("*.partial"))


def test_atomic_target_preserves_existing_final_after_failed_write(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"
    target.write_bytes(b"existing")

    with pytest.raises(RuntimeError), atomic_target(target) as partial:
        partial.write_bytes(b"broken")
        raise RuntimeError("stop")

    assert target.read_bytes() == b"existing"
    assert not list(tmp_path.glob("*.partial"))
