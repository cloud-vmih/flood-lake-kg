from pathlib import Path

import pytest

from flashflood_data.io_atomic import atomic_target


def test_atomic_target_publishes_complete_write(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"

    with atomic_target(target) as partial:
        partial.write_bytes(b"complete")

    assert target.read_bytes() == b"complete"
    assert not target.with_name("asset.bin.partial").exists()


def test_atomic_target_does_not_publish_failed_write(tmp_path: Path) -> None:
    target = tmp_path / "asset.bin"

    with pytest.raises(RuntimeError), atomic_target(target) as partial:
        partial.write_bytes(b"broken")
        raise RuntimeError("stop")

    assert not target.exists()
    assert not target.with_name("asset.bin.partial").exists()
