"""Atomic file publication helpers."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def atomic_target(final_path: Path) -> Iterator[Path]:
    """Yield a sibling partial path and publish it only after successful writing."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    partial = final_path.with_name(f"{final_path.name}.partial")
    partial.unlink(missing_ok=True)
    try:
        yield partial
        partial.replace(final_path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
