"""Atomic file publication helpers."""

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def atomic_target(final_path: Path) -> Iterator[Path]:
    """Yield a sibling partial path and publish it only after successful writing."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{final_path.name}.", suffix=".partial", dir=final_path.parent
    )
    os.close(descriptor)
    partial = Path(temporary_name)
    try:
        yield partial
        partial.replace(final_path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
