"""Deterministic work batching for static raw-to-Bronze processing."""

from collections.abc import Sequence


def batch_object_refs(
    source_id: str, object_ids: Sequence[str], *, batch_size: int
) -> list[dict[str, object]]:
    """Group unique raw object IDs into stable JSON-serializable work items."""
    if batch_size < 1:
        raise ValueError("Bronze batch size must be positive")
    ordered = list(object_ids)
    if len(set(ordered)) != len(ordered):
        raise ValueError("Bronze work contains duplicate object IDs")
    return [
        {"source_id": source_id, "object_ids": ordered[index : index + batch_size]}
        for index in range(0, len(ordered), batch_size)
    ]
