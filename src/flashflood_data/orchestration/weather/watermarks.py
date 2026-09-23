"""Iceberg-backed operational cursors for dynamic weather streams."""

from typing import Protocol

from flashflood_data.orchestration.weather.models import IngestWatermark


class _MetaStore(Protocol):
    def get_meta_row(self, identifier, key): ...
    def upsert_meta_row(self, identifier, key_fields, row): ...


class IngestWatermarkStore:
    """Read and atomically replace one cursor identified by source/product/stream."""

    KEY_FIELDS = ("source_id", "product", "stream_id")

    def __init__(self, store: _MetaStore, namespace: str = "meta") -> None:
        self.store = store
        self.identifier = (namespace, "ingest_watermarks")

    def load(
        self, source_id: str, product: str, stream_id: str
    ) -> IngestWatermark | None:
        row = self.store.get_meta_row(
            self.identifier,
            {"source_id": source_id, "product": product, "stream_id": stream_id},
        )
        return None if row is None else IngestWatermark.model_validate(row)

    def save(self, watermark: IngestWatermark) -> int:
        return self.store.upsert_meta_row(
            self.identifier,
            self.KEY_FIELDS,
            watermark.model_dump(mode="python"),
        )

