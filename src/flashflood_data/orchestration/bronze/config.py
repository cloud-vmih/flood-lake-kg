"""Versioned parser policy for the static raw-to-Bronze Airflow DAG."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class BronzeConfig:
    sources: dict[str, dict[str, str]]
    contract_version: str = "v1"
    rules: dict[str, dict[str, str]] | None = None

    def status(self, source_id: str) -> str:
        return self.sources[source_id]["status"]

    def ready_source_ids(self) -> tuple[str, ...]:
        """Only sources with implemented parsers should enter the parse DAG."""
        return tuple(source_id for source_id in self.sources if self.status(source_id) == "ready")

    def should_process(self, source_id: str, requested_source_id: str | None) -> bool:
        """Limit an ad-hoc DAG run to one ready source when requested."""
        requested = (requested_source_id or "").strip()
        if not requested:
            return True
        if requested not in self.ready_source_ids():
            raise ValueError(f"requested Bronze source is not ready: {requested}")
        return source_id == requested

    @staticmethod
    def force_reprocess(value: object) -> bool:
        """Parse an Airflow conf value without treating the string 'false' as true."""
        if isinstance(value, bool):
            return value
        normalized = str(value or "").strip().lower()
        if normalized in {"", "0", "false", "no"}:
            return False
        if normalized in {"1", "true", "yes"}:
            return True
        raise ValueError(f"invalid force_reprocess value: {value}")

    def parser_version(self, source_id: str) -> str:
        if self.status(source_id) != "ready":
            raise ValueError(f"Bronze parser is not ready for {source_id}")
        return self.sources[source_id]["parser_version"]

    def target_table(self, source_id: str) -> str:
        return self.sources[source_id].get("target_table", "")


def load_bronze_config(path: Path) -> BronzeConfig:
    """Validate parser versions/statuses rather than deriving them from run time."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    sources = document["sources"]
    if not sources or any(
        not isinstance(source_id, str)
        or details.get("status") not in {"ready", "benchmark_required"}
        or not isinstance(details.get("parser_version"), str)
        or not details["parser_version"]
        for source_id, details in sources.items()
    ):
        raise ValueError("invalid static Bronze parser policy")
    return BronzeConfig(
        sources=sources,
        contract_version=str(document.get("contract_version", "v1")),
        rules=document.get("rules", {}),
    )
