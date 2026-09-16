"""Pure workflow dependency fingerprinting."""
import hashlib
import json
from collections.abc import Iterable, Mapping


def dependency_fingerprint(
    checksums: Iterable[str], config: Mapping[str, object], processor_version: str
) -> str:
    """Return a stable fingerprint for unordered immutable dependencies."""
    payload = {
        "checksums": sorted(checksums),
        "config": config,
        "processor": processor_version,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


