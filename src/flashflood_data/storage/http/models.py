from dataclasses import dataclass


@dataclass(frozen=True)
class ResumeState:
    remote_fingerprint: str
    validator_header: str | None = None
    validator_value: str | None = None
    verified_size_bytes: int | None = None
    verified_checksum: str | None = None
    pending_quarantine_path: str | None = None
    pending_quarantine_error: str | None = None
    pending_quarantine_size_bytes: int | None = None
    pending_quarantine_checksum: str | None = None


@dataclass(frozen=True)
class VerifiedPayload:
    size_bytes: int
    checksum: str
