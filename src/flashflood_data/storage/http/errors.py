class BudgetRejected(RuntimeError):
    """Raised before acquisition when the storage preflight rejects a payload."""


class DownloadFailed(RuntimeError):
    """Raised when a remote payload cannot be acquired safely."""


class PayloadMismatch(DownloadFailed):
    """Raised after a downloaded payload fails size or checksum validation."""


class ExistingAssetConflict(DownloadFailed):
    """Raised when a target already contains a different payload."""


class DownloadLocked(DownloadFailed):
    """Raised when another process owns the target's download lock."""


class ResumeCleanupError(DownloadFailed):
    """Raised when an unsafe partial download cannot be cleaned up."""


class RetryableStatus(Exception):
    """Internal signal for an HTTP status that may be retried."""

    def __init__(self, status_code: int, retry_after: float | None) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"retryable HTTP status {status_code}")


class BoundExceeded(Exception):
    """Internal signal for a response that exceeded its declared bound."""


class RangeBodyMismatch(Exception):
    """Internal signal for an incompatible ranged response body."""


class UnsafePartialResponse(Exception):
    """Internal signal for a partial response that cannot be resumed safely."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)
