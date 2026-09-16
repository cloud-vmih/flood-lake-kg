"""Static workflow run summary."""
from typing import Literal

from pydantic import BaseModel, Field


class RunSummary(BaseModel):
    """Sanitized aggregate outcome for one pipeline command."""

    run_id: str
    status: Literal["completed", "partial_failure", "failed"]
    fetched: int = 0
    validated: int = 0
    harmonized: int = 0
    derived: int = 0
    reused: int = 0
    completed_sources: list[str] = Field(default_factory=list)
    failed_sources: list[str] = Field(default_factory=list)
    errors: dict[str, str] = Field(default_factory=dict)
    metrics: dict[str, int] = Field(default_factory=dict)


