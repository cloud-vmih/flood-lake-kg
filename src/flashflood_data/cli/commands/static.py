"""Static-data CLI command exports."""
from flashflood_data.cli.app import (
    cleanup,
    derive,
    fetch,
    harmonize,
    inventory,
    map_stage,
    run_static,
    validate,
)

__all__ = ["cleanup", "derive", "fetch", "harmonize", "inventory", "map_stage", "run_static", "validate"]
