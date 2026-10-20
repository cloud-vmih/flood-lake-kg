"""Lazy public contracts for the Silver L12 basin topology domain."""

from importlib import import_module
from typing import Any

_PUBLIC = {
    "BasinExecutionPlan": ("planner", "BasinExecutionPlan"),
    "BasinTopologyConfig": ("config", "BasinTopologyConfig"),
    "build_basin_silver_runtime": ("runtime", "build_basin_silver_runtime"),
    "load_basin_topology_config": ("config", "load_basin_topology_config"),
}

__all__ = tuple(sorted(_PUBLIC))


def __getattr__(name: str) -> Any:
    target = _PUBLIC.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attribute = target
    value = getattr(import_module(f"{__name__}.{module_name}"), attribute)
    globals()[name] = value
    return value
