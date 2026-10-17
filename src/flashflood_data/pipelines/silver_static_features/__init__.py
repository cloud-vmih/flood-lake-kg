"""Pure static-feature kernels reserved for the Bronze-to-Silver pipeline."""

from flashflood_data.pipelines.silver_static_features.hydrology import (
    derive_hydrology_features,
)
from flashflood_data.pipelines.silver_static_features.landcover import (
    derive_landcover_fractions,
)
from flashflood_data.pipelines.silver_static_features.population import (
    aggregate_population_by_basin,
)
from flashflood_data.pipelines.silver_static_features.soil import depth_weighted_soil
from flashflood_data.pipelines.silver_static_features.terrain import derive_terrain_features

__all__ = [
    "aggregate_population_by_basin",
    "depth_weighted_soil",
    "derive_hydrology_features",
    "derive_landcover_fractions",
    "derive_terrain_features",
]
