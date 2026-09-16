"""Static workflow stages."""
from enum import StrEnum


class Stage(StrEnum):
    INVENTORY = "inventory"
    BOOTSTRAP_ADMIN = "bootstrap_admin"
    AOI = "aoi"
    FETCH = "fetch"
    VALIDATE = "validate"
    HARMONIZE = "harmonize"
    DERIVE = "derive"
    MAP = "map"
    QA = "qa"

STATIC_ORDER = tuple(Stage)
