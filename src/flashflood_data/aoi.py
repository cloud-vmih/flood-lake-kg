from flashflood_data.static.harmonize import aoi as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
