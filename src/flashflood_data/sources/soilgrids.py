from flashflood_data.static.sources import soilgrids as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
