from flashflood_data.static.sources import osm as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
