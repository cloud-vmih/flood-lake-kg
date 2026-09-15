from flashflood_data.static.spatial import raster as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
