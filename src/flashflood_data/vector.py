from flashflood_data.static.spatial import vector as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
