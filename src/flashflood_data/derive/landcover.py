from flashflood_data.static.features import landcover as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
