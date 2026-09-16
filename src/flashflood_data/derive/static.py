from flashflood_data.static.features import builder as _canonical


def __getattr__(name: str) -> object:
    return getattr(_canonical, name)
