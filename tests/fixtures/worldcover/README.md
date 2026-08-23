# ESA WorldCover fixture

`grid.geojson` is a tiny stand-in for ESA's official 2020 v100 grid, whose
3-by-3-degree `ll_tile` identifiers are also used by WorldCover 2021 v200.

The production flow first preserves that official grid at
`dataset/raw/worldcover/2021-v200/grid.geojson`; a later resolve pass selects
and HEAD-preflights only the v200 map tiles that intersect the Environmental
AOI.  The project CLI currently has a scaffolded `fetch` command and does not
yet orchestrate this dependency-aware two-pass flow or `--resolve-only`.
Consequently no live 5 MB grid or multi-hundred-MB map-tile download is made
by these tests.
