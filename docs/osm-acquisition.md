# Geofabrik OSM acquisition

The OSM adapter resolves the PBF with a HEAD request, derives a dated raw target from
`Last-Modified`, and retains the MD5 sidecar before exposing the PBF download. The
source payload belongs at `dataset/raw/osm/geofabrik/<YYYYMMDD>/`; harmonized
Exposure-AOI layers are GeoParquet only. The raw PBF is never clipped, rewritten, or
replaced.

The current `flashflood-data fetch` command is deliberately scaffolded. Its future
orchestrator must fetch the sidecar first, verify the downloaded PBF MD5 before it is
catalogued as valid, and re-resolve/discard a partial when `Last-Modified` changes
between preflight and GET. Until that stage exists, the adapter and its fixture tests
provide metadata, size-preflight, stable-ID, exact-clip, and GeoParquet contracts; no
live multi-gigabyte PBF download is run by tests or this implementation.
