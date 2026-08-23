# SoilGrids WCS fixture proof

`wv0033_capabilities.xml` advertises the twelve configured mean and uncertainty
coverages for the six standard depths. `wv0033_describe.xml` records the WCS
2.0.1 envelope CRS and its `Long Lat` subset axes. Contract tests substitute
the property identifier to prove the same exact 8 × 6 × 2 request shape for all
configured SoilGrids products without accessing the network.

The planned capability-only live preflight is blocked at this revision: the
`flashflood-data fetch` CLI is still scaffolded and does not accept `--source`
or `--resolve-only`. No live coverage TIFF request was made, and raw payloads
remain immutable when the adapter is exercised against fixture rasters.
