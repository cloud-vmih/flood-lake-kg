# Sơn La static geospatial data pipeline

This repository builds the file-first static lake under `dataset/`. Existing and newly
acquired raw inputs are retained in place; `run-static` never invokes cleanup. Keep
Copernicus credentials only in the ignored `.env`, using the names shown in `.env.example`.

Set up the Python 3.11 environment with `make setup`. Run the hermetic regression suite
and lint locally with:

```bash
make test
make lint
```

The focused miniature-lake proof is:

```bash
.venv/bin/pytest tests/integration/test_static_pipeline_smoke.py tests/integration/test_static_pipeline_idempotence.py tests/integration/test_data_quality_gates.py -q
```

The live workflow is deliberately split so storage and source resolution can be inspected
before any environmental or exposure payload crawl:

```bash
make inventory
make preflight-aoi
make resolve-live
make live
```

The equivalent explicit commands are:

```bash
.venv/bin/flashflood-data inventory --root /home/cloud/cloud/TLCN/Project
.venv/bin/flashflood-data run-static --profile live --root /home/cloud/cloud/TLCN/Project --stop-after aoi --json-summary
.venv/bin/flashflood-data fetch --profile live --root /home/cloud/cloud/TLCN/Project --resolve-only --json-summary
.venv/bin/flashflood-data run-static --profile live --root /home/cloud/cloud/TLCN/Project --json-summary
```

Do not start the full live run unless the resolve-only summary satisfies the configured
8 GiB new-raw cap and 10 GiB free-space reserve. A transient branch failure is resumable:
rerun the same command without deleting successful raw assets.

After a successful run, serve the movable QA map locally with `make qa-map`, then open
`http://127.0.0.1:8000/`. The machine-readable QA results remain in
`dataset/qa/report.json` and `dataset/qa/report.parquet`.
