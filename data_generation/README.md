# data_generation/

Seeded synthetic data for the three sources. See [docs/data_sources.md](../docs/data_sources.md).

| Module | Role |
|---|---|
| `reference.py` | countries, currencies, product catalogue (static reference data) |
| `generators.py` | pure functions: config in, `Dataset` out (no I/O, fully unit-tested) |
| `file_writers.py` | CSV, JSON, API seed file, manifest |
| `pg_loader.py` | transactional bulk load via `COPY` |
| `generate.py` | CLI: `python -m data_generation.generate {files,postgres,all}` |
| `mock_api/app.py` | FastAPI exchange-rate service |
