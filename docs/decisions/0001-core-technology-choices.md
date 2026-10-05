# ADR 0001: Core technology choices

Status: accepted (Phase 1)

## Decisions

1. **Single installable package `src/edp`; DAGs stay thin.**
   Keeps logic unit-testable and independent of the scheduler.
2. **Docker is the reference environment.** PySpark 3.5 needs Python 3.8-3.12 and Java 11/17.
   Docker pins these so local runs, CI and teammates behave the same.
3. **Delta Lake via `delta-spark` on plain PySpark locally.** Free, and the same code runs on
   Databricks. No Databricks workspace is used in this repo; a deployment guide is provided instead.
4. **Storage abstracted by `LakeLayout`.** Local disk by default, S3 by configuration.
5. **Custom PySpark data-quality framework instead of Great Expectations.**
   Smaller, easier to explain, and directly produces the passed/failed/processed/rejected
   output we need. Trade-off: fewer built-in checks and no data docs.
6. **Mock REST API (FastAPI) with synthetic data.** Demonstrates pagination, incremental
   `updated_since` and auth without external keys or licensing issues.
7. **Settings vs pipeline config.** Secrets and environment values come from environment
   variables (`.env`, never committed); pipeline structure lives in `config/*.yaml`.
8. **Synthetic, seeded data (Faker).** Reproducible, no private or copyrighted data.

## Consequences

- Windows users need only Docker Desktop and Git.
- Custom DQ code means we maintain it ourselves, covered by tests.
