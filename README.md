# Enterprise Sales & Customer Data Platform

> Status: **Phase 1 of 16 complete** (repository foundation). Sections marked _TBD_ are filled
> in as the matching phase is built. Nothing here is claimed to work until it has been run.

## 1. Project overview
End-to-end data engineering project: ingest sales data from PostgreSQL, a REST API and files,
process it through a Bronze/Silver/Gold lakehouse on Delta Lake with PySpark, orchestrate with
Airflow, validate with automated data-quality checks, and serve a star schema to Power BI.

## 2. Business problem
_TBD (Phase 16)_

## 3. Architecture
See [docs/architecture.md](docs/architecture.md).

## 4. Technology stack
Python, SQL, PostgreSQL, PySpark, Delta Lake, Airflow, Docker, GitHub Actions, Pytest, Ruff,
Black. Optional: AWS S3 / RDS, Databricks, Power BI.

## 5. Data sources
See [docs/data_sources.md](docs/data_sources.md). _Implemented in Phase 2._

## 6. Data model
_TBD (Phases 2 and 6)_

## 7. Bronze / Silver / Gold architecture
_TBD (Phases 4-6)_

## 8. Pipeline workflow
_TBD (Phase 9)_

## 9. Incremental processing
_TBD (Phase 7)_

## 10. Data quality
_TBD (Phase 8)_

## 11. Airflow
_TBD (Phase 9)_

## 12. Docker
_TBD (Phase 10)_ Currently `docker compose run --rm app` runs the test suite.

## 13. CI/CD
_TBD (Phase 12)_

## 14. AWS architecture
_TBD (Phase 13)_

## 15. Power BI
_TBD (Phase 14)_

## 16. Testing
```bash
pytest                 # unit tests
ruff check .           # lint
black --check .        # formatting
```

## 17. Monitoring
_TBD (Phase 15)_

## 18. Security
Secrets live only in `.env` (git-ignored). Copy `.env.example` to `.env` and fill it in.
Never commit credentials.

## 19. Local setup
Prerequisites: Docker Desktop, Git. Optional: Python 3.10-3.12 for running tests without Docker.
```bash
cp .env.example .env
docker compose run --rm --build app            # runs pytest in the reference environment
```

## 20. Deployment
_TBD (Phase 13)_

## 21. Example pipeline execution
_TBD (Phase 16)_

## 22. Future improvements
_TBD (Phase 16)_

## Repository layout
```
src/edp/            installable package (common, ingestion, transformations, quality)
dags/               Airflow DAGs (thin wrappers)
sql/                Postgres DDL and analytics views
data_generation/    synthetic data + mock API
config/             pipeline YAML (no secrets)
tests/              unit and integration tests
docker/             Dockerfiles
dashboards/         Power BI requirements and DAX
infra/aws/          optional cloud deployment guide
docs/               architecture, ADRs, data sources
```
