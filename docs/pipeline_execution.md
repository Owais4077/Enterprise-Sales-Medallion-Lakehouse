# End-to-End Pipeline Execution Guide

This document provides step-by-step instructions for executing the complete **Enterprise Sales & Customer Data Platform** lakehouse pipeline from data generation to Gold analytics layer.

---

## 🚀 Step-by-Step Pipeline Execution

### 1. Environment Setup
Create your local environment file:
```bash
cp .env.example .env
```

Start the PostgreSQL source database and FastAPI Exchange Rates mock API:
```bash
docker compose up -d --build postgres mock-api
```

---

### 2. Synthetic Data Generation & Database Load
Generate synthetic PostgreSQL sales records, customer reviews JSONs, target CSVs, and country reference data:
```bash
docker compose run --rm app python -m data_generation.generate all
```

---

### 3. Source Data Ingestion (Extraction $\rightarrow$ Landing)
Extract data incrementally from PostgreSQL, REST API, and raw files into `data/landing/`:
```bash
docker compose run --rm app python -m edp.ingestion
```

---

### 4. Bronze Layer Processing (Landing $\rightarrow$ Delta Lake)
Load raw landed batches into append-only Bronze Delta Lake tables:
```bash
docker compose run --rm app python -m edp.transformations.bronze load
```

Run structural integrity checks comparing Bronze tables against landed manifests:
```bash
docker compose run --rm app python -m edp.transformations.bronze validate
```

---

### 5. Silver Layer Transformations (Deduplication & MERGE)
Cast raw text types, enforce business rules, route invalid records to quarantine, and execute `MERGE INTO` on Silver Delta tables:
```bash
docker compose run --rm app python -m edp.transformations.silver load
```

Run automated Data Quality checks on Silver tables:
```bash
docker compose run --rm app python -m edp.quality run --layer silver
```

---

### 6. Gold Layer Star Schema & KPI Aggregations
Build dimension tables (`dim_customer`, `dim_product`, `dim_country`, `dim_date`), `fact_sales` with USD currency conversions, and `kpi_monthly_sales`:
```bash
docker compose run --rm app python -m edp.transformations.gold build
```

Run automated Data Quality checks on Gold tables:
```bash
docker compose run --rm app python -m edp.quality run --layer gold
```

---

### 7. Run Complete Unit & Spark Test Suite
Verify 100% test pass rate across all 162 unit, Bronze, Silver, Gold, and Data Quality tests:
```bash
docker compose run --rm app pytest tests/unit tests/spark
```
