# NASA C-MAPSS Project Next Steps

This checklist captures the practical next actions after implementation.

## Current Progress Snapshot
- Completed: **Step 1 (Environment Bootstrap)**.
- Next: **Step 2 (Start Local Stack)**.

## 1) Environment Bootstrap
- Copy `.env.example` to `.env` and fill values if needed.
- Install dependencies:
  - `python -m pip install -r requirements.txt`
- Ensure Docker Desktop is running.
- Verify completion:
  - `.env` exists in project root
  - dependency install exits successfully
  - `docker --version` returns a version string
- Windows note:
  - If `dbt` or `airflow` is not recognized, add this to `PATH`:
    - `C:\Users\sonic\AppData\Roaming\Python\Python314\Scripts`

## 2) Start Local Stack
- Bring up infrastructure:
  - `docker compose --env-file .env up -d postgres airflow-init airflow-webserver airflow-scheduler`
- Verify services:
  - `docker compose ps`
  - confirm `postgres`, `airflow-webserver`, and `airflow-scheduler` are `Up`
- (Optional) Run dbt container:
  - `docker compose --env-file .env run --rm dbt-runner`

## 3) Validate Data Organization Gate
- Confirm required folders/files exist under `data/raw/{train,test,truth}`.
- Run:
  - `python src/ingestion/validate_layout.py`
  - `python src/ingestion/build_manifest.py`
- Verify `data/manifest/data_file_manifest.csv` has `status=approved` for all rows.

## 4) Bootstrap Database DDL
- Execute:
  - `psql "host=localhost port=5432 dbname=$env:PGDATABASE user=$env:PGUSER password=$env:PGPASSWORD" -f sql/ddl/001_create_schemas.sql`
  - `psql "host=localhost port=5432 dbname=$env:PGDATABASE user=$env:PGUSER password=$env:PGPASSWORD" -f sql/ddl/010_bronze_tables.sql`
  - `psql "host=localhost port=5432 dbname=$env:PGDATABASE user=$env:PGUSER password=$env:PGPASSWORD" -f sql/ddl/020_ops_tables.sql`

## 5) Run Bronze Ingestion
- Execute:
  - `python src/ingestion/load_bronze.py`
- Validate row counts and audit logs in:
  - `bronze.raw_train_data`
  - `bronze.raw_test_data`
  - `bronze.raw_rul_truth`
  - `ops.ingestion_audit_log`

## 6) Build dbt Silver and Gold
- Install dbt packages:
  - `dbt deps --project-dir .`
- Build models and tests:
  - `dbt build --project-dir . --profiles-dir profiles --fail-fast`
- Generate docs:
  - `dbt docs generate --project-dir . --profiles-dir profiles`

## 7) Publish Serving Objects
- Execute publish script:
  - `python src/ops/publish_gold.py`
- Verify final serving objects:
  - `gold.gold_train_features`
  - `gold.gold_test_features`
  - `gold.gold_unit_latest_health`

## 8) Orchestration Dry Run
- In Airflow UI (`http://localhost:8080`), trigger `cmapss_pipeline`.
- Confirm task order and retry behavior:
  - `validate_layout -> build_manifest -> load_bronze -> dbt_build -> dbt_docs -> publish_views`
- Check `ops.pipeline_run_log` entries.

## 9) Test and CI Readiness
- Run local tests:
  - `python -m pytest -q`
- Ensure CI file is valid:
  - `.github/workflows/ci.yml`
- Confirm CI expectations:
  - lint/tests
  - dbt parse
  - targeted dbt build on `silver_*` and `gold_*`

## 10) Hardening Before Release
- Add retention policy for versioned Gold tables.
- Add sample-data fixture for deterministic CI dbt runs.
- Add rollback runbook for publish failures.
- Add monitoring alerts for failed ingestion/publish tasks.

## 11) Documentation and Handoff
- Add architecture notes and data dictionary for Silver/Gold columns.
- Record operational commands in team wiki/README.
- Capture known limitations and follow-up items.

## 12) Final Step: Push to GitHub
- If repository is not initialized:
  - `git init`
  - `git branch -M main`
- Add remote:
  - `git remote add origin <your-github-repo-url>`
- Commit and push:
  - `git add .`
  - `git commit -m "Implement NASA C-MAPSS production medallion pipeline"`
  - `git push -u origin main`
