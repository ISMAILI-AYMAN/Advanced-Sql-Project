---
name: nasa-cmapss-pipeline
description: Implements a deterministic NASA C-MAPSS medallion pipeline with strict Bronze/Silver/Gold contracts, RUL feature marts, schema-drift handling, dbt quality gates, and atomic persistence. Use when the user mentions C-MAPSS, turbofan RUL, predictive maintenance ETL, medallion architecture, or dbt data validation.
---

# NASA C-MAPSS Pipeline

## Objective

Produce a reproducible, idempotent, and ML-ready warehouse pipeline for NASA C-MAPSS:
- `Bronze`: source-faithful raw landing
- `Silver`: validated, cleaned, typed telemetry
- `Gold`: RUL and rolling-feature marts for BI and model training

## When To Apply

Apply this skill for:
- NASA C-MAPSS ingestion and warehouse design
- turbofan predictive maintenance ETL
- RUL feature engineering in SQL/dbt
- data quality, schema drift, and lineage requirements

## Non-Negotiable Constraints

- Keep transformations stateless and idempotent.
- Use deterministic ordering: `(unit_id, cycle)` ascending.
- Avoid implicit defaults; document every fallback.
- Use atomic writes: write to staging relation, then swap/rename.
- Never mutate source files.
- Block release if any critical dbt test fails.

## Canonical Source Assumptions

- Input files are space-delimited text files from C-MAPSS FD subsets.
- Core telemetry records contain:
  - identifiers: `unit_id`, `cycle`
  - operating settings: `op_setting_1..3`
  - sensors: `sensor_1..sensor_21`
- `rul_truth` contains one RUL value per test `unit_id`.
- If a file violates these assumptions, fail fast with explicit diagnostics.

## Phase 0) Data Organization (Mandatory First Step)

Purpose: create deterministic file structure and file-level audit before ingestion.

Required directory contract:
- `data/raw/train/`
- `data/raw/test/`
- `data/raw/truth/`
- `data/staging/`
- `data/archive/`

Required organization actions:
1. Build file inventory with `checksum_sha256`, file size, and modified timestamp.
2. Validate filename convention (`train_*`, `test_*`, `RUL_*`) or map via explicit config.
3. Enforce one-to-one split assignment (`train`, `test`, `truth`) for each file.
4. Create `data_file_manifest` with:
   - `file_id`, `file_path`, `split`, `checksum_sha256`, `record_count`, `status`
5. Quarantine invalid files and duplicate checksums unless version override is declared.
6. Gate ingestion until all required files are `status = approved`.

## Layer Contracts

### 1) Bronze (Raw Landing)

Purpose: exact source fidelity and replayability.

Required tables:
- `raw_train_data`
- `raw_test_data`
- `raw_rul_truth`
- `ingestion_audit_log`

Required columns in each Bronze table:
- `ingestion_id` (UUID)
- `ingestion_ts_utc` (timestamp)
- `source_file_name` (text)
- `source_row_num` (integer)
- raw payload columns from source

Rules:
- No renaming or business logic in Bronze.
- Preserve raw value representation unless parser requires type coercion for load.
- Duplicate source rows are allowed in Bronze; dedupe is Silver responsibility.

### 2) Silver (Staging and Cleansing)

Purpose: enforce quality, types, uniqueness, and semantic names.

Required models:
- `silver_train_data`
- `silver_test_data`
- `silver_rul_truth`
- `silver_data_quality_report`

Required actions:
1. Rename columns to canonical snake_case names.
2. Enforce strict types:
   - `unit_id`, `cycle`: integer
   - settings/sensors: float
3. Dedupe on `(unit_id, cycle)` with deterministic keep rule:
   - keep latest by `ingestion_ts_utc`, tie-break by max `source_row_num`
4. Remove zero-variance sensors (variance == 0 within dataset split).
5. Apply physical plausibility checks:
   - enforce `mean +/- 6 * std` per sensor per FD subset
   - optional overrides are allowed only from versioned config table `sensor_physical_bounds`
   - if override exists, it takes precedence and must include `lower_bound`, `upper_bound`, and `source`
6. Record all dropped rows and reasons in quality report.
7. Schema drift handling:
   - missing required columns: fail model
   - new unexpected columns: quarantine in `*_extra_columns` JSON field and continue with warning

### 3) Gold (Analytics and ML Marts)

Purpose: produce RUL targets and model features.

Required models:
- `gold_train_features`
- `gold_test_features`
- `gold_unit_latest_health`

Required feature logic:
1. RUL (train):
   - `max_cycle = max(cycle) over (partition by unit_id)`
   - `rul = max_cycle - cycle`
2. Test-set aligned RUL:
   - join `silver_test_data` with `silver_rul_truth` on `unit_id`
   - `final_rul_at_last_cycle = rul_truth`
   - infer per-cycle RUL by reverse offset from last observed cycle
3. Rolling statistics per sensor:
   - moving average windows: 10 and 50 cycles
   - moving standard deviation windows: 10 and 50 cycles
4. Health indicators:
   - z-score: `(x - mean_unit_baseline) / std_unit_baseline`
   - delta-from-baseline: `x - baseline_first_n_cycles_mean`
5. Baseline definition:
   - default baseline window: first 20 cycles per unit
   - if unit has <20 cycles, use all available cycles and flag baseline_quality = 'low'

## dbt Quality Gates

Minimum required tests:
- `not_null` on all primary keys and critical features
- `unique` on `(unit_id, cycle)` in Silver and Gold
- `relationships`:
  - Gold units must exist in Silver
  - Test truth units must exist in test telemetry
- accepted range tests:
  - `rul >= 0`
  - z-score finite and non-null where baseline is valid

Execution policy:
- Run `dbt build` for full DAG validation.
- Generate docs and lineage after successful build.
- Do not publish Gold artifacts if any severity=error test fails.

## Orchestration Contract

Default run order:
1. Organize files and approve manifest
2. Ingest raw files to Bronze
3. Build Silver models
4. Build Gold models
5. Run dbt tests and docs
6. Publish serving views/tables

Retry policy:
- transient load failures: retry up to 3 times with exponential backoff
- deterministic data validation failures: no retry, fail immediately

## Persistence and Atomicity

- Write each target model to a temporary staging relation.
- Validate row counts and key constraints on staging relation.
- Swap staging into production relation in one atomic step.
- Keep previous relation as rollback target until post-deploy checks pass.

## Observability and Traceability

Required metadata per run:
- `run_id` UUID
- `pipeline_version` (git commit hash)
- `start_ts_utc`, `end_ts_utc`
- row counts in/out by layer
- dropped row counts by reason
- sensor columns removed (zero variance)

Log these to a persistent run log table and emit structured JSON logs.

## Output Interface

Expose:
- BI-friendly latest health snapshot by unit (`gold_unit_latest_health`)
- training-ready feature table (`gold_train_features`)
- inference-ready feature table (`gold_test_features`)

Ensure no hardcoded local paths; all paths and schemas are environment-driven.

## Implementation Checklist

Use this checklist during execution:

```text
Task Progress:
- [ ] Organize dataset files into required directory contract
- [ ] Build and approve `data_file_manifest`
- [ ] Validate source files against canonical schema assumptions
- [ ] Ingest to Bronze with ingestion metadata and audit log
- [ ] Build Silver with typing, dedupe, drift handling, and quality report
- [ ] Build Gold with RUL, rolling stats, and health indicators
- [ ] Configure and run dbt quality gates
- [ ] Publish atomically and verify serving contracts
- [ ] Persist run metadata, lineage, and validation outcomes
```

## Output Expectations

For implementation tasks using this skill, always return:
1. Files created/updated by layer (`Bronze`, `Silver`, `Gold`, governance).
2. Exact formulas implemented (RUL, rolling windows, z-score, baseline logic).
3. Data quality results (failed/passed tests, dropped rows, drift events).
4. Any assumption overrides and why defaults were changed.
5. Remaining risks and explicit next hardening steps.
