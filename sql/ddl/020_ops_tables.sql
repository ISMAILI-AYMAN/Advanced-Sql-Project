create table if not exists ops.ingestion_audit_log (
    run_id uuid not null,
    ingestion_id uuid not null,
    split text not null,
    source_file_name text not null,
    status text not null,
    rows_loaded integer not null default 0,
    error_message text,
    started_at_utc timestamptz not null,
    finished_at_utc timestamptz
);

create table if not exists ops.pipeline_run_log (
    run_id uuid not null,
    pipeline_version text not null,
    task_name text not null,
    status text not null,
    rows_in bigint,
    rows_out bigint,
    metadata jsonb default '{}'::jsonb,
    started_at_utc timestamptz not null,
    finished_at_utc timestamptz
);

create table if not exists ops.sensor_physical_bounds (
    sensor_name text primary key,
    lower_bound double precision not null,
    upper_bound double precision not null,
    source text not null,
    updated_at_utc timestamptz not null default now()
);

create index if not exists idx_ingestion_audit_run
    on ops.ingestion_audit_log(run_id, split);
create index if not exists idx_pipeline_run_status
    on ops.pipeline_run_log(run_id, status);
