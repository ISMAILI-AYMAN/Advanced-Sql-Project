create table if not exists bronze.raw_train_data (
    ingestion_id uuid not null,
    ingestion_ts_utc timestamptz not null,
    source_file_name text not null,
    source_row_num integer not null,
    unit_id integer not null,
    cycle integer not null,
    op_setting_1 double precision not null,
    op_setting_2 double precision not null,
    op_setting_3 double precision not null,
    sensor_1 double precision not null,
    sensor_2 double precision not null,
    sensor_3 double precision not null,
    sensor_4 double precision not null,
    sensor_5 double precision not null,
    sensor_6 double precision not null,
    sensor_7 double precision not null,
    sensor_8 double precision not null,
    sensor_9 double precision not null,
    sensor_10 double precision not null,
    sensor_11 double precision not null,
    sensor_12 double precision not null,
    sensor_13 double precision not null,
    sensor_14 double precision not null,
    sensor_15 double precision not null,
    sensor_16 double precision not null,
    sensor_17 double precision not null,
    sensor_18 double precision not null,
    sensor_19 double precision not null,
    sensor_20 double precision not null,
    sensor_21 double precision not null
);

create table if not exists bronze.raw_test_data (
    ingestion_id uuid not null,
    ingestion_ts_utc timestamptz not null,
    source_file_name text not null,
    source_row_num integer not null,
    unit_id integer not null,
    cycle integer not null,
    op_setting_1 double precision not null,
    op_setting_2 double precision not null,
    op_setting_3 double precision not null,
    sensor_1 double precision not null,
    sensor_2 double precision not null,
    sensor_3 double precision not null,
    sensor_4 double precision not null,
    sensor_5 double precision not null,
    sensor_6 double precision not null,
    sensor_7 double precision not null,
    sensor_8 double precision not null,
    sensor_9 double precision not null,
    sensor_10 double precision not null,
    sensor_11 double precision not null,
    sensor_12 double precision not null,
    sensor_13 double precision not null,
    sensor_14 double precision not null,
    sensor_15 double precision not null,
    sensor_16 double precision not null,
    sensor_17 double precision not null,
    sensor_18 double precision not null,
    sensor_19 double precision not null,
    sensor_20 double precision not null,
    sensor_21 double precision not null
);

create table if not exists bronze.raw_rul_truth (
    ingestion_id uuid not null,
    ingestion_ts_utc timestamptz not null,
    source_file_name text not null,
    source_row_num integer not null,
    unit_id integer not null,
    rul_truth integer not null
);

create index if not exists idx_raw_train_unit_cycle
    on bronze.raw_train_data(unit_id, cycle);
create index if not exists idx_raw_test_unit_cycle
    on bronze.raw_test_data(unit_id, cycle);
create index if not exists idx_raw_train_ingestion
    on bronze.raw_train_data(ingestion_ts_utc);
create index if not exists idx_raw_test_ingestion
    on bronze.raw_test_data(ingestion_ts_utc);
