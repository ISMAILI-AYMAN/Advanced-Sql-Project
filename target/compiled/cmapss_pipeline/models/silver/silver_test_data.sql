with ranked as (
    select
        *,
        row_number() over (
            partition by unit_id, cycle
            order by ingestion_ts_utc desc, source_row_num desc
        ) as dedupe_rank
    from "cmapss"."bronze"."raw_test_data"
),
deduped as (
    select *
    from ranked
    where dedupe_rank = 1
)
select
    cast(unit_id as integer) as unit_id,
    cast(cycle as integer) as cycle,
    cast(op_setting_1 as double precision) as op_setting_1,
    cast(op_setting_2 as double precision) as op_setting_2,
    cast(op_setting_3 as double precision) as op_setting_3,
    cast(sensor_1 as double precision) as sensor_1,
    cast(sensor_2 as double precision) as sensor_2,
    cast(sensor_3 as double precision) as sensor_3,
    cast(sensor_4 as double precision) as sensor_4,
    cast(sensor_5 as double precision) as sensor_5,
    cast(sensor_6 as double precision) as sensor_6,
    cast(sensor_7 as double precision) as sensor_7,
    cast(sensor_8 as double precision) as sensor_8,
    cast(sensor_9 as double precision) as sensor_9,
    cast(sensor_10 as double precision) as sensor_10,
    cast(sensor_11 as double precision) as sensor_11,
    cast(sensor_12 as double precision) as sensor_12,
    cast(sensor_13 as double precision) as sensor_13,
    cast(sensor_14 as double precision) as sensor_14,
    cast(sensor_15 as double precision) as sensor_15,
    cast(sensor_16 as double precision) as sensor_16,
    cast(sensor_17 as double precision) as sensor_17,
    cast(sensor_18 as double precision) as sensor_18,
    cast(sensor_19 as double precision) as sensor_19,
    cast(sensor_20 as double precision) as sensor_20,
    cast(sensor_21 as double precision) as sensor_21,
    '{}'::jsonb as test_extra_columns
from deduped