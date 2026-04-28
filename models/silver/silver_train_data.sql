with ranked as (
    select
        *,
        row_number() over (
            partition by unit_id, cycle
            order by ingestion_ts_utc desc, source_row_num desc
        ) as dedupe_rank
    from {{ source('bronze', 'raw_train_data') }}
),
deduped as (
    select *
    from ranked
    where dedupe_rank = 1
),
stats as (
    select
        avg(sensor_1) as mean_sensor_1, stddev_pop(sensor_1) as std_sensor_1,
        avg(sensor_2) as mean_sensor_2, stddev_pop(sensor_2) as std_sensor_2,
        avg(sensor_3) as mean_sensor_3, stddev_pop(sensor_3) as std_sensor_3,
        avg(sensor_4) as mean_sensor_4, stddev_pop(sensor_4) as std_sensor_4,
        avg(sensor_5) as mean_sensor_5, stddev_pop(sensor_5) as std_sensor_5,
        avg(sensor_6) as mean_sensor_6, stddev_pop(sensor_6) as std_sensor_6,
        avg(sensor_7) as mean_sensor_7, stddev_pop(sensor_7) as std_sensor_7,
        avg(sensor_8) as mean_sensor_8, stddev_pop(sensor_8) as std_sensor_8,
        avg(sensor_9) as mean_sensor_9, stddev_pop(sensor_9) as std_sensor_9,
        avg(sensor_10) as mean_sensor_10, stddev_pop(sensor_10) as std_sensor_10,
        avg(sensor_11) as mean_sensor_11, stddev_pop(sensor_11) as std_sensor_11,
        avg(sensor_12) as mean_sensor_12, stddev_pop(sensor_12) as std_sensor_12,
        avg(sensor_13) as mean_sensor_13, stddev_pop(sensor_13) as std_sensor_13,
        avg(sensor_14) as mean_sensor_14, stddev_pop(sensor_14) as std_sensor_14,
        avg(sensor_15) as mean_sensor_15, stddev_pop(sensor_15) as std_sensor_15,
        avg(sensor_16) as mean_sensor_16, stddev_pop(sensor_16) as std_sensor_16,
        avg(sensor_17) as mean_sensor_17, stddev_pop(sensor_17) as std_sensor_17,
        avg(sensor_18) as mean_sensor_18, stddev_pop(sensor_18) as std_sensor_18,
        avg(sensor_19) as mean_sensor_19, stddev_pop(sensor_19) as std_sensor_19,
        avg(sensor_20) as mean_sensor_20, stddev_pop(sensor_20) as std_sensor_20,
        avg(sensor_21) as mean_sensor_21, stddev_pop(sensor_21) as std_sensor_21
    from deduped
),
filtered as (
    select d.*
    from deduped d
    cross join stats s
    where
        abs(d.sensor_1 - s.mean_sensor_1) <= (6 * greatest(s.std_sensor_1, 1e-9))
        and abs(d.sensor_2 - s.mean_sensor_2) <= (6 * greatest(s.std_sensor_2, 1e-9))
        and abs(d.sensor_3 - s.mean_sensor_3) <= (6 * greatest(s.std_sensor_3, 1e-9))
        and abs(d.sensor_4 - s.mean_sensor_4) <= (6 * greatest(s.std_sensor_4, 1e-9))
        and abs(d.sensor_5 - s.mean_sensor_5) <= (6 * greatest(s.std_sensor_5, 1e-9))
        and abs(d.sensor_6 - s.mean_sensor_6) <= (6 * greatest(s.std_sensor_6, 1e-9))
        and abs(d.sensor_7 - s.mean_sensor_7) <= (6 * greatest(s.std_sensor_7, 1e-9))
        and abs(d.sensor_8 - s.mean_sensor_8) <= (6 * greatest(s.std_sensor_8, 1e-9))
        and abs(d.sensor_9 - s.mean_sensor_9) <= (6 * greatest(s.std_sensor_9, 1e-9))
        and abs(d.sensor_10 - s.mean_sensor_10) <= (6 * greatest(s.std_sensor_10, 1e-9))
        and abs(d.sensor_11 - s.mean_sensor_11) <= (6 * greatest(s.std_sensor_11, 1e-9))
        and abs(d.sensor_12 - s.mean_sensor_12) <= (6 * greatest(s.std_sensor_12, 1e-9))
        and abs(d.sensor_13 - s.mean_sensor_13) <= (6 * greatest(s.std_sensor_13, 1e-9))
        and abs(d.sensor_14 - s.mean_sensor_14) <= (6 * greatest(s.std_sensor_14, 1e-9))
        and abs(d.sensor_15 - s.mean_sensor_15) <= (6 * greatest(s.std_sensor_15, 1e-9))
        and abs(d.sensor_16 - s.mean_sensor_16) <= (6 * greatest(s.std_sensor_16, 1e-9))
        and abs(d.sensor_17 - s.mean_sensor_17) <= (6 * greatest(s.std_sensor_17, 1e-9))
        and abs(d.sensor_18 - s.mean_sensor_18) <= (6 * greatest(s.std_sensor_18, 1e-9))
        and abs(d.sensor_19 - s.mean_sensor_19) <= (6 * greatest(s.std_sensor_19, 1e-9))
        and abs(d.sensor_20 - s.mean_sensor_20) <= (6 * greatest(s.std_sensor_20, 1e-9))
        and abs(d.sensor_21 - s.mean_sensor_21) <= (6 * greatest(s.std_sensor_21, 1e-9))
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
    '{}'::jsonb as train_extra_columns
from filtered
