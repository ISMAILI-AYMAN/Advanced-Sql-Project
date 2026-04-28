with base as (
    select
        s.*,
        max(cycle) over (partition by unit_id) as max_cycle
    from "cmapss"."silver_silver"."silver_train_data" s
),
rul as (
    select
        *,
        (max_cycle - cycle) as rul
    from base
),
baseline as (
    select
        unit_id,
        avg(sensor_1) filter (where cycle <= 20) as base_s1,
        stddev_pop(sensor_1) filter (where cycle <= 20) as std_s1,
        avg(sensor_2) filter (where cycle <= 20) as base_s2,
        stddev_pop(sensor_2) filter (where cycle <= 20) as std_s2,
        avg(sensor_3) filter (where cycle <= 20) as base_s3,
        stddev_pop(sensor_3) filter (where cycle <= 20) as std_s3
    from rul
    group by unit_id
)
select
    r.unit_id,
    r.cycle,
    r.rul,
    r.op_setting_1,
    r.op_setting_2,
    r.op_setting_3,
    r.sensor_1,
    r.sensor_2,
    r.sensor_3,
    avg(r.sensor_1) over (partition by r.unit_id order by r.cycle rows between 9 preceding and current row) as sensor_1_ma_10,
    avg(r.sensor_1) over (partition by r.unit_id order by r.cycle rows between 49 preceding and current row) as sensor_1_ma_50,
    stddev_pop(r.sensor_1) over (partition by r.unit_id order by r.cycle rows between 9 preceding and current row) as sensor_1_std_10,
    stddev_pop(r.sensor_1) over (partition by r.unit_id order by r.cycle rows between 49 preceding and current row) as sensor_1_std_50,
    avg(r.sensor_2) over (partition by r.unit_id order by r.cycle rows between 9 preceding and current row) as sensor_2_ma_10,
    avg(r.sensor_2) over (partition by r.unit_id order by r.cycle rows between 49 preceding and current row) as sensor_2_ma_50,
    stddev_pop(r.sensor_2) over (partition by r.unit_id order by r.cycle rows between 9 preceding and current row) as sensor_2_std_10,
    stddev_pop(r.sensor_2) over (partition by r.unit_id order by r.cycle rows between 49 preceding and current row) as sensor_2_std_50,
    avg(r.sensor_3) over (partition by r.unit_id order by r.cycle rows between 9 preceding and current row) as sensor_3_ma_10,
    avg(r.sensor_3) over (partition by r.unit_id order by r.cycle rows between 49 preceding and current row) as sensor_3_ma_50,
    stddev_pop(r.sensor_3) over (partition by r.unit_id order by r.cycle rows between 9 preceding and current row) as sensor_3_std_10,
    stddev_pop(r.sensor_3) over (partition by r.unit_id order by r.cycle rows between 49 preceding and current row) as sensor_3_std_50,
    (r.sensor_1 - b.base_s1) / greatest(coalesce(b.std_s1, 1e-9), 1e-9) as sensor_1_zscore,
    (r.sensor_2 - b.base_s2) / greatest(coalesce(b.std_s2, 1e-9), 1e-9) as sensor_2_zscore,
    (r.sensor_3 - b.base_s3) / greatest(coalesce(b.std_s3, 1e-9), 1e-9) as sensor_3_zscore,
    (r.sensor_1 - b.base_s1) as sensor_1_delta_baseline,
    (r.sensor_2 - b.base_s2) as sensor_2_delta_baseline,
    (r.sensor_3 - b.base_s3) as sensor_3_delta_baseline,
    case when r.max_cycle >= 20 then 'ok' else 'low' end as baseline_quality
from rul r
join baseline b using (unit_id)