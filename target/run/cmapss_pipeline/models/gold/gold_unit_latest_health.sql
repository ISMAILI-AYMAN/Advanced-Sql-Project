
  
    

  create  table "cmapss"."silver_gold"."gold_unit_latest_health__dbt_tmp"
  
  
    as
  
  (
    with ranked as (
    select
        *,
        row_number() over (
            partition by unit_id
            order by cycle desc
        ) as cycle_rank
    from "cmapss"."silver_gold"."gold_train_features"
)
select
    unit_id,
    cycle as latest_cycle,
    rul as latest_rul,
    sensor_1_zscore,
    sensor_2_zscore,
    sensor_3_zscore,
    sensor_1_delta_baseline,
    sensor_2_delta_baseline,
    sensor_3_delta_baseline,
    baseline_quality
from ranked
where cycle_rank = 1
  );
  