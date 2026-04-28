
  
    

  create  table "cmapss"."silver_gold"."gold_test_features__dbt_tmp"
  
  
    as
  
  (
    with max_cycle as (
    select unit_id, max(cycle) as max_cycle
    from "cmapss"."silver_silver"."silver_test_data"
    group by unit_id
),
joined as (
    select
        t.*,
        m.max_cycle,
        r.rul_truth
    from "cmapss"."silver_silver"."silver_test_data" t
    join max_cycle m using (unit_id)
    left join "cmapss"."silver_silver"."silver_rul_truth" r using (unit_id)
)
select
    unit_id,
    cycle,
    op_setting_1,
    op_setting_2,
    op_setting_3,
    sensor_1,
    sensor_2,
    sensor_3,
    sensor_4,
    sensor_5,
    sensor_6,
    sensor_7,
    sensor_8,
    sensor_9,
    sensor_10,
    sensor_11,
    sensor_12,
    sensor_13,
    sensor_14,
    sensor_15,
    sensor_16,
    sensor_17,
    sensor_18,
    sensor_19,
    sensor_20,
    sensor_21,
    (rul_truth + (max_cycle - cycle))::integer as inferred_rul
from joined
  );
  