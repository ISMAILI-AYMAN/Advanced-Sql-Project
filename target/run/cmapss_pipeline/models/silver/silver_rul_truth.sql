
  
    

  create  table "cmapss"."silver_silver"."silver_rul_truth__dbt_tmp"
  
  
    as
  
  (
    with ranked as (
    select
        *,
        row_number() over (
            partition by unit_id
            order by ingestion_ts_utc desc, source_row_num desc
        ) as dedupe_rank
    from "cmapss"."bronze"."raw_rul_truth"
)
select
    cast(unit_id as integer) as unit_id,
    cast(rul_truth as integer) as rul_truth
from ranked
where dedupe_rank = 1
  );
  