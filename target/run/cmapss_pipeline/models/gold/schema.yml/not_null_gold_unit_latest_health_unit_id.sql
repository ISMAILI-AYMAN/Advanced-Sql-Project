select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select unit_id
from "cmapss"."silver_gold"."gold_unit_latest_health"
where unit_id is null



      
    ) dbt_internal_test