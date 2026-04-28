select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select unit_id
from "cmapss"."silver_silver"."silver_rul_truth"
where unit_id is null



      
    ) dbt_internal_test