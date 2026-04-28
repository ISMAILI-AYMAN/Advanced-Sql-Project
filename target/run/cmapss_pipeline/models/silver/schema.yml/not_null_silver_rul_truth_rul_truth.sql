select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select rul_truth
from "cmapss"."silver_silver"."silver_rul_truth"
where rul_truth is null



      
    ) dbt_internal_test