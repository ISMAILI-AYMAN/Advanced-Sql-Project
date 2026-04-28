select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cycle
from "cmapss"."silver_silver"."silver_test_data"
where cycle is null



      
    ) dbt_internal_test