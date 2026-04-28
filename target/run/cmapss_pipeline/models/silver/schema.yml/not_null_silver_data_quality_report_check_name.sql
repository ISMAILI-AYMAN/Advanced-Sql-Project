select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select check_name
from "cmapss"."silver_silver"."silver_data_quality_report"
where check_name is null



      
    ) dbt_internal_test