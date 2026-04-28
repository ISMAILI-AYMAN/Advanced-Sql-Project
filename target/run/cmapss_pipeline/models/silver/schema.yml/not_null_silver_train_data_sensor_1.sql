select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sensor_1
from "cmapss"."silver_silver"."silver_train_data"
where sensor_1 is null



      
    ) dbt_internal_test