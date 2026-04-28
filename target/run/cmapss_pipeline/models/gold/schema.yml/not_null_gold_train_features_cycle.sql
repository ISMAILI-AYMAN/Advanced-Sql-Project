select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cycle
from "cmapss"."silver_gold"."gold_train_features"
where cycle is null



      
    ) dbt_internal_test