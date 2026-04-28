select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select rul
from "cmapss"."silver_gold"."gold_train_features"
where rul is null



      
    ) dbt_internal_test