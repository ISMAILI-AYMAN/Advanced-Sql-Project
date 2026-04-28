select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from "cmapss"."silver_gold"."gold_train_features"

where not(rul >= 0)


      
    ) dbt_internal_test