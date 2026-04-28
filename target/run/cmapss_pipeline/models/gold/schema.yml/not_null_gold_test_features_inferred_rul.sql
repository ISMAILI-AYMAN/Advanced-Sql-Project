select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select inferred_rul
from "cmapss"."silver_gold"."gold_test_features"
where inferred_rul is null



      
    ) dbt_internal_test