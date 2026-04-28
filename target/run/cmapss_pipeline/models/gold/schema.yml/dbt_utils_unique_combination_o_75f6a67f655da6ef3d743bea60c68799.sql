select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        unit_id, cycle
    from "cmapss"."silver_gold"."gold_train_features"
    group by unit_id, cycle
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test