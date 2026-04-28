select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        baseline_quality as value_field,
        count(*) as n_records

    from "cmapss"."silver_gold"."gold_unit_latest_health"
    group by baseline_quality

)

select *
from all_values
where value_field not in (
    'ok','low'
)



      
    ) dbt_internal_test