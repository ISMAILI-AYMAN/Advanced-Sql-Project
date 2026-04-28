select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    unit_id as unique_field,
    count(*) as n_records

from "cmapss"."silver_silver"."silver_rul_truth"
where unit_id is not null
group by unit_id
having count(*) > 1



      
    ) dbt_internal_test