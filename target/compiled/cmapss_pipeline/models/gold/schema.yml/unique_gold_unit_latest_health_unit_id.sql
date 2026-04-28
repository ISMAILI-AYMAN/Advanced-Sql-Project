
    
    

select
    unit_id as unique_field,
    count(*) as n_records

from "cmapss"."silver_gold"."gold_unit_latest_health"
where unit_id is not null
group by unit_id
having count(*) > 1


