





with validation_errors as (

    select
        unit_id, cycle
    from "cmapss"."silver_gold"."gold_train_features"
    group by unit_id, cycle
    having count(*) > 1

)

select *
from validation_errors


