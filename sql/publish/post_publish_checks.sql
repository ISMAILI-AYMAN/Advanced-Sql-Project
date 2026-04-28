with checks as (
    select 'gold_train_features_not_empty' as check_name,
           case when count(*) > 0 then 'pass' else 'fail' end as status
    from gold.gold_train_features
    union all
    select 'gold_test_features_not_empty',
           case when count(*) > 0 then 'pass' else 'fail' end
    from gold.gold_test_features
    union all
    select 'gold_unit_latest_health_not_empty',
           case when count(*) > 0 then 'pass' else 'fail' end
    from gold.gold_unit_latest_health
)
select * from checks;
