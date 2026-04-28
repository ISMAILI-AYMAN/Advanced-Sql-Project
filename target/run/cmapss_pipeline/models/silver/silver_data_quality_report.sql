
  
    

  create  table "cmapss"."silver_silver"."silver_data_quality_report__dbt_tmp"
  
  
    as
  
  (
    with train_dupes as (
    select
        'train_duplicate_keys' as check_name,
        count(*)::bigint as issue_count
    from (
        select unit_id, cycle
        from "cmapss"."bronze"."raw_train_data"
        group by 1, 2
        having count(*) > 1
    ) d
),
test_dupes as (
    select
        'test_duplicate_keys' as check_name,
        count(*)::bigint as issue_count
    from (
        select unit_id, cycle
        from "cmapss"."bronze"."raw_test_data"
        group by 1, 2
        having count(*) > 1
    ) d
),
train_rows as (
    select 'train_rows_after_silver' as check_name, count(*)::bigint as issue_count
    from "cmapss"."silver_silver"."silver_train_data"
),
test_rows as (
    select 'test_rows_after_silver' as check_name, count(*)::bigint as issue_count
    from "cmapss"."silver_silver"."silver_test_data"
)
select * from train_dupes
union all
select * from test_dupes
union all
select * from train_rows
union all
select * from test_rows
  );
  