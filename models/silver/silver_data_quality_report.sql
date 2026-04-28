with train_dupes as (
    select
        'train_duplicate_keys' as check_name,
        count(*)::bigint as issue_count
    from (
        select unit_id, cycle
        from {{ source('bronze', 'raw_train_data') }}
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
        from {{ source('bronze', 'raw_test_data') }}
        group by 1, 2
        having count(*) > 1
    ) d
),
train_rows as (
    select 'train_rows_after_silver' as check_name, count(*)::bigint as issue_count
    from {{ ref('silver_train_data') }}
),
test_rows as (
    select 'test_rows_after_silver' as check_name, count(*)::bigint as issue_count
    from {{ ref('silver_test_data') }}
)
select * from train_dupes
union all
select * from test_dupes
union all
select * from train_rows
union all
select * from test_rows
