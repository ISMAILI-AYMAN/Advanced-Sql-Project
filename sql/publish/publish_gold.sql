do $$
declare
    suffix text := to_char(now(), 'YYYYMMDDHH24MISS');
begin
    execute format(
        'create table if not exists gold.gold_train_features__v%s as table gold.gold_train_features',
        suffix
    );
    execute format(
        'create table if not exists gold.gold_test_features__v%s as table gold.gold_test_features',
        suffix
    );
    execute format(
        'create table if not exists gold.gold_unit_latest_health__v%s as table gold.gold_unit_latest_health',
        suffix
    );

    begin
        execute 'drop view if exists gold.gold_train_features';
    exception when wrong_object_type then
        execute 'drop table if exists gold.gold_train_features';
    end;
    begin
        execute 'drop view if exists gold.gold_test_features';
    exception when wrong_object_type then
        execute 'drop table if exists gold.gold_test_features';
    end;
    begin
        execute 'drop view if exists gold.gold_unit_latest_health';
    exception when wrong_object_type then
        execute 'drop table if exists gold.gold_unit_latest_health';
    end;

    execute format(
        'create view gold.gold_train_features as select * from gold.gold_train_features__v%s',
        suffix
    );
    execute format(
        'create view gold.gold_test_features as select * from gold.gold_test_features__v%s',
        suffix
    );
    execute format(
        'create view gold.gold_unit_latest_health as select * from gold.gold_unit_latest_health__v%s',
        suffix
    );
end $$;
