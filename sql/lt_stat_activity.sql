-- test for lt_stat_activity

create extension lt_stat_statements;
create extension lt_stat_activity;

select query,query_id  from lt_stat_activity where pid = pg_backend_pid();
select query,query_id  from lt_stat_activity where pid = pg_backend_pid() AND 1 = 1;
select query,query_id  from lt_stat_activity where pid = pg_backend_pid() AND 2 = 2;
select query,query_id  from lt_stat_activity where pid = pg_backend_pid() AND query_id = query_id;

select query from pg_stat_statements where queryid = 2058761335241628816;
select query from pg_stat_statements where queryid = 5846314738819909906;
select query from pg_stat_statements where queryid = -984280587591178296;