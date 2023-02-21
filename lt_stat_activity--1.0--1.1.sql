
\echo Use "CREATE EXTENSION lt_stat_activity" to load this file. \quit

ALTER TABLE sample_activity_profile ADD COLUMN state text;
ALTER TABLE sample_activity_history ALTER COLUMN id TYPE bigint;
ALTER TABLE sample_activity_profile ALTER COLUMN id TYPE bigint;
ALTER TABLE sample_active_session_history ALTER COLUMN id TYPE bigint;
ALTER TABLE sample_active_session_profile ALTER COLUMN id TYPE bigint;

CREATE OR REPLACE FUNCTION collect_activity_history()
RETURNS VOID AS $$
DECLARE
BEGIN
    INSERT INTO sample_activity_history(
        sample_time,
        datid,
        datname,
        pid,
        leader_pid,
        usesysid,
        usename,
        application_name,
        client_addr,
        client_hostname,
        client_port,
        backend_start,
        xact_start,
        query_start,
        state_change,
        wait_event_type,
        wait_event,
        state,
        backend_xid,
        backend_xmin,
        query_id,
        query,
        backend_type
    )
    SELECT
        now(),
        datid,
        datname,
        pid,
        leader_pid,
        usesysid,
        usename,
        application_name,
        client_addr,
        client_hostname,
        client_port,
        backend_start,
        xact_start,
        query_start,
        state_change,
        COALESCE(wait_event_type, CASE WHEN state = 'active' THEN 'DBCpu' ELSE 'Null' END) AS wait_event_type,
        COALESCE(wait_event, CASE WHEN state = 'active' THEN 'DBCpu' ELSE 'Null' END) AS wait_event,
        state,
        backend_xid,
        backend_xmin,
        query_id,
        query,
        backend_type
    FROM lt_stat_activity
    WHERE NOT ((COALESCE(wait_event_type, 'Null') != 'Activity' AND backend_type != 'client backend' AND COALESCE(state, 'Null') != 'idle')
              OR (COALESCE(wait_event_type, 'Null') = 'Client' AND COALESCE(wait_event, 'Null') = 'ClientRead'))
    AND application_name NOT LIKE '%pg_cron%'
    AND state NOT IN ('idle', 'idle in transaction');

    INSERT INTO sample_active_session_history (
        active_sessions,
        sample_time
    )
    SELECT
        count(1),
        now()
    FROM lt_stat_activity
    WHERE state = 'active';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collect_activity_profile()
RETURNS VOID AS $$
DECLARE
    last_sample_time timestamptz;
    start_time timestamptz;
    end_time timestamptz;
    delete_time timestamptz;
BEGIN
    SELECT max(sample_time) INTO last_sample_time FROM sample_activity_profile;

    IF last_sample_time IS NULL THEN
        start_time := to_timestamp('1900-01-01 00:00:01', 'yyyy-MM-dd hh24:mi:ss') ;
    ELSE
	    start_time := last_sample_time + '1 min';
    END IF;

    end_time := date_trunc('minute', now());
    delete_time := end_time + '-10 min';

    INSERT INTO sample_activity_profile(
        sample_time,
        pid,
        wait_event_type,
        wait_event,
        query_id,
        query,
        count,
        state
    )
    SELECT
        date_trunc('minute', sample_time) AS sample_time,
        pid,
        wait_event_type,
        wait_event,
        query_id,
        query,
        count(1) as count,
        state
    FROM sample_activity_history
	WHERE sample_time >= start_time AND sample_time < end_time
	GROUP BY wait_event, wait_event_type, state, query, query_id, pid, date_trunc('minute', sample_time)
    ORDER BY sample_time;

    DELETE FROM sample_activity_history WHERE sample_time < delete_time;

    INSERT INTO sample_active_session_profile (
        active_sessions,
        sample_time
    )
    SELECT
        sum(active_sessions),
        now()
    FROM sample_active_session_history
    WHERE sample_time >= start_time AND sample_time < end_time;

    DELETE FROM sample_active_session_history WHERE sample_time < delete_time;
END;
$$ LANGUAGE plpgsql;