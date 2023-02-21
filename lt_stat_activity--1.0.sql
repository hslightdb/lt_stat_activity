
\echo Use "CREATE EXTENSION lt_stat_activity" to load this file. \quit

CREATE FUNCTION lt_stat_activity_get_query_id(
    OUT pid int4,
	OUT query_id int8
)RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;

CREATE view lt_stat_activity
AS
    SELECT a.datid,
           a.datname,
           a.pid,
           a.leader_pid,
           a.usesysid,
           a.usename,
           a.application_name,
           a.client_addr,
           a.client_hostname,
           a.client_port,
           a.backend_start,
           a.xact_start,
           a.query_start,
           a.state_change,
           a.wait_event_type,
           a.wait_event,
           a.state,
           a.backend_xid,
           a.backend_xmin,
           b.query_id,
           a.query,
           a.backend_type
      FROM pg_stat_activity a 
 LEFT JOIN lt_stat_activity_get_query_id() b  
        ON a.pid = b.pid
     WHERE a.leader_pid IS NULL
     UNION
    SELECT a.datid,
           a.datname,
           a.pid,
           a.leader_pid,
           a.usesysid,
           a.usename,
           a.application_name,
           a.client_addr,
           a.client_hostname,
           a.client_port,
           a.backend_start,
           a.xact_start,
           a.query_start,
           a.state_change,
           a.wait_event_type,
           a.wait_event,
           a.state,
           a.backend_xid,
           a.backend_xmin,
           b.query_id,
           a.query,
           a.backend_type
      FROM pg_stat_activity a 
INNER JOIN pg_stat_activity c 
        ON c.leader_pid IS NULL
       AND a.leader_pid = c.pid	
 LEFT JOIN lt_stat_activity_get_query_id() b  
        ON c.pid = b.pid
     WHERE a.leader_pid IS NOT NULL;

GRANT SELECT ON lt_stat_activity TO PUBLIC;

CREATE TABLE sample_activity_history (
    id                serial,
	sample_time       timestamptz,
	datid             oid,
	datname           name,
	pid               integer,
	leader_pid        integer,
	usesysid          oid,
	usename           name,
	application_name  text,
	client_addr       inet,
	client_hostname   text,
	client_port       integer,
	backend_start     timestamptz,
	xact_start        timestamptz,
	query_start       timestamptz,
	state_change      timestamptz,
	wait_event_type   text,
	wait_event        text,
	state             text,
	backend_xid       xid,
	backend_xmin      xid,
    query_id          bigint,
	query             text,
	backend_type      text,
	CONSTRAINT        pk_sample_activity_history PRIMARY KEY (id)
);
COMMENT ON TABLE sample_activity_history IS 'lt_stat_activity';
CREATE TABLE sample_activity_profile (
    id                serial,
    sample_time       timestamptz,
    pid               integer,
    wait_event_type   text,
    wait_event        text,
    query_id          bigint,
    query             text,
    count             bigint,
    CONSTRAINT        pk_sample_activity_profile PRIMARY KEY (id)
);
COMMENT ON TABLE sample_activity_profile IS 'lt_stat_activity';
CREATE INDEX idx_sample_activity_profile ON sample_activity_profile(sample_time);

CREATE TABLE sample_active_session_history (
    id               serial,
    active_sessions  integer,
    sample_time      timestamp,
    CONSTRAINT       pk_active_session_history PRIMARY KEY (sample_time)
);
COMMENT ON TABLE sample_active_session_history IS 'lt_stat_activity';
CREATE TABLE sample_active_session_profile (
    id               serial,
    active_sessions  integer,
    sample_time      timestamp,
    CONSTRAINT       pk_active_session_profile PRIMARY KEY (sample_time)
);
COMMENT ON TABLE sample_active_session_profile IS 'lt_stat_activity';
CREATE FUNCTION collect_activity_history()
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
        coalesce(wait_event_type, 'Null'),
        coalesce(wait_event, 'Null'),
        state,
        backend_xid,
        backend_xmin,
        query_id,
        query,
        backend_type
    FROM lt_stat_activity;

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

CREATE FUNCTION collect_activity_profile()
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
        count
    )
    SELECT
        date_trunc('minute', sample_time) AS sample_time,
        pid,
        wait_event_type,
        wait_event,
        query_id,
        query,
        count(1) as count
    FROM sample_activity_history
	WHERE sample_time >= start_time AND sample_time < end_time
	GROUP BY wait_event, wait_event_type, query, query_id, pid, date_trunc('minute', sample_time);

    DELETE FROM sample_activity_history WHERE sample_time < delete_time;

    INSERT INTO sample_active_session_profile (
        active_sessions,
        sample_time
    )
    SELECT
        sum(active_sessions),
        now()
    FROM sample_active_session_history
    WHERE sample_time <= now();

    DELETE FROM sample_active_session_history WHERE sample_time <= now();
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION clean_activity_profile()
RETURNS VOID AS $$
DECLARE
    delete_time timestamptz := now() + '-1 month';
BEGIN
    DELETE FROM sample_activity_profile WHERE sample_time < delete_time;
    DELETE FROM sample_active_session_profile WHERE sample_time < delete_time;
END;
$$ LANGUAGE plpgsql;