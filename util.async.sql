\if :{?util_async_sql}
\else
\set util_async_sql true

\if :test
\if :local
    drop schema if exists util cascade;
\endif
\endif
create schema if not exists util;

create extension if not exists dblink;

create or replace function util.async (
    connstr text,
    sql text
)
    returns text
    language plpgsql
    security definer
as $$
declare
    conn text = connstr;
begin
    if coalesce(conn = any(dblink_get_connections()), false)
    then
        if dblink_is_busy(conn) = 1 then
            return null;
        end if;
    else
        conn = 'util.async.' || md5(
            connstr
            || pg_backend_pid()::text
            || clock_timestamp()::text
            || random()::text
        );
        if coalesce(conn = any(dblink_get_connections()), false)
        then
            if dblink_is_busy(conn) = 1 then
                return null;
            end if;
        else
            perform dblink_connect_u(conn, connstr);
        end if;
    end if;

    perform dblink_send_query(conn, sql);
    return conn;
end;
$$;

create or replace function util.await (
    conn text,
    timeout double precision default 0.0 -- in seconds
)
    returns jsonb
    language plpgsql
    security definer
as $$
declare
    n int = 0;
    sleep_int double precision = 0.01; -- (per doc, 0.01 is typical sleep interval)
    max_n int = timeout / sleep_int;
    data jsonb;
begin
    if max_n>0 then
        while dblink_is_busy(conn)
        loop
            perform pg_sleep(sleep_int);
            n = n + 1;

            if n>max_n then
                perform dblink_cancel_query(conn);

                if position('util.async.' in conn) = 1
                then
                    perform dblink_disconnect(conn);
                end if;

                raise exception 'util.async.timeout';
            end if;
        end loop;

    elsif dblink_is_busy(conn) then
        return null;
    end if;

    select t.data
    into data
    from dblink_get_result(conn) as t(data jsonb);

    if position('util.async.' in conn) = 1
    then
        perform dblink_disconnect(conn);
    end if;

    return data;
end;
$$;

\if :test
    create function tests.test_util_async_named_connection() returns setof text language plpgsql as $$
    declare
        conn text;
        data jsonb;
    begin
        perform dblink_connect_u('my_conn', 'dbname=web user=web');
        conn = util.async('my_conn', $x$select tests.delay()$x$);
        return next ok(
            conn = 'my_conn',
            'can re-use existing connection');
        return next ok(
            util.await(conn) is null,
            'data is yet avail');
        return next ok(
            util.async(
                'my_conn',
                $x$select tests.delay()$x$
            ) is null,
            'while connection busy it returns null');

        perform pg_sleep(0.2); -- do something-else

        return next ok(
            util.await(conn) is not null,
            'able to get async data');

        perform dblink_disconnect('my_conn');
    end;
    $$;

    create function tests.test_util_async_anon_connection() returns setof text language plpgsql as $$
    declare
        conn text;
        data jsonb;
    begin
        conn = util.async(
            'dbname=web user=web',
            $x$select tests.delay()$x$
        );
        return next ok(
            position('util.async.' in conn) = 1,
            'temporary name will be created');

        return next ok(
            (util.await(conn, 0.2))->>'hello' is not null,
            'waits async call with timeout');

    end;
    $$;

    create function tests.test_util_async_timed_out() returns setof text language plpgsql as $$
    declare
        conn text;
        data jsonb;
    begin
        begin
            perform util.await(util.async(
                'dbname=web user=web',
                $x$select tests.delay()$x$
            ), 0.02);
            return next ok(false, 'unable to capture before timeout');
        exception
            when others then
            return next ok(
                sqlerrm = 'util.async.timeout',
                'throws timeout error');
        end;
    end;
    $$;


    create function tests.test_util_async_multiple() returns setof text language plpgsql as $$
    declare
        async1 text;
        async2 text;
    begin
        async1 = util.async('dbname=web user=web', $x$select tests.delay(0.2)$x$);
        async2 = util.async('dbname=web user=web', $x$select tests.delay(0.1)$x$);
        perform pg_sleep(0.3); -- do something-else

        return next ok(((util.await(async1, 0.1))->>'hello')::float = 0.2, 'returns async1');
        return next ok(((util.await(async2, 0.1))->>'hello')::float = 0.1, 'returns async2');
    end;
    $$;

    create function tests.delay(sec double precision default 0.1)
    returns jsonb language plpgsql as $$
    begin
        perform pg_sleep(sec);
        return jsonb_build_object('hello', sec);
    end;
    $$;
\endif

\endif
