


-- ----------------------------------------------------------------------
-- Section: Internal Tables
--
-- Overview:
--      pgq.queue                   - Queue configuration
--      pgq.consumer                - Consumer names
--      pgq.subscription            - Consumer registrations
--      pgq.tick                    - Per-queue snapshots (ticks)
--      pgq.event_*                 - Data tables
--      pgq.retry_queue             - Events to be retried later
--
-- 
-- Standard triggers store events in the pgq.event_* data tables
-- There is one top event table pgq.event_<queue_id> for each queue
-- inherited from pgq.event_template wuith three tables for actual data
-- pgq.event_<queue_id>_0 to pgq.event_<queue_id>_2.
--
-- The active table is rotated at interval, so that if all the consubers
-- have passed some poin the oldes one can be emptied using TRUNCATE command
-- for efficiency
-- 
-- 
-- ----------------------------------------------------------------------

set client_min_messages = 'warning';
set default_with_oids = 'off';

-- drop schema if exists pgq cascade;
create schema pgq;

-- ----------------------------------------------------------------------
-- Table: pgq.consumer
--
--      Name to id lookup for consumers
--
-- Columns:
--      co_id       - consumer's id for internal usage
--      co_name     - consumer's id for external usage
-- ----------------------------------------------------------------------
create table pgq.consumer (
        co_id       serial,
        co_name     text        not null,

        constraint consumer_pkey primary key (co_id),
        constraint consumer_name_uq UNIQUE (co_name)
);


-- ----------------------------------------------------------------------
-- Table: pgq.queue
--
--     Information about available queues
--
-- Columns:
--      queue_id                    - queue id for internal usage
--      queue_name                  - queue name visible outside
--      queue_ntables               - how many data tables the queue has
--      queue_cur_table             - which data table is currently active
--      queue_rotation_period       - period for data table rotation
--      queue_switch_step1          - tx when rotation happened
--      queue_switch_step2          - tx after rotation was committed
--      queue_switch_time           - time when switch happened
--      queue_external_ticker       - ticks come from some external sources
--      queue_ticker_paused         - ticker is paused
--      queue_disable_insert        - disallow pgq.insert_event()
--      queue_ticker_max_count      - batch should not contain more events
--      queue_ticker_max_lag        - events should not age more
--      queue_ticker_idle_period    - how often to tick when no events happen
--      queue_per_tx_limit          - Max number of events single TX can insert
--      queue_data_pfx              - prefix for data table names
--      queue_event_seq             - sequence for event id's
--      queue_tick_seq              - sequence for tick id's
--      queue_extra_maint           - array of functon names to call during maintenance
-- ----------------------------------------------------------------------
create table pgq.queue (
        queue_id                    serial,
        queue_name                  text        not null,

        queue_ntables               integer     not null default 3,
        queue_cur_table             integer     not null default 0,
        queue_rotation_period       interval    not null default '2 hours',
        queue_switch_step1          bigint      not null default txid_current(),
        queue_switch_step2          bigint               default txid_current(),
        queue_switch_time           timestamptz not null default now(),

        queue_external_ticker       boolean     not null default false,
        queue_disable_insert        boolean     not null default false,
        queue_ticker_paused         boolean     not null default false,

        queue_ticker_max_count      integer     not null default 500,
        queue_ticker_max_lag        interval    not null default '3 seconds',
        queue_ticker_idle_period    interval    not null default '1 minute',
        queue_per_tx_limit          integer,

        queue_data_pfx              text        not null,
        queue_event_seq             text        not null,
        queue_tick_seq              text        not null,

        queue_extra_maint           text[],

        constraint queue_pkey primary key (queue_id),
        constraint queue_name_uq unique (queue_name)
);

-- ----------------------------------------------------------------------
-- Table: pgq.tick
--
--      Snapshots for event batching
--
-- Columns:
--      tick_queue      - queue id whose tick it is
--      tick_id         - ticks id (per-queue)
--      tick_time       - time when tick happened
--      tick_snapshot   - transaction state
--      tick_event_seq  - last value for event seq
-- ----------------------------------------------------------------------
create table pgq.tick (
        tick_queue                  int4            not null,
        tick_id                     bigint          not null,
        tick_time                   timestamptz     not null default now(),
        tick_snapshot               txid_snapshot   not null default txid_current_snapshot(),
        tick_event_seq              bigint          not null, -- may be NULL on upgraded dbs

        constraint tick_pkey primary key (tick_queue, tick_id),
        constraint tick_queue_fkey foreign key (tick_queue)
                                   references pgq.queue (queue_id)
);

-- ----------------------------------------------------------------------
-- Sequence: pgq.batch_id_seq
--
--      Sequence for batch id's.
-- ----------------------------------------------------------------------
create sequence pgq.batch_id_seq;

-- ----------------------------------------------------------------------
-- Table: pgq.subscription
--
--      Consumer registration on a queue.
--
-- Columns:
--
--      sub_id          - subscription id for internal usage
--      sub_queue       - queue id
--      sub_consumer    - consumer's id
--      sub_last_tick   - last tick the consumer processed
--      sub_batch       - shortcut for queue_id/consumer_id/tick_id
--      sub_next_tick   - batch end pos
-- ----------------------------------------------------------------------
create table pgq.subscription (
        sub_id                          serial      not null,
        sub_queue                       int4        not null,
        sub_consumer                    int4        not null,
        sub_last_tick                   bigint,
        sub_active                      timestamptz not null default now(),
        sub_batch                       bigint,
        sub_next_tick                   bigint,

        constraint subscription_pkey primary key (sub_queue, sub_consumer),
        constraint subscription_batch_idx unique (sub_batch),
        constraint sub_queue_fkey foreign key (sub_queue)
                                   references pgq.queue (queue_id),
        constraint sub_consumer_fkey foreign key (sub_consumer)
                                   references pgq.consumer (co_id)
);

-- ----------------------------------------------------------------------
-- Table: pgq.event_template
--
--      Parent table for all event tables
--
-- Columns:
--      ev_id               - event's id, supposed to be unique per queue
--      ev_time             - when the event was inserted
--      ev_txid             - transaction id which inserted the event
--      ev_owner            - subscription id that wanted to retry this
--      ev_retry            - how many times the event has been retried, NULL for new events
--      ev_type             - consumer/producer can specify what the data fields contain
--      ev_data             - data field
--      ev_extra1           - extra data field
--      ev_extra2           - extra data field
--      ev_extra3           - extra data field
--      ev_extra4           - extra data field
-- ----------------------------------------------------------------------
create table pgq.event_template (
        ev_id               bigint          not null,
        ev_time             timestamptz     not null,

        ev_txid             bigint          not null default txid_current(),
        ev_owner            int4,
        ev_retry            int4,

        ev_type             text,
        ev_data             text,
        ev_extra1           text,
        ev_extra2           text,
        ev_extra3           text,
        ev_extra4           text
);

-- ----------------------------------------------------------------------
-- Table: pgq.retry_queue
--
--      Events to be retried.  When retry time reaches, they will
--      be put back into main queue.
--
-- Columns:
--      ev_retry_after          - time when it should be re-inserted to main queue
--      ev_queue                - queue id, used to speed up event copy into queue
--      *                       - same as pgq.event_template
-- ----------------------------------------------------------------------
create table pgq.retry_queue (
    ev_retry_after          timestamptz     not null,
    ev_queue                int4            not null,

    like pgq.event_template,

    constraint rq_pkey primary key (ev_owner, ev_id),
    constraint rq_queue_id_fkey foreign key (ev_queue)
                             references pgq.queue (queue_id)
);
alter table pgq.retry_queue alter column ev_owner set not null;
alter table pgq.retry_queue alter column ev_txid drop not null;
create index rq_retry_idx on pgq.retry_queue (ev_retry_after);



-- Section: Internal Functions

-- install & launch schema upgrade


create or replace function pgq.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    cnt int4 = 0;
begin

    -- pgq.subscription.sub_last_tick: NOT NULL -> NULL
    perform 1 from information_schema.columns
      where table_schema = 'pgq'
        and table_name = 'subscription'
        and column_name ='sub_last_tick'
        and is_nullable = 'NO';
    if found then
        alter table pgq.subscription
            alter column sub_last_tick
            drop not null;
        cnt := cnt + 1;
    end if;

    -- create roles
    perform 1 from pg_catalog.pg_roles where rolname = 'pgq_reader';
    if not found then
        create role pgq_reader;
        cnt := cnt + 1;
    end if;
    perform 1 from pg_catalog.pg_roles where rolname = 'pgq_writer';
    if not found then
        create role pgq_writer;
        cnt := cnt + 1;
    end if;
    perform 1 from pg_catalog.pg_roles where rolname = 'pgq_admin';
    if not found then
        create role pgq_admin in role pgq_reader, pgq_writer;
        cnt := cnt + 1;
    end if;

    perform 1 from pg_attribute
        where attrelid = 'pgq.queue'::regclass
          and attname = 'queue_extra_maint';
    if not found then
        alter table pgq.queue add column queue_extra_maint text[];
    end if;

    return 0;
end;
$$ language plpgsql;


select pgq.upgrade_schema();

-- Group: Low-level event handling


create or replace function pgq.batch_event_sql(x_batch_id bigint)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_event_sql(1)
--      Creates SELECT statement that fetches events for this batch.
--
-- Parameters:
--      x_batch_id    - ID of a active batch.
--
-- Returns:
--      SQL statement.
-- ----------------------------------------------------------------------

-- ----------------------------------------------------------------------
-- Algorithm description:
--      Given 2 snapshots, sn1 and sn2 with sn1 having xmin1, xmax1
--      and sn2 having xmin2, xmax2 create expression that filters
--      right txid's from event table.
--
--      Simplest solution would be
--      > WHERE ev_txid >= xmin1 AND ev_txid <= xmax2
--      >   AND NOT txid_visible_in_snapshot(ev_txid, sn1)
--      >   AND txid_visible_in_snapshot(ev_txid, sn2)
--
--      The simple solution has a problem with long transactions (xmin1 very low).
--      All the batches that happen when the long tx is active will need
--      to scan all events in that range.  Here is 2 optimizations used:
--
--      1)  Use [xmax1..xmax2] for range scan.  That limits the range to
--      txids that actually happened between two snapshots.  For txids
--      in the range [xmin1..xmax1] look which ones were actually
--      committed between snapshots and search for them using exact
--      values using IN (..) list.
--
--      2) As most TX are short, there could be lot of them that were
--      just below xmax1, but were committed before xmax2.  So look
--      if there are ID's near xmax1 and lower the range to include
--      them, thus decresing size of IN (..) list.
-- ----------------------------------------------------------------------
declare
    rec             record;
    sql             text;
    tbl             text;
    arr             text;
    part            text;
    select_fields   text;
    retry_expr      text;
    batch           record;
begin
    select s.sub_last_tick, s.sub_next_tick, s.sub_id, s.sub_queue,
           txid_snapshot_xmax(last.tick_snapshot) as tx_start,
           txid_snapshot_xmax(cur.tick_snapshot) as tx_end,
           last.tick_snapshot as last_snapshot,
           cur.tick_snapshot as cur_snapshot
        into batch
        from pgq.subscription s, pgq.tick last, pgq.tick cur
        where s.sub_batch = x_batch_id
          and last.tick_queue = s.sub_queue
          and last.tick_id = s.sub_last_tick
          and cur.tick_queue = s.sub_queue
          and cur.tick_id = s.sub_next_tick;
    if not found then
        raise exception 'batch not found';
    end if;

    -- load older transactions
    arr := '';
    for rec in
        -- active tx-es in prev_snapshot that were committed in cur_snapshot
        select id1 from
            txid_snapshot_xip(batch.last_snapshot) id1 left join
            txid_snapshot_xip(batch.cur_snapshot) id2 on (id1 = id2)
        where id2 is null
        order by 1 desc
    loop
        -- try to avoid big IN expression, so try to include nearby
        -- tx'es into range
        if batch.tx_start - 100 <= rec.id1 then
            batch.tx_start := rec.id1;
        else
            if arr = '' then
                arr := rec.id1::text;
            else
                arr := arr || ',' || rec.id1::text;
            end if;
        end if;
    end loop;

    -- must match pgq.event_template
    select_fields := 'select ev_id, ev_time, ev_txid, ev_retry, ev_type,'
        || ' ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4';
    retry_expr :=  ' and (ev_owner is null or ev_owner = '
        || batch.sub_id::text || ')';

    -- now generate query that goes over all potential tables
    sql := '';
    for rec in
        select xtbl from pgq.batch_event_tables(x_batch_id) xtbl
    loop
        tbl := pgq.quote_fqname(rec.xtbl);
        -- this gets newer queries that definitely are not in prev_snapshot
        part := select_fields
            || ' from pgq.tick cur, pgq.tick last, ' || tbl || ' ev '
            || ' where cur.tick_id = ' || batch.sub_next_tick::text
            || ' and cur.tick_queue = ' || batch.sub_queue::text
            || ' and last.tick_id = ' || batch.sub_last_tick::text
            || ' and last.tick_queue = ' || batch.sub_queue::text
            || ' and ev.ev_txid >= ' || batch.tx_start::text
            || ' and ev.ev_txid <= ' || batch.tx_end::text
            || ' and txid_visible_in_snapshot(ev.ev_txid, cur.tick_snapshot)'
            || ' and not txid_visible_in_snapshot(ev.ev_txid, last.tick_snapshot)'
            || retry_expr;
        -- now include older tx-es, that were ongoing
        -- at the time of prev_snapshot
        if arr <> '' then
            part := part || ' union all '
                || select_fields || ' from ' || tbl || ' ev '
                || ' where ev.ev_txid in (' || arr || ')'
                || retry_expr;
        end if;
        if sql = '' then
            sql := part;
        else
            sql := sql || ' union all ' || part;
        end if;
    end loop;
    if sql = '' then
        raise exception 'could not construct sql for batch %', x_batch_id;
    end if;
    return sql || ' order by 1';
end;
$$ language plpgsql;  -- no perms needed



create or replace function pgq.batch_event_tables(x_batch_id bigint)
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_event_tables(1)
--
--     Returns set of table names where this batch events may reside.
--
-- Parameters:
--     x_batch_id    - ID of a active batch.
-- ----------------------------------------------------------------------
declare
    nr                    integer;
    tbl                   text;
    use_prev              integer;
    use_next              integer;
    batch                 record;
begin
    select
           txid_snapshot_xmin(last.tick_snapshot) as tx_min, -- absolute minimum
           txid_snapshot_xmax(cur.tick_snapshot) as tx_max, -- absolute maximum
           q.queue_data_pfx, q.queue_ntables,
           q.queue_cur_table, q.queue_switch_step1, q.queue_switch_step2
        into batch
        from pgq.tick last, pgq.tick cur, pgq.subscription s, pgq.queue q
        where cur.tick_id = s.sub_next_tick
          and cur.tick_queue = s.sub_queue
          and last.tick_id = s.sub_last_tick
          and last.tick_queue = s.sub_queue
          and s.sub_batch = x_batch_id
          and q.queue_id = s.sub_queue;
    if not found then
        raise exception 'Cannot find data for batch %', x_batch_id;
    end if;

    -- if its definitely not in one or other, look into both
    if batch.tx_max < batch.queue_switch_step1 then
        use_prev := 1;
        use_next := 0;
    elsif batch.queue_switch_step2 is not null
      and (batch.tx_min > batch.queue_switch_step2)
    then
        use_prev := 0;
        use_next := 1;
    else
        use_prev := 1;
        use_next := 1;
    end if;

    if use_prev then
        nr := batch.queue_cur_table - 1;
        if nr < 0 then
            nr := batch.queue_ntables - 1;
        end if;
        tbl := batch.queue_data_pfx || '_' || nr::text;
        return next tbl;
    end if;

    if use_next then
        tbl := batch.queue_data_pfx || '_' || batch.queue_cur_table::text;
        return next tbl;
    end if;

    return;
end;
$$ language plpgsql; -- no perms needed




create or replace function pgq.event_retry_raw(
    x_queue text,
    x_consumer text,
    x_retry_after timestamptz,
    x_ev_id bigint,
    x_ev_time timestamptz,
    x_ev_retry integer,
    x_ev_type text,
    x_ev_data text,
    x_ev_extra1 text,
    x_ev_extra2 text,
    x_ev_extra3 text,
    x_ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry_raw(12)
--
--      Allows full control over what goes to retry queue.
--
-- Parameters:
--      x_queue         - name of the queue
--      x_consumer      - name of the consumer
--      x_retry_after   - when the event should be processed again
--      x_ev_id         - event id
--      x_ev_time       - creation time
--      x_ev_retry      - retry count
--      x_ev_type       - user data
--      x_ev_data       - user data
--      x_ev_extra1     - user data
--      x_ev_extra2     - user data
--      x_ev_extra3     - user data
--      x_ev_extra4     - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
declare
    q record;
    id bigint;
begin
    select sub_id, queue_event_seq, sub_queue into q
      from pgq.consumer, pgq.queue, pgq.subscription
     where queue_name = x_queue
       and co_name = x_consumer
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'consumer not registered';
    end if;

    id := x_ev_id;
    if id is null then
        id := nextval(q.queue_event_seq);
    end if;

    insert into pgq.retry_queue (ev_retry_after, ev_queue,
            ev_id, ev_time, ev_owner, ev_retry,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    values (x_retry_after, q.sub_queue,
            id, x_ev_time, q.sub_id, x_ev_retry,
            x_ev_type, x_ev_data, x_ev_extra1, x_ev_extra2,
            x_ev_extra3, x_ev_extra4);

    return id;
end;
$$ language plpgsql security definer;



create or replace function pgq.find_tick_helper(
    in i_queue_id int4,
    in i_prev_tick_id int8,
    in i_prev_tick_time timestamptz,
    in i_prev_tick_seq int8,
    in i_min_count int8,
    in i_min_interval interval,
    out next_tick_id int8,
    out next_tick_time timestamptz,
    out next_tick_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq.find_tick_helper(6)
--
--      Helper function for pgq.next_batch_custom() to do extended tick search.
-- ----------------------------------------------------------------------
declare
    sure    boolean;
    can_set boolean;
    t       record;
    cnt     int8;
    ival    interval;
begin
    -- first, fetch last tick of the queue
    select tick_id, tick_time, tick_event_seq into t
        from pgq.tick
        where tick_queue = i_queue_id
          and tick_id > i_prev_tick_id
        order by tick_queue desc, tick_id desc
        limit 1;
    if not found then
        return;
    end if;
    
    -- check whether batch would end up within reasonable limits
    sure := true;
    can_set := false;
    if i_min_count is not null then
        cnt = t.tick_event_seq - i_prev_tick_seq;
        if cnt >= i_min_count then
            can_set := true;
        end if;
        if cnt > i_min_count * 2 then
            sure := false;
        end if;
    end if;
    if i_min_interval is not null then
        ival = t.tick_time - i_prev_tick_time;
        if ival >= i_min_interval then
            can_set := true;
        end if;
        if ival > i_min_interval * 2 then
            sure := false;
        end if;
    end if;

    -- if last tick too far away, do large scan
    if not sure then
        select tick_id, tick_time, tick_event_seq into t
            from pgq.tick
            where tick_queue = i_queue_id
              and tick_id > i_prev_tick_id
              and ((i_min_count is not null and (tick_event_seq - i_prev_tick_seq) >= i_min_count)
                  or
                   (i_min_interval is not null and (tick_time - i_prev_tick_time) >= i_min_interval))
            order by tick_queue asc, tick_id asc
            limit 1;
        can_set := true;
    end if;
    if can_set then
        next_tick_id := t.tick_id;
        next_tick_time := t.tick_time;
        next_tick_seq := t.tick_event_seq;
    end if;
    return;
end;
$$ language plpgsql stable;



-- Group: Ticker


create or replace function pgq.ticker(i_queue_name text, i_tick_id bigint, i_orig_timestamp timestamptz, i_event_seq bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(3)
--
--     External ticker: Insert a tick with a particular tick_id and timestamp.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--     i_tick_id        - Id of new tick.
--
-- Returns:
--     Tick id.
-- ----------------------------------------------------------------------
begin
    insert into pgq.tick (tick_queue, tick_id, tick_time, tick_event_seq)
    select queue_id, i_tick_id, i_orig_timestamp, i_event_seq
        from pgq.queue
        where queue_name = i_queue_name
          and queue_external_ticker
          and not queue_ticker_paused;
    if not found then
        raise exception 'queue not found or ticker disabled: %', i_queue_name;
    end if;

    -- make sure seqs stay current
    perform pgq.seq_setval(queue_tick_seq, i_tick_id),
            pgq.seq_setval(queue_event_seq, i_event_seq)
        from pgq.queue
        where queue_name = i_queue_name;

    return i_tick_id;
end;
$$ language plpgsql security definer; -- unsure about access


create or replace function pgq.ticker(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(1)
--
--     Check if tick is needed for the queue and insert it.
--
--     For pgqadm usage.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--
-- Returns:
--     Tick id or NULL if no tick was done.
-- ----------------------------------------------------------------------
declare
    res bigint;
    q record;
    state record;
    last2 record;
begin
    select queue_id, queue_tick_seq, queue_external_ticker,
            queue_ticker_max_count, queue_ticker_max_lag,
            queue_ticker_idle_period, queue_event_seq,
            pgq.seq_getval(queue_event_seq) as event_seq,
            queue_ticker_paused
        into q
        from pgq.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'no such queue';
    end if;

    if q.queue_external_ticker then
        raise exception 'This queue has external tick source.';
    end if;

    if q.queue_ticker_paused then
        raise exception 'Ticker has been paused for this queue';
    end if;

    -- load state from last tick
    select now() - tick_time as lag,
           q.event_seq - tick_event_seq as new_events,
           tick_id, tick_time, tick_event_seq,
           txid_snapshot_xmax(tick_snapshot) as sxmax,
           txid_snapshot_xmin(tick_snapshot) as sxmin
        into state
        from pgq.tick
        where tick_queue = q.queue_id
        order by tick_queue desc, tick_id desc
        limit 1;

    if found then
        if state.sxmin > txid_current() then
            raise exception 'Invalid PgQ state: old xmin=%, old xmax=%, cur txid=%',
                            state.sxmin, state.sxmax, txid_current();
        end if;
        if state.new_events < 0 then
            raise warning 'Negative new_events?  old=% cur=%', state.tick_event_seq, q.event_seq;
        end if;
        if state.sxmax > txid_current() then
            raise warning 'Dubious PgQ state: old xmax=%, cur txid=%', state.sxmax, txid_current();
        end if;

        if state.new_events > 0 then
            -- there are new events, should we wait a bit?
            if state.new_events < q.queue_ticker_max_count
                and state.lag < q.queue_ticker_max_lag
            then
                return NULL;
            end if;
        else
            -- no new events, should we apply idle period?
            -- check previous event from the last one.
            select state.tick_time - tick_time as lag
                into last2
                from pgq.tick
                where tick_queue = q.queue_id
                    and tick_id < state.tick_id
                order by tick_queue desc, tick_id desc
                limit 1;
            if found then
                -- gradually decrease the tick frequency
                if (state.lag < q.queue_ticker_max_lag / 2)
                    or
                   (state.lag < last2.lag * 2
                    and state.lag < q.queue_ticker_idle_period)
                then
                    return NULL;
                end if;
            end if;
        end if;
    end if;

    insert into pgq.tick (tick_queue, tick_id, tick_event_seq)
        values (q.queue_id, nextval(q.queue_tick_seq), q.event_seq);

    return currval(q.queue_tick_seq);
end;
$$ language plpgsql security definer; -- unsure about access

create or replace function pgq.ticker() returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(0)
--
--     Creates ticks for all unpaused queues which dont have external ticker.
--
-- Returns:
--     Number of queues that were processed.
-- ----------------------------------------------------------------------
declare
    res bigint;
    q record;
begin
    res := 0;
    for q in
        select queue_name from pgq.queue
            where not queue_external_ticker
                  and not queue_ticker_paused
            order by queue_name
    loop
        if pgq.ticker(q.queue_name) > 0 then
            res := res + 1;
        end if;
    end loop;
    return res;
end;
$$ language plpgsql security definer;



-- Group: Periodic maintenence


create or replace function pgq.maint_retry_events()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_retry_events(0)
--
--      Moves retry events back to main queue.
--
--      It moves small amount at a time.  It should be called
--      until it returns 0
--
-- Returns:
--      Number of events processed.
-- ----------------------------------------------------------------------
declare
    cnt    integer;
    rec    record;
begin
    cnt := 0;

    -- allow only single event mover at a time, without affecting inserts
    lock table pgq.retry_queue in share update exclusive mode;

    for rec in
        select queue_name,
               ev_id, ev_time, ev_owner, ev_retry, ev_type, ev_data,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
          from pgq.retry_queue, pgq.queue
         where ev_retry_after <= current_timestamp
           and queue_id = ev_queue
         order by ev_retry_after
         limit 10
    loop
        cnt := cnt + 1;
        perform pgq.insert_event_raw(rec.queue_name,
                    rec.ev_id, rec.ev_time, rec.ev_owner, rec.ev_retry,
                    rec.ev_type, rec.ev_data, rec.ev_extra1, rec.ev_extra2,
                    rec.ev_extra3, rec.ev_extra4);
        delete from pgq.retry_queue
         where ev_owner = rec.ev_owner
           and ev_id = rec.ev_id;
    end loop;
    return cnt;
end;
$$ language plpgsql; -- need admin access



create or replace function pgq.maint_rotate_tables_step1(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_rotate_tables_step1(1)
--
--      Rotate tables for one queue.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--
-- Returns:
--      0
-- ----------------------------------------------------------------------
declare
    badcnt          integer;
    cf              record;
    nr              integer;
    tbl             text;
    lowest_tick_id  int8;
    lowest_xmin     int8;
begin
    -- check if needed and load record
    select * from pgq.queue into cf
        where queue_name = i_queue_name
          and queue_rotation_period is not null
          and queue_switch_step2 is not null
          and queue_switch_time + queue_rotation_period < current_timestamp
        for update;
    if not found then
        return 0;
    end if;

    -- if DB is in invalid state, stop
    if txid_current() < cf.queue_switch_step1 then
        raise exception 'queue % maint failure: step1=%, current=%',
                i_queue_name, cf.queue_switch_step1, txid_current();
    end if;

    -- find lowest tick for that queue
    select min(sub_last_tick) into lowest_tick_id
      from pgq.subscription
     where sub_queue = cf.queue_id;

    -- if some consumer exists
    if lowest_tick_id is not null then
        -- is the slowest one still on previous table?
        select txid_snapshot_xmin(tick_snapshot) into lowest_xmin
          from pgq.tick
         where tick_queue = cf.queue_id
           and tick_id = lowest_tick_id;
        if not found then
            raise exception 'queue % maint failure: tick % not found', i_queue_name, lowest_tick_id;
        end if;
        if lowest_xmin <= cf.queue_switch_step2 then
            return 0; -- skip rotation then
        end if;
    end if;

    -- nobody on previous table, we can rotate
    
    -- calc next table number and name
    nr := cf.queue_cur_table + 1;
    if nr = cf.queue_ntables then
        nr := 0;
    end if;
    tbl := cf.queue_data_pfx || '_' || nr::text;

    -- there may be long lock on the table from pg_dump,
    -- detect it and skip rotate then
    begin
        execute 'lock table ' || pgq.quote_fqname(tbl) || ' nowait';
        execute 'truncate ' || pgq.quote_fqname(tbl);
    exception
        when lock_not_available then
            -- cannot truncate, skipping rotate
            return 0;
    end;

    -- remember the moment
    update pgq.queue
        set queue_cur_table = nr,
            queue_switch_time = current_timestamp,
            queue_switch_step1 = txid_current(),
            queue_switch_step2 = NULL
        where queue_id = cf.queue_id;

    -- Clean ticks by using step2 txid from previous rotation.
    -- That should keep all ticks for all batches that are completely
    -- in old table.  This keeps them for longer than needed, but:
    -- 1. we want the pgq.tick table to be big, to avoid Postgres
    --    accitentally switching to seqscans on that.
    -- 2. that way we guarantee to consumers that they an be moved
    --    back on the queue at least for one rotation_period.
    --    (may help in disaster recovery)
    delete from pgq.tick
        where tick_queue = cf.queue_id
          and txid_snapshot_xmin(tick_snapshot) < cf.queue_switch_step2;

    return 0;
end;
$$ language plpgsql; -- need admin access


create or replace function pgq.maint_rotate_tables_step2()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_rotate_tables_step2(0)
--
--      Stores the txid when the rotation was visible.  It should be
--      called in separate transaction than pgq.maint_rotate_tables_step1()
-- ----------------------------------------------------------------------
begin
    update pgq.queue
       set queue_switch_step2 = txid_current()
     where queue_switch_step2 is null;
    return 0;
end;
$$ language plpgsql; -- need admin access



create or replace function pgq.maint_tables_to_vacuum()
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_tables_to_vacuum(0)
--
--      Returns list of tablenames that need frequent vacuuming.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Returns:
--      List of table names.
-- ----------------------------------------------------------------------
declare
    scm text;
    tbl text;
    fqname text;
begin
    -- assume autovacuum handles them fine
    if current_setting('autovacuum') = 'on' then
        return;
    end if;

    for scm, tbl in values
        ('pgq', 'subscription'),
        ('pgq', 'consumer'),
        ('pgq', 'queue'),
        ('pgq', 'tick'),
        ('pgq', 'retry_queue'),
        ('pgq_ext', 'completed_tick'),
        ('pgq_ext', 'completed_batch'),
        ('pgq_ext', 'completed_event'),
        ('pgq_ext', 'partial_batch'),
        --('pgq_node', 'node_location'),
        --('pgq_node', 'node_info'),
        ('pgq_node', 'local_state'),
        --('pgq_node', 'subscriber_info'),
        --('londiste', 'table_info'),
        ('londiste', 'seq_info'),
        --('londiste', 'applied_execute'),
        --('londiste', 'pending_fkeys'),
        ('txid', 'epoch'),
        ('londiste', 'completed')
    loop
        select n.nspname || '.' || t.relname into fqname
            from pg_class t, pg_namespace n
            where n.oid = t.relnamespace
                and n.nspname = scm
                and t.relname = tbl;
        if found then
            return next fqname;
        end if;
    end loop;
    return;
end;
$$ language plpgsql;




create or replace function pgq.maint_operations(out func_name text, out func_arg text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_operations(0)
--
--      Returns list of functions to call for maintenance.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Function signature:
--      Function should take either 1 or 0 arguments and return 1 if it wants
--      to be called immediately again, 0 if not.
--
-- Returns:
--      func_name   - Function to call
--      func_arg    - Optional argument to function (queue name)
-- ----------------------------------------------------------------------
declare
    ops text[];
    nrot int4;
begin
    -- rotate step 1
    nrot := 0;
    func_name := 'pgq.maint_rotate_tables_step1';
    for func_arg in
        select queue_name from pgq.queue
            where queue_rotation_period is not null
                and queue_switch_step2 is not null
                and queue_switch_time + queue_rotation_period < current_timestamp
            order by 1
    loop
        nrot := nrot + 1;
        return next;
    end loop;

    -- rotate step 2
    if nrot = 0 then
        select count(1) from pgq.queue
            where queue_rotation_period is not null
                and queue_switch_step2 is null
            into nrot;
    end if;
    if nrot > 0 then
        func_name := 'pgq.maint_rotate_tables_step2';
        func_arg := NULL;
        return next;
    end if;

    -- check if extra field exists
    perform 1 from pg_attribute
      where attrelid = 'pgq.queue'::regclass
        and attname = 'queue_extra_maint';
    if found then
        -- add extra ops
        for func_arg, ops in
            select q.queue_name, queue_extra_maint from pgq.queue q
             where queue_extra_maint is not null
             order by 1
        loop
            for i in array_lower(ops, 1) .. array_upper(ops, 1)
            loop
                func_name = ops[i];
                return next;
            end loop;
        end loop;
    end if;

    -- vacuum tables
    func_name := 'vacuum';
    for func_arg in
        select * from pgq.maint_tables_to_vacuum()
    loop
        return next;
    end loop;

    --
    -- pgq_node & londiste
    --
    -- although they belong to queue_extra_maint, they are
    -- common enough so its more effective to handle them here.
    --

    perform 1 from pg_proc p, pg_namespace n
      where p.pronamespace = n.oid
        and n.nspname = 'pgq_node'
        and p.proname = 'maint_watermark';
    if found then
        func_name := 'pgq_node.maint_watermark';
        for func_arg in
            select n.queue_name
              from pgq_node.node_info n
              where n.node_type = 'root'
        loop
            return next;
        end loop;

    end if;

    perform 1 from pg_proc p, pg_namespace n
      where p.pronamespace = n.oid
        and n.nspname = 'londiste'
        and p.proname = 'root_check_seqs';
    if found then
        func_name := 'londiste.root_check_seqs';
        for func_arg in
            select distinct s.queue_name
              from londiste.seq_info s, pgq_node.node_info n
              where s.local
                and n.node_type = 'root'
                and n.queue_name = s.queue_name
        loop
            return next;
        end loop;
    end if;

    perform 1 from pg_proc p, pg_namespace n
      where p.pronamespace = n.oid
        and n.nspname = 'londiste'
        and p.proname = 'periodic_maintenance';
    if found then
        func_name := 'londiste.periodic_maintenance';
        func_arg := NULL;
        return next;
    end if;

    return;
end;
$$ language plpgsql;



-- Group: Random utility functions


create or replace function pgq.grant_perms(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.grant_perms(1)
--
--      Make event tables readable by public.
--
-- Parameters:
--      x_queue_name        - Name of the queue.
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    q           record;
    i           integer;
    pos         integer;
    tbl_perms   text;
    seq_perms   text;
    dst_schema  text;
    dst_table   text;
    part_table  text;
begin
    select * from pgq.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;

    -- split data table name to components
    pos := position('.' in q.queue_data_pfx);
    if pos > 0 then
        dst_schema := substring(q.queue_data_pfx for pos - 1);
        dst_table := substring(q.queue_data_pfx from pos + 1);
    else
        dst_schema := 'public';
        dst_table := q.queue_data_pfx;
    end if;

    -- tick seq, normal users don't need to modify it
    execute 'grant select on ' || pgq.quote_fqname(q.queue_tick_seq) || ' to public';

    -- event seq
    execute 'grant select on ' || pgq.quote_fqname(q.queue_event_seq) || ' to public';
    -- execute 'grant usage on ' || pgq.quote_fqname(q.queue_event_seq) || ' to pgq_admin';

    -- set grants on parent table
    perform pgq._grant_perms_from('pgq', 'event_template', dst_schema, dst_table);

    -- set grants on real event tables
    for i in 0 .. q.queue_ntables - 1 loop
        part_table := dst_table  || '_' || i::text;
        perform pgq._grant_perms_from('pgq', 'event_template', dst_schema, part_table);
    end loop;

    return 1;
end;
$$ language plpgsql security definer;

create or replace function pgq._grant_perms_from(src_schema text, src_table text, dst_schema text, dst_table text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.grant_perms_from(1)
--
--      Copy grants from one table to another.
--      Workaround for missing GRANTS option for CREATE TABLE LIKE.
-- ----------------------------------------------------------------------
declare
    fq_table text;
    sql text;
    g record;
    q_grantee text;
begin
    fq_table := quote_ident(dst_schema) || '.' || quote_ident(dst_table);

    for g in
        select grantor, grantee, privilege_type, is_grantable
            from information_schema.table_privileges
            where table_schema = src_schema
                and table_name = src_table
    loop
        if g.grantee = 'PUBLIC' then
            q_grantee = 'public';
        else
            q_grantee = quote_ident(g.grantee);
        end if;
        sql := 'grant ' || g.privilege_type || ' on ' || fq_table
            || ' to ' || q_grantee;
        if g.is_grantable = 'YES' then
            sql := sql || ' with grant option';
        end if;
        execute sql;
    end loop;

    return 1;
end;
$$ language plpgsql strict;



create or replace function pgq.tune_storage(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.tune_storage(1)
--
--      Tunes storage settings for queue data tables
-- ----------------------------------------------------------------------
declare
    tbl  text;
    tbloid oid;
    q record;
    i int4;
    sql text;
    pgver int4;
begin
    pgver := current_setting('server_version_num');

    select * into q
      from pgq.queue where queue_name = i_queue_name;
    if not found then
        return 0;
    end if;

    for i in 0 .. (q.queue_ntables - 1) loop
        tbl := q.queue_data_pfx || '_' || i::text;

        -- set fillfactor
        sql := 'alter table ' || tbl || ' set (fillfactor = 100';

        -- autovacuum for 8.4+
        if pgver >= 80400 then
            sql := sql || ', autovacuum_enabled=off, toast.autovacuum_enabled =off';
        end if;
        sql := sql || ')';
        execute sql;

        -- autovacuum for 8.3
        if pgver < 80400 then
            tbloid := tbl::regclass::oid;
            delete from pg_catalog.pg_autovacuum where vacrelid = tbloid;
            insert into pg_catalog.pg_autovacuum values (tbloid, false, -1,-1,-1,-1,-1,-1,-1,-1);
        end if;
    end loop;

    return 1;
end;
$$ language plpgsql strict;





create or replace function pgq.force_tick(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.force_tick(2)
--
--      Simulate lots of events happening to force ticker to tick.
--
--      Should be called in loop, with some delay until last tick
--      changes or too much time is passed.
--
--      Such function is needed because paraller calls of pgq.ticker() are
--      dangerous, and cannot be protected with locks as snapshot
--      is taken before locking.
--
-- Parameters:
--      i_queue_name     - Name of the queue
--
-- Returns:
--      Currently last tick id.
-- ----------------------------------------------------------------------
declare
    q  record;
    t  record;
begin
    -- bump seq and get queue id
    select queue_id,
           setval(queue_event_seq, nextval(queue_event_seq)
                                   + queue_ticker_max_count * 2 + 1000) as tmp
      into q from pgq.queue
     where queue_name = i_queue_name
       and not queue_external_ticker
       and not queue_ticker_paused;

    --if not found then
    --    raise notice 'queue not found or ticks not allowed';
    --end if;

    -- return last tick id
    select tick_id into t
      from pgq.tick, pgq.queue
     where tick_queue = queue_id and queue_name = i_queue_name
     order by tick_queue desc, tick_id desc limit 1;

    return t.tick_id;
end;
$$ language plpgsql security definer;




create or replace function pgq.seq_getval(i_seq_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.seq_getval(1)
--
--      Read current last_val from seq, without affecting it.
--
-- Parameters:
--      i_seq_name     - Name of the sequence
--
-- Returns:
--      last value.
-- ----------------------------------------------------------------------
declare
    res     int8;
    fqname  text;
    pos     integer;
    s       text;
    n       text;
begin
    pos := position('.' in i_seq_name);
    if pos > 0 then
        s := substring(i_seq_name for pos - 1);
        n := substring(i_seq_name from pos + 1);
    else
        s := 'public';
        n := i_seq_name;
    end if;
    fqname := quote_ident(s) || '.' || quote_ident(n);

    execute 'select last_value from ' || fqname into res;
    return res;
end;
$$ language plpgsql strict;

create or replace function pgq.seq_setval(i_seq_name text, i_new_value int8)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.seq_setval(2)
--
--      Like setval() but does not allow going back.
--
-- Parameters:
--      i_seq_name      - Name of the sequence
--      i_new_value     - new value
--
-- Returns:
--      current last value.
-- ----------------------------------------------------------------------
declare
    res     int8;
    fqname  text;
begin
    fqname := pgq.quote_fqname(i_seq_name);

    res := pgq.seq_getval(i_seq_name);
    if res < i_new_value then
        perform setval(fqname, i_new_value);
        return i_new_value;
    end if;
    return res;
end;
$$ language plpgsql strict;




create or replace function pgq.quote_fqname(i_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.quote_fqname(1)
--
--      Quete fully-qualified object name for SQL.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_name  - fully qualified object name.
--
-- Returns:
--      Quoted name.
-- ----------------------------------------------------------------------
declare
    res     text;
    pos     integer;
    s       text;
    n       text;
begin
    pos := position('.' in i_name);
    if pos > 0 then
        s := substring(i_name for pos - 1);
        n := substring(i_name from pos + 1);
    else
        s := 'public';
        n := i_name;
    end if;
    return quote_ident(s) || '.' || quote_ident(n);
end;
$$ language plpgsql strict immutable;






-- ----------------------------------------------------------------------
-- Function: pgq.insert_event_raw(11)
--
--      Actual event insertion.  Used also by retry queue maintenance.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_id           - Event ID.  If NULL, will be taken from seq.
--      ev_time         - Event creation time.
--      ev_owner        - Subscription ID when retry event. If NULL, the event is for everybody.
--      ev_retry        - Retry count. NULL for first-time events.
--      ev_type         - user data
--      ev_data         - user data
--      ev_extra1       - user data
--      ev_extra2       - user data
--      ev_extra3       - user data
--      ev_extra4       - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
create or replace function pgq.insert_event_raw(
    queue_name text, ev_id bigint, ev_time timestamptz,
    ev_owner integer, ev_retry integer, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns int8 as $$
declare
    qstate record;
    _qname text;
begin
    _qname := queue_name;
    select q.queue_id,
        pgq.quote_fqname(q.queue_data_pfx || '_' || q.queue_cur_table::text) as cur_table_name,
        nextval(q.queue_event_seq) as next_ev_id,
        q.queue_disable_insert,
        q.queue_per_tx_limit
    from pgq.queue q where q.queue_name = _qname into qstate;

    if ev_id is null then
        ev_id := qstate.next_ev_id;
    end if;

    if qstate.queue_disable_insert then
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Insert into queue disallowed';
        end if;
    end if;

    execute 'insert into ' || qstate.cur_table_name
        || ' (ev_id, ev_time, ev_owner, ev_retry,'
        || ' ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)'
        || 'values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)'
        using ev_id, ev_time, ev_owner, ev_retry,
              ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4;

    return ev_id;
end;
$$ language plpgsql;



-- ----------------------------------------------------------------------
-- Section: Public Functions
-- 
-- The queue is used by a client in the following steps
-- 
-- 1. Register the client (a queue consumer)
-- 
--    pgq.register_consumer(queue_name, consumer_id)
-- 
-- 2. run a loop createing, consuming and closing batches
-- 
--    2a. pgq.get_batch_events(batch_id int8) - returns an int8 batch handle
-- 
--    2b. pgq.get_batch_events(batch_id int8) - returns a set of events for current batch
--    
--         the event structure is :(ev_id int8, ev_time timestamptz, ev_txid int8, ev_retry
--         int4, ev_type text, ev_data text, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
--    
--    2c. if any of the events need to be tagged as failed, use a the function
--    
--         pgq.event_failed(batch_id int8, event_id int8, reason text)
--    
--    2d.  if you want the event to be re-inserted in the main queue afrer N seconds, use
--    
--         pgq.event_retry(batch_id int8, event_id int8, retry_seconds int4)
--    
--    2e. To finish processing and release the batch, use
--    
--         pgq.finish_batch(batch_id int8)
-- 
--         Until this is not done, the consumer will get same batch again.
-- 
--         After calling finish_batch consumer cannot do any operations with events
--         of that batch.  All operations must be done before.
-- 
-- -- ----------------------------------------------------------------------


-- Group: Queue creation


create or replace function pgq.create_queue(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.create_queue(1)
--
--      Creates new queue with given name.
--
-- Returns:
--      0 - queue already exists
--      1 - queue created
-- Calls:
--      pgq.grant_perms(i_queue_name);
--      pgq.ticker(i_queue_name);
--      pgq.tune_storage(i_queue_name);
-- Tables directly manipulated:
--      insert - pgq.queue
--      create - pgq.event_N () inherits (pgq.event_template)
--      create - pgq.event_N_0 .. pgq.event_N_M () inherits (pgq.event_N)
-- ----------------------------------------------------------------------
declare
    tblpfx   text;
    tblname  text;
    idxpfx   text;
    idxname  text;
    sql      text;
    id       integer;
    tick_seq text;
    ev_seq text;
    n_tables integer;
begin
    if i_queue_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if exists
    perform 1 from pgq.queue where queue_name = i_queue_name;
    if found then
        return 0;
    end if;

    -- insert event
    id := nextval('pgq.queue_queue_id_seq');
    tblpfx := 'pgq.event_' || id::text;
    idxpfx := 'event_' || id::text;
    tick_seq := 'pgq.event_' || id::text || '_tick_seq';
    ev_seq := 'pgq.event_' || id::text || '_id_seq';
    insert into pgq.queue (queue_id, queue_name,
            queue_data_pfx, queue_event_seq, queue_tick_seq)
        values (id, i_queue_name, tblpfx, ev_seq, tick_seq);

    select queue_ntables into n_tables from pgq.queue
        where queue_id = id;

    -- create seqs
    execute 'CREATE SEQUENCE ' || pgq.quote_fqname(tick_seq);
    execute 'CREATE SEQUENCE ' || pgq.quote_fqname(ev_seq);

    -- create data tables
    execute 'CREATE TABLE ' || pgq.quote_fqname(tblpfx) || ' () '
            || ' INHERITS (pgq.event_template)';
    for i in 0 .. (n_tables - 1) loop
        tblname := tblpfx || '_' || i::text;
        idxname := idxpfx || '_' || i::text || '_txid_idx';
        execute 'CREATE TABLE ' || pgq.quote_fqname(tblname) || ' () '
                || ' INHERITS (' || pgq.quote_fqname(tblpfx) || ')';
        execute 'ALTER TABLE ' || pgq.quote_fqname(tblname) || ' ALTER COLUMN ev_id '
                || ' SET DEFAULT nextval(' || quote_literal(ev_seq) || ')';
        execute 'create index ' || quote_ident(idxname) || ' on '
                || pgq.quote_fqname(tblname) || ' (ev_txid)';
    end loop;

    perform pgq.grant_perms(i_queue_name);

    perform pgq.ticker(i_queue_name);

    perform pgq.tune_storage(i_queue_name);

    return 1;
end;
$$ language plpgsql security definer;



create or replace function pgq.drop_queue(x_queue_name text, x_force bool)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.drop_queue(2)
--
--     Drop queue and all associated tables.
--
-- Parameters:
--      x_queue_name    - queue name
--      x_force         - ignore (drop) existing consumers
-- Returns:
--      1 - success
-- Calls:
--      pgq.unregister_consumer(queue_name, consumer_name)
--      perform pgq.ticker(i_queue_name);
--      perform pgq.tune_storage(i_queue_name);
-- Tables directly manipulated:
--      delete - pgq.queue
--      drop - pgq.event_N (), pgq.event_N_0 .. pgq.event_N_M 
-- ----------------------------------------------------------------------
declare
    tblname  text;
    q record;
    num integer;
begin
    -- check if exists
    select * into q from pgq.queue
        where queue_name = x_queue_name
        for update;
    if not found then
        raise exception 'No such event queue';
    end if;

    if x_force then
        perform pgq.unregister_consumer(queue_name, consumer_name)
           from pgq.get_consumer_info(x_queue_name);
    else
        -- check if no consumers
        select count(*) into num from pgq.subscription
            where sub_queue = q.queue_id;
        if num > 0 then
            raise exception 'cannot drop queue, consumers still attached';
        end if;
    end if;

    -- drop data tables
    for i in 0 .. (q.queue_ntables - 1) loop
        tblname := q.queue_data_pfx || '_' || i::text;
        execute 'DROP TABLE ' || pgq.quote_fqname(tblname);
    end loop;
    execute 'DROP TABLE ' || pgq.quote_fqname(q.queue_data_pfx);

    -- delete ticks
    delete from pgq.tick where tick_queue = q.queue_id;

    -- drop seqs
    -- FIXME: any checks needed here?
    execute 'DROP SEQUENCE ' || pgq.quote_fqname(q.queue_tick_seq);
    execute 'DROP SEQUENCE ' || pgq.quote_fqname(q.queue_event_seq);

    -- delete event
    delete from pgq.queue
        where queue_name = x_queue_name;

    return 1;
end;
$$ language plpgsql security definer;

create or replace function pgq.drop_queue(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.drop_queue(1)
--
--     Drop queue and all associated tables.
--     No consumers must be listening on the queue.
--
-- ----------------------------------------------------------------------
begin
    return pgq.drop_queue(x_queue_name, false);
end;
$$ language plpgsql strict;




create or replace function pgq.set_queue_config(
    x_queue_name    text,
    x_param_name    text,
    x_param_value   text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.set_queue_config(3)
--
--
--     Set configuration for specified queue.
--
-- Parameters:
--      x_queue_name    - Name of the queue to configure.
--      x_param_name    - Configuration parameter name.
--      x_param_value   - Configuration parameter value.
--  
-- Returns:
--     0 if event was already in queue, 1 otherwise.
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgq.queue
-- ----------------------------------------------------------------------
declare
    v_param_name    text;
begin
    -- discard NULL input
    if x_queue_name is null or x_param_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if queue exists
    perform 1 from pgq.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'No such event queue';
    end if;

    -- check if valid parameter name
    v_param_name := 'queue_' || x_param_name;
    if v_param_name not in (
        'queue_ticker_max_count',
        'queue_ticker_max_lag',
        'queue_ticker_idle_period',
        'queue_ticker_paused',
        'queue_rotation_period',
        'queue_external_ticker')
    then
        raise exception 'cannot change parameter "%s"', x_param_name;
    end if;

    execute 'update pgq.queue set ' 
        || v_param_name || ' = ' || quote_literal(x_param_value)
        || ' where queue_name = ' || quote_literal(x_queue_name);

    return 1;
end;
$$ language plpgsql security definer;



-- Group: Event publishing


create or replace function pgq.insert_event(queue_name text, ev_type text, ev_data text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event(3)
--
--      Insert a event into queue.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--
-- Returns:
--      Event ID
-- Calls:
--      pgq.insert_event(7)
-- ----------------------------------------------------------------------
begin
    return pgq.insert_event(queue_name, ev_type, ev_data, null, null, null, null);
end;
$$ language plpgsql;



create or replace function pgq.insert_event(
    queue_name text, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event(7)
--
--      Insert a event into queue with all the extra fields.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--      ev_extra1       - Extra data field for the event
--      ev_extra2       - Extra data field for the event
--      ev_extra3       - Extra data field for the event
--      ev_extra4       - Extra data field for the event
--
-- Returns:
--      Event ID
-- Calls:
--      pgq.insert_event_raw(11)
-- Tables directly manipulated:
--      insert - pgq.insert_event_raw(11), a C function, inserts into current event_N_M table
-- ----------------------------------------------------------------------
begin
    return pgq.insert_event_raw(queue_name, null, now(), null, null,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
end;
$$ language plpgsql security definer;



create or replace function pgq.current_event_table(x_queue_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.current_event_table(1)
--
--      Return active event table for particular queue.
--      Event can be added to it without going via functions,
--      e.g. by COPY.
--
--      If queue is disabled and GUC session_replication_role <> 'replica'
--      then raises exception.
--
--      or expressed in a different way - an even table of a disabled queue
--      is returned only on replica
--
-- Note:
--      The result is valid only during current transaction.
--
-- Permissions:
--      Actual insertion requires superuser access.
--
-- Parameters:
--      x_queue_name    - Queue name.
-- ----------------------------------------------------------------------
declare
    res text;
    disabled boolean;
begin
    select queue_data_pfx || '_' || queue_cur_table::text,
           queue_disable_insert
        into res, disabled
        from pgq.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not found';
    end if;
    if disabled then
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Writing to queue disabled';
        end if;
    end if;
    return res;
end;
$$ language plpgsql; -- no perms needed



-- Group: Subscribing to queue


create or replace function pgq.register_consumer(
    x_queue_name text,
    x_consumer_id text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.register_consumer(2)
--
--      Subscribe consumer on a queue.
--
--      From this moment forward, consumer will see all events in the queue.
--
-- Parameters:
--      x_queue_name        - Name of queue
--      x_consumer_name     - Name of consumer
--
-- Returns:
--      0  - if already registered
--      1  - if new registration
-- Calls:
--      pgq.register_consumer_at(3)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq.register_consumer_at(x_queue_name, x_consumer_id, NULL);
end;
$$ language plpgsql security definer;


create or replace function pgq.register_consumer_at(
    x_queue_name text,
    x_consumer_name text,
    x_tick_pos bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.register_consumer_at(3)
--
--      Extended registration, allows to specify tick_id.
--
-- Note:
--      For usage in special situations.
--
-- Parameters:
--      x_queue_name        - Name of a queue
--      x_consumer_name     - Name of consumer
--      x_tick_pos          - Tick ID
--
-- Returns:
--      0/1 whether consumer has already registered.
-- Calls:
--      None
-- Tables directly manipulated:
--      update/insert - pgq.subscription
-- ----------------------------------------------------------------------
declare
    tmp         text;
    last_tick   bigint;
    x_queue_id  integer;
    x_consumer_id integer;
    queue integer;
    sub record;
begin
    select queue_id into x_queue_id from pgq.queue
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not created yet';
    end if;

    -- get consumer and create if new
    select co_id into x_consumer_id from pgq.consumer
        where co_name = x_consumer_name
        for update;
    if not found then
        insert into pgq.consumer (co_name) values (x_consumer_name);
        x_consumer_id := currval('pgq.consumer_co_id_seq');
    end if;

    -- if particular tick was requested, check if it exists
    if x_tick_pos is not null then
        perform 1 from pgq.tick
            where tick_queue = x_queue_id
              and tick_id = x_tick_pos;
        if not found then
            raise exception 'cannot reposition, tick not found: %', x_tick_pos;
        end if;
    end if;

    -- check if already registered
    select sub_last_tick, sub_batch into sub
        from pgq.subscription
        where sub_consumer = x_consumer_id
          and sub_queue  = x_queue_id;
    if found then
        if x_tick_pos is not null then
            -- if requested, update tick pos and drop partial batch
            update pgq.subscription
                set sub_last_tick = x_tick_pos,
                    sub_batch = null,
                    sub_next_tick = null,
                    sub_active = now()
                where sub_consumer = x_consumer_id
                  and sub_queue = x_queue_id;
        end if;
        -- already registered
        return 0;
    end if;

    --  new registration
    if x_tick_pos is null then
        -- start from current tick
        select tick_id into last_tick from pgq.tick
            where tick_queue = x_queue_id
            order by tick_queue desc, tick_id desc
            limit 1;
        if not found then
            raise exception 'No ticks for this queue.  Please run ticker on database.';
        end if;
    else
        last_tick := x_tick_pos;
    end if;

    -- register
    insert into pgq.subscription (sub_queue, sub_consumer, sub_last_tick)
        values (x_queue_id, x_consumer_id, last_tick);
    return 1;
end;
$$ language plpgsql security definer;





create or replace function pgq.unregister_consumer(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.unregister_consumer(2)
--
--      Unsubscribe consumer from the queue.
--      Also consumer's retry events are deleted.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      number of (sub)consumers unregistered
-- Calls:
--      None
-- Tables directly manipulated:
--      delete - pgq.retry_queue
--      delete - pgq.subscription
-- ----------------------------------------------------------------------
declare
    x_sub_id integer;
    _sub_id_cnt integer;
    _consumer_id integer;
    _is_subconsumer boolean;
begin
    select s.sub_id, c.co_id,
           -- subconsumers can only have both null or both not null - main consumer for subconsumers has only one not null
           (s.sub_last_tick IS NULL AND s.sub_next_tick IS NULL) OR (s.sub_last_tick IS NOT NULL AND s.sub_next_tick IS NOT NULL)
      into x_sub_id, _consumer_id, _is_subconsumer
      from pgq.subscription s, pgq.consumer c, pgq.queue q
     where s.sub_queue = q.queue_id
       and s.sub_consumer = c.co_id
       and q.queue_name = x_queue_name
       and c.co_name = x_consumer_name
       for update of s, c;
    if not found then
        return 0;
    end if;

    -- consumer + subconsumer count
    select count(*) into _sub_id_cnt
        from pgq.subscription
       where sub_id = x_sub_id;

    -- delete only one subconsumer
    if _sub_id_cnt > 1 and _is_subconsumer then
        delete from pgq.subscription
              where sub_id = x_sub_id
                and sub_consumer = _consumer_id;
        return 1;
    else
        -- delete main consumer (including possible subconsumers)

        -- retry events
        delete from pgq.retry_queue
            where ev_owner = x_sub_id;

        -- this will drop subconsumers too
        delete from pgq.subscription
            where sub_id = x_sub_id;

        perform 1 from pgq.subscription
            where sub_consumer = _consumer_id;
        if not found then
            delete from pgq.consumer
                where co_id = _consumer_id;
        end if;

        return _sub_id_cnt;
    end if;

end;
$$ language plpgsql security definer;



-- Group: Batch processing


create or replace function pgq.next_batch_info(
    in i_queue_name text,
    in i_consumer_name text,
    out batch_id int8,
    out cur_tick_id int8,
    out prev_tick_id int8,
    out cur_tick_time timestamptz,
    out prev_tick_time timestamptz,
    out cur_tick_event_seq int8,
    out prev_tick_event_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq.next_batch_info(2)
--
--      Makes next block of events active.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep then.
--
--      The values from event_id sequence may give hint how big the
--      batch may be.  But they are inexact, they do not give exact size.
--      Client *MUST NOT* use them to detect whether the batch contains any
--      events at all - the values are unfit for that purpose.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--
-- Returns:
--      batch_id            - Batch ID or NULL if there are no more events available.
--      cur_tick_id         - End tick id.
--      cur_tick_time       - End tick time.
--      cur_tick_event_seq  - Value from event id sequence at the time tick was issued.
--      prev_tick_id        - Start tick id.
--      prev_tick_time      - Start tick time.
--      prev_tick_event_seq - value from event id sequence at the time tick was issued.
-- Calls:
--      pgq.next_batch_custom(5)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    select f.batch_id, f.cur_tick_id, f.prev_tick_id,
           f.cur_tick_time, f.prev_tick_time,
           f.cur_tick_event_seq, f.prev_tick_event_seq
        into batch_id, cur_tick_id, prev_tick_id, cur_tick_time, prev_tick_time,
             cur_tick_event_seq, prev_tick_event_seq
        from pgq.next_batch_custom(i_queue_name, i_consumer_name, NULL, NULL, NULL) f;
    return;
end;
$$ language plpgsql;

create or replace function pgq.next_batch(
    in i_queue_name text,
    in i_consumer_name text)
returns int8 as $$
-- ----------------------------------------------------------------------
-- Function: pgq.next_batch(2)
--
--      Old function that returns just batch_id.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--
-- Returns:
--      Batch ID or NULL if there are no more events available.
-- ----------------------------------------------------------------------
declare
    res int8;
begin
    select batch_id into res
        from pgq.next_batch_info(i_queue_name, i_consumer_name);
    return res;
end;
$$ language plpgsql;

create or replace function pgq.next_batch_custom(
    in i_queue_name text,
    in i_consumer_name text,
    in i_min_lag interval,
    in i_min_count int4,
    in i_min_interval interval,
    out batch_id int8,
    out cur_tick_id int8,
    out prev_tick_id int8,
    out cur_tick_time timestamptz,
    out prev_tick_time timestamptz,
    out cur_tick_event_seq int8,
    out prev_tick_event_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq.next_batch_custom(5)
--
--      Makes next block of events active.  Block size can be tuned
--      with i_min_count, i_min_interval parameters.  Events age can
--      be tuned with i_min_lag.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep then.
--
--      The values from event_id sequence may give hint how big the
--      batch may be.  But they are inexact, they do not give exact size.
--      Client *MUST NOT* use them to detect whether the batch contains any
--      events at all - the values are unfit for that purpose.
--
-- Note:
--      i_min_lag together with i_min_interval/i_min_count is inefficient.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--      i_min_lag           - Consumer wants events older than that
--      i_min_count         - Consumer wants batch to contain at least this many events
--      i_min_interval      - Consumer wants batch to cover at least this much time
--
-- Returns:
--      batch_id            - Batch ID or NULL if there are no more events available.
--      cur_tick_id         - End tick id.
--      cur_tick_time       - End tick time.
--      cur_tick_event_seq  - Value from event id sequence at the time tick was issued.
--      prev_tick_id        - Start tick id.
--      prev_tick_time      - Start tick time.
--      prev_tick_event_seq - value from event id sequence at the time tick was issued.
-- Calls:
--      pgq.insert_event_raw(11)
-- Tables directly manipulated:
--      update - pgq.subscription
-- ----------------------------------------------------------------------
declare
    errmsg          text;
    queue_id        integer;
    sub_id          integer;
    cons_id         integer;
begin
    select s.sub_queue, s.sub_consumer, s.sub_id, s.sub_batch,
            t1.tick_id, t1.tick_time, t1.tick_event_seq,
            t2.tick_id, t2.tick_time, t2.tick_event_seq
        into queue_id, cons_id, sub_id, batch_id,
             prev_tick_id, prev_tick_time, prev_tick_event_seq,
             cur_tick_id, cur_tick_time, cur_tick_event_seq
        from pgq.consumer c,
             pgq.queue q,
             pgq.subscription s
             left join pgq.tick t1
                on (t1.tick_queue = s.sub_queue
                    and t1.tick_id = s.sub_last_tick)
             left join pgq.tick t2
                on (t2.tick_queue = s.sub_queue
                    and t2.tick_id = s.sub_next_tick)
        where q.queue_name = i_queue_name
          and c.co_name = i_consumer_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id;
    if not found then
        errmsg := 'Not subscriber to queue: '
            || coalesce(i_queue_name, 'NULL')
            || '/'
            || coalesce(i_consumer_name, 'NULL');
        raise exception '%', errmsg;
    end if;

    -- sanity check
    if prev_tick_id is null then
        raise exception 'PgQ corruption: Consumer % on queue % does not see tick %', i_consumer_name, i_queue_name, prev_tick_id;
    end if;

    -- has already active batch
    if batch_id is not null then
        return;
    end if;

    if i_min_interval is null and i_min_count is null then
        -- find next tick
        select tick_id, tick_time, tick_event_seq
            into cur_tick_id, cur_tick_time, cur_tick_event_seq
            from pgq.tick
            where tick_id > prev_tick_id
              and tick_queue = queue_id
            order by tick_queue asc, tick_id asc
            limit 1;
    else
        -- find custom tick
        select next_tick_id, next_tick_time, next_tick_seq
          into cur_tick_id, cur_tick_time, cur_tick_event_seq
          from pgq.find_tick_helper(queue_id, prev_tick_id,
                                    prev_tick_time, prev_tick_event_seq,
                                    i_min_count, i_min_interval);
    end if;

    if i_min_lag is not null then
        -- enforce min lag
        if now() - cur_tick_time < i_min_lag then
            cur_tick_id := NULL;
            cur_tick_time := NULL;
            cur_tick_event_seq := NULL;
        end if;
    end if;

    if cur_tick_id is null then
        -- nothing to do
        prev_tick_id := null;
        prev_tick_time := null;
        prev_tick_event_seq := null;
        return;
    end if;

    -- get next batch
    batch_id := nextval('pgq.batch_id_seq');
    update pgq.subscription
        set sub_batch = batch_id,
            sub_next_tick = cur_tick_id,
            sub_active = now()
        where sub_queue = queue_id
          and sub_consumer = cons_id;
    return;
end;
$$ language plpgsql security definer;



create or replace function pgq.get_batch_events(
    in x_batch_id   bigint,
    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_events(1)
--
--      Get all events in batch.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--
-- Returns:
--      List of events.
-- ----------------------------------------------------------------------
declare
    sql text;
begin
    sql := pgq.batch_event_sql(x_batch_id);
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
        in execute sql
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql; -- no perms needed





create or replace function pgq.get_batch_cursor(
    in i_batch_id       bigint,
    in i_cursor_name    text,
    in i_quick_limit    int4,
    in i_extra_where    text,

    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_cursor(4)
--
--      Get events in batch using a cursor.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_cursor_name   - Name for new cursor
--      i_quick_limit   - Number of events to return immediately
--      i_extra_where   - optional where clause to filter events
--
-- Returns:
--      List of events.
-- Calls:
--      pgq.batch_event_sql(i_batch_id) - internal function which generates SQL optimised specially for getting events in this batch
-- ----------------------------------------------------------------------
declare
    _cname  text;
    _sql    text;
begin
    if i_batch_id is null or i_cursor_name is null or i_quick_limit is null then
        return;
    end if;

    _cname := quote_ident(i_cursor_name);
    _sql := pgq.batch_event_sql(i_batch_id);

    -- apply extra where
    if i_extra_where is not null then
        _sql := replace(_sql, ' order by 1', '');
        _sql := 'select * from (' || _sql
            || ') _evs where ' || i_extra_where
            || ' order by 1';
    end if;

    -- create cursor
    execute 'declare ' || _cname || ' no scroll cursor for ' || _sql;

    -- if no events wanted, don't bother with execute
    if i_quick_limit <= 0 then
        return;
    end if;

    -- return first block of events
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
        in execute 'fetch ' || i_quick_limit::text || ' from ' || _cname
    loop
        return next;
    end loop;

    return;
end;
$$ language plpgsql; -- no perms needed

create or replace function pgq.get_batch_cursor(
    in i_batch_id       bigint,
    in i_cursor_name    text,
    in i_quick_limit    int4,

    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_cursor(3)
--
--      Get events in batch using a cursor.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_cursor_name   - Name for new cursor
--      i_quick_limit   - Number of events to return immediately
--
-- Returns:
--      List of events.
-- Calls:
--      pgq.get_batch_cursor(4)
-- ----------------------------------------------------------------------
begin
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
    in
        select * from pgq.get_batch_cursor(i_batch_id,
            i_cursor_name, i_quick_limit, null)
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql strict; -- no perms needed



create or replace function pgq.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_time timestamptz)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry(3a)
--
--     Put the event into retry queue, to be processed again later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_time    - Time when the event should be put back into queue
--
-- Returns:
--     1 - success
--     0 - event already in retry queue
-- Calls:
--      None
-- Tables directly manipulated:
--      insert - pgq.retry_queue
-- ----------------------------------------------------------------------
begin
    insert into pgq.retry_queue (ev_retry_after, ev_queue,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_retry_time, sub_queue,
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0) + 1,
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgq.get_batch_events(x_batch_id),
           pgq.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer;


create or replace function pgq.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_seconds integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry(3b)
--
--     Put the event into retry queue, to be processed later again.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_seconds - Time when the event should be put back into queue
--
-- Returns:
--     1 - success
--     0 - event already in retry queue
-- Calls:
--      pgq.event_retry(3a)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
declare
    new_retry  timestamptz;
begin
    new_retry := current_timestamp + ((x_retry_seconds::text || ' seconds')::interval);
    return pgq.event_retry(x_batch_id, x_event_id, new_retry);
end;
$$ language plpgsql security definer;



create or replace function pgq.batch_retry(
    i_batch_id bigint,
    i_retry_seconds integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.batch_retry(2)
--
--     Put whole batch into retry queue, to be processed again later.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_retry_time    - Time when the event should be put back into queue
--
-- Returns:
--     number of events inserted
-- Calls:
--      None
-- Tables directly manipulated:
--      pgq.retry_queue
-- ----------------------------------------------------------------------
declare
    _retry timestamptz;
    _cnt   integer;
    _s     record;
begin
    _retry := current_timestamp + ((i_retry_seconds::text || ' seconds')::interval);

    select * into _s from pgq.subscription where sub_batch = i_batch_id;
    if not found then
        raise exception 'batch_retry: batch % not found', i_batch_id;
    end if;

    insert into pgq.retry_queue (ev_retry_after, ev_queue,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry,
        ev_type, ev_data, ev_extra1, ev_extra2,
        ev_extra3, ev_extra4)
    select distinct _retry, _s.sub_queue,
           b.ev_id, b.ev_time, NULL::int8, _s.sub_id, coalesce(b.ev_retry, 0) + 1,
           b.ev_type, b.ev_data, b.ev_extra1, b.ev_extra2,
           b.ev_extra3, b.ev_extra4
      from pgq.get_batch_events(i_batch_id) b
           left join pgq.retry_queue rq
                  on (rq.ev_id = b.ev_id
                      and rq.ev_owner = _s.sub_id
                      and rq.ev_queue = _s.sub_queue)
      where rq.ev_id is null;

    GET DIAGNOSTICS _cnt = ROW_COUNT;
    return _cnt;
end;
$$ language plpgsql security definer;





create or replace function pgq.finish_batch(
    x_batch_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.finish_batch(1)
--
--      Closes a batch.  No more operations can be done with events
--      of this batch.
--
-- Parameters:
--      x_batch_id      - id of batch.
--
-- Returns:
--      1 if batch was found, 0 otherwise.
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgq.subscription
-- ----------------------------------------------------------------------
begin
    update pgq.subscription
        set sub_active = now(),
            sub_last_tick = sub_next_tick,
            sub_next_tick = null,
            sub_batch = null
        where sub_batch = x_batch_id;
    if not found then
        raise warning 'finish_batch: batch % not found', x_batch_id;
        return 0;
    end if;

    return 1;
end;
$$ language plpgsql security definer;



-- Group: General info functions


drop function if exists pgq.get_queue_info();
drop function if exists pgq.get_queue_info(text);

create or replace function pgq.get_queue_info(
    out queue_name                  text,
    out queue_ntables               integer,
    out queue_cur_table             integer,
    out queue_rotation_period       interval,
    out queue_switch_time           timestamptz,
    out queue_external_ticker       boolean,
    out queue_ticker_paused         boolean,
    out queue_ticker_max_count      integer,
    out queue_ticker_max_lag        interval,
    out queue_ticker_idle_period    interval,
    out ticker_lag                  interval,
    out ev_per_sec                  float8,
    out ev_new                      bigint,
    out last_tick_id                bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(0)
--
--      Get info about all queues.
--
-- Returns:
--      List of pgq.ret_queue_info records.
--     queue_name                  - queue name
--     queue_ntables               - number of tables in this queue
--     queue_cur_table             - ???
--     queue_rotation_period       - how often the event_N_M tables in this queue are rotated
--     queue_switch_time           - ??? when was this queue last rotated
--     queue_external_ticker       - ???
--     queue_ticker_paused         - ??? is ticker paused in this queue
--     queue_ticker_max_count      - max number of events before a tick is issued
--     queue_ticker_max_lag        - maks time without a tick
--     queue_ticker_idle_period    - how often the ticker should check this queue
--     ticker_lag                  - time from last tick
--     ev_per_sec                  - how many events per second this queue serves
--     ev_new                      - ???
--     last_tick_id                - last tick id for this queue
--
-- ----------------------------------------------------------------------
begin
    for queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
        queue_switch_time, queue_external_ticker, queue_ticker_paused,
        queue_ticker_max_count, queue_ticker_max_lag, queue_ticker_idle_period,
        ticker_lag, ev_per_sec, ev_new, last_tick_id
    in select
        f.queue_name, f.queue_ntables, f.queue_cur_table, f.queue_rotation_period,
        f.queue_switch_time, f.queue_external_ticker, f.queue_ticker_paused,
        f.queue_ticker_max_count, f.queue_ticker_max_lag, f.queue_ticker_idle_period,
        f.ticker_lag, f.ev_per_sec, f.ev_new, f.last_tick_id
        from pgq.get_queue_info(null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql;

create or replace function pgq.get_queue_info(
    in i_queue_name                 text,
    out queue_name                  text,
    out queue_ntables               integer,
    out queue_cur_table             integer,
    out queue_rotation_period       interval,
    out queue_switch_time           timestamptz,
    out queue_external_ticker       boolean,
    out queue_ticker_paused         boolean,
    out queue_ticker_max_count      integer,
    out queue_ticker_max_lag        interval,
    out queue_ticker_idle_period    interval,
    out ticker_lag                  interval,
    out ev_per_sec                  float8,
    out ev_new                      bigint,
    out last_tick_id                bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(1)
--
--      Get info about particular queue.
--
-- Returns:
--      One pgq.ret_queue_info record.
--      contente same as forpgq.get_queue_info() 
-- ----------------------------------------------------------------------
declare
    _ticker_lag interval;
    _top_tick_id bigint;
    _ht_tick_id bigint;
    _top_tick_time timestamptz;
    _top_tick_event_seq bigint;
    _ht_tick_time timestamptz;
    _ht_tick_event_seq bigint;
    _queue_id integer;
    _queue_event_seq text;
begin
    for queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
        queue_switch_time, queue_external_ticker, queue_ticker_paused,
        queue_ticker_max_count, queue_ticker_max_lag, queue_ticker_idle_period,
        _queue_id, _queue_event_seq
    in select
        q.queue_name, q.queue_ntables, q.queue_cur_table,
        q.queue_rotation_period, q.queue_switch_time,
        q.queue_external_ticker, q.queue_ticker_paused,
        q.queue_ticker_max_count, q.queue_ticker_max_lag,
        q.queue_ticker_idle_period,
        q.queue_id, q.queue_event_seq
        from pgq.queue q
        where (i_queue_name is null or q.queue_name = i_queue_name)
        order by q.queue_name
    loop
        -- most recent tick
        select (current_timestamp - t.tick_time),
               tick_id, t.tick_time, t.tick_event_seq
            into ticker_lag, _top_tick_id, _top_tick_time, _top_tick_event_seq
            from pgq.tick t
            where t.tick_queue = _queue_id
            order by t.tick_queue desc, t.tick_id desc
            limit 1;
        -- slightly older tick
        select ht.tick_id, ht.tick_time, ht.tick_event_seq
            into _ht_tick_id, _ht_tick_time, _ht_tick_event_seq
            from pgq.tick ht
            where ht.tick_queue = _queue_id
             and ht.tick_id >= _top_tick_id - 20
            order by ht.tick_queue asc, ht.tick_id asc
            limit 1;
        if _ht_tick_time < _top_tick_time then
            ev_per_sec = (_top_tick_event_seq - _ht_tick_event_seq) / extract(epoch from (_top_tick_time - _ht_tick_time));
        else
            ev_per_sec = null;
        end if;
        ev_new = pgq.seq_getval(_queue_event_seq) - _top_tick_event_seq;
        last_tick_id = _top_tick_id;
        return next;
    end loop;
    return;
end;
$$ language plpgsql;




create or replace function pgq.get_consumer_info(
    out queue_name      text,
    out consumer_name   text,
    out lag             interval,
    out last_seen       interval,
    out last_tick       bigint,
    out current_batch   bigint,
    out next_tick       bigint,
    out pending_events  bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_consumer_info(0)
--
--      Returns info about all consumers on all queues.
--
-- Returns:
--      See pgq.get_consumer_info(2)
-- ----------------------------------------------------------------------
begin
    for queue_name, consumer_name, lag, last_seen,
        last_tick, current_batch, next_tick, pending_events
    in
        select f.queue_name, f.consumer_name, f.lag, f.last_seen,
               f.last_tick, f.current_batch, f.next_tick, f.pending_events
            from pgq.get_consumer_info(null, null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;



create or replace function pgq.get_consumer_info(
    in i_queue_name     text,
    out queue_name      text,
    out consumer_name   text,
    out lag             interval,
    out last_seen       interval,
    out last_tick       bigint,
    out current_batch   bigint,
    out next_tick       bigint,
    out pending_events  bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_consumer_info(1)
--
--      Returns info about all consumers on single queue.
--
-- Returns:
--      See pgq.get_consumer_info(2)
-- ----------------------------------------------------------------------
begin
    for queue_name, consumer_name, lag, last_seen,
        last_tick, current_batch, next_tick, pending_events
    in
        select f.queue_name, f.consumer_name, f.lag, f.last_seen,
               f.last_tick, f.current_batch, f.next_tick, f.pending_events
            from pgq.get_consumer_info(i_queue_name, null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;



create or replace function pgq.get_consumer_info(
    in i_queue_name     text,
    in i_consumer_name  text,
    out queue_name      text,
    out consumer_name   text,
    out lag             interval,
    out last_seen       interval,
    out last_tick       bigint,
    out current_batch   bigint,
    out next_tick       bigint,
    out pending_events  bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_consumer_info(2)
--
--      Get info about particular consumer on particular queue.
--
-- Parameters:
--      i_queue_name        - name of a queue. (null = all)
--      i_consumer_name     - name of a consumer (null = all)
--
-- Returns:
--      queue_name          - Queue name
--      consumer_name       - Consumer name
--      lag                 - How old are events the consumer is processing
--      last_seen           - When the consumer seen by pgq
--      last_tick           - Tick ID of last processed tick
--      current_batch       - Current batch ID, if one is active or NULL
--      next_tick           - If batch is active, then its final tick.
-- ----------------------------------------------------------------------
declare
    _pending_events bigint;
    _queue_id bigint;
begin
    for queue_name, consumer_name, lag, last_seen,
        last_tick, current_batch, next_tick, _pending_events, _queue_id
    in
        select q.queue_name, c.co_name,
               current_timestamp - t.tick_time,
               current_timestamp - s.sub_active,
               s.sub_last_tick, s.sub_batch, s.sub_next_tick,
               t.tick_event_seq, q.queue_id
          from pgq.queue q,
               pgq.consumer c,
               pgq.subscription s
               left join pgq.tick t
                 on (t.tick_queue = s.sub_queue and t.tick_id = s.sub_last_tick)
         where q.queue_id = s.sub_queue
           and c.co_id = s.sub_consumer
           and (i_queue_name is null or q.queue_name = i_queue_name)
           and (i_consumer_name is null or c.co_name = i_consumer_name)
         order by 1,2
    loop
        select t.tick_event_seq - _pending_events
            into pending_events
            from pgq.tick t
            where t.tick_queue = _queue_id
            order by t.tick_queue desc, t.tick_id desc
            limit 1;
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;



create or replace function pgq.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.version(0)
--
--      Returns version string for pgq.
-- ----------------------------------------------------------------------
declare
    _vers text;
begin
    select extversion from pg_catalog.pg_extension
        where extname = 'pgq' into _vers;
    return _vers;
end;
$$ language plpgsql;




create or replace function pgq.get_batch_info(
    in x_batch_id       bigint,
    out queue_name      text,
    out consumer_name   text,
    out batch_start     timestamptz,
    out batch_end       timestamptz,
    out prev_tick_id    bigint,
    out tick_id         bigint,
    out lag             interval,
    out seq_start       bigint,
    out seq_end         bigint)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_info(1)
--
--      Returns detailed info about a batch.
--
-- Parameters:
--      x_batch_id      - id of a active batch.
--
-- Returns: ??? pls check
--      queue_name      - which queue this batch came from
--      consumer_name   - batch processed by
--      batch_start     - start time of batch
--      batch_end       - end time of batch
--      prev_tick_id    - start tick for this batch
--      tick_id         - end tick for this batch
--      lag             - now() - tick_id.time 
--      seq_start       - start event id for batch
--      seq_end         - end event id for batch
-- ----------------------------------------------------------------------
begin
    select q.queue_name, c.co_name,
           prev.tick_time, cur.tick_time,
           s.sub_last_tick, s.sub_next_tick,
           current_timestamp - cur.tick_time,
           prev.tick_event_seq, cur.tick_event_seq
        into queue_name, consumer_name, batch_start, batch_end,
             prev_tick_id, tick_id, lag, seq_start, seq_end
        from pgq.subscription s, pgq.tick cur, pgq.tick prev,
             pgq.queue q, pgq.consumer c
        where s.sub_batch = x_batch_id
          and prev.tick_id = s.sub_last_tick
          and prev.tick_queue = s.sub_queue
          and cur.tick_id = s.sub_next_tick
          and cur.tick_queue = s.sub_queue
          and q.queue_id = s.sub_queue
          and c.co_id = s.sub_consumer;
    return;
end;
$$ language plpgsql security definer;






-- Section: Public Triggers

-- Group: Trigger Functions


create or replace function pgq.jsontriga() returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: pgq.jsontriga()
--
--      Trigger function that puts row data in JSON-encoded form into queue.
--
-- Purpose:
--      Convert row data into easily parseable form.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optional arguments:
--      SKIP                - The actual operation should be skipped (BEFORE trigger)
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--      colname=EXPR        - Override field value with SQL expression.  Can reference table
--                            columns.  colname can be: ev_type, ev_data, ev_extra1 .. ev_extra4
--      when=EXPR           - If EXPR returns false, don't insert event.
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.jsontriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.jsontriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
declare
    qname text;
    ev_type text;
    ev_data text;
    ev_extra1 text;
    ev_extra2 text;
    ev_extra3 text;
    ev_extra4 text;
    do_skip boolean := false;
    do_backup boolean := false;
    do_insert boolean := true;
    do_deny boolean := false;
    extra_ignore_list text[];
    full_ignore_list text[];
    ignore_list text[] := '{}';
    pkey_list text[];
    pkey_str text;
    field_sql_sfx text;
    field_sql text[] := '{}';
    data_sql text;
    ignore_col_changes int4 := 0;
begin
    if TG_NARGS < 1 then
        raise exception 'Trigger needs queue name';
    end if;
    qname := TG_ARGV[0];

    -- standard output
    ev_extra1 := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- prepare to handle magic fields
    field_sql_sfx := ')::text as val from (select $1.*) r';
    extra_ignore_list := array['_pgq_ev_type', '_pgq_ev_extra1', '_pgq_ev_extra2',
                               '_pgq_ev_extra3', '_pgq_ev_extra4']::text[];

    -- parse trigger args
    declare
        got boolean;
        argpair text[];
        i integer;
    begin
        for i in 1 .. TG_NARGS-1 loop
            if TG_ARGV[i] in ('skip', 'SKIP') then
                do_skip := true;
            elsif TG_ARGV[i] = 'backup' then
                do_backup := true;
            elsif TG_ARGV[i] = 'deny' then
                do_deny := true;
            else
                got := false;
                for argpair in select regexp_matches(TG_ARGV[i], '^([^=]+)=(.*)') loop
                    got := true;
                    if argpair[1] = 'pkey' then
                        pkey_str := argpair[2];
                        pkey_list := string_to_array(pkey_str, ',');
                    elsif argpair[1] = 'ignore' then
                        ignore_list := string_to_array(argpair[2], ',');
                    elsif argpair[1] ~ '^ev_(type|extra[1-4])$' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (' || argpair[2] || field_sql_sfx);
                    elsif argpair[1] = 'when' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (case when (' || argpair[2]
                                                  || ')::boolean then ''proceed'' else null end' || field_sql_sfx);
                    else
                        got := false;
                    end if;
                end loop;
                if not got then
                    raise exception 'bad argument: %', TG_ARGV[i];
                end if;
            end if;
        end loop;
    end;

    full_ignore_list := ignore_list || extra_ignore_list;

    if pkey_str is null then
        select array_agg(pk.attname)
            from (select k.attname from pg_index i, pg_attribute k
                    where i.indrelid = TG_RELID
                        and k.attrelid = i.indexrelid and i.indisprimary
                        and k.attnum > 0 and not k.attisdropped
                    order by k.attnum) pk
            into pkey_list;
        if pkey_list is null then
            pkey_list := '{}';
            pkey_str := '';
        else
            pkey_str := array_to_string(pkey_list, ',');
        end if;
    end if;
    if pkey_str = '' and TG_OP in ('UPDATE', 'DELETE') then
        raise exception 'Update/Delete on table without pkey';
    end if;

    if TG_OP not in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE') then
        raise exception 'TG_OP not supported: %', TG_OP;
    end if;

    -- fill ev_type
    select to_json(t.*)::text
        from (select TG_OP as op, array[TG_TABLE_SCHEMA,TG_TABLE_NAME] as "table", pkey_list as "pkey") t
        into ev_type;

    -- early exit?
    if current_setting('session_replication_role') = 'local' then
        if TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
            return null;
        elsif TG_OP = 'DELETE' then
            return OLD;
        else
            return NEW;
        end if;
    elsif do_deny then
        raise exception 'Table ''%.%'' to queue ''%'': change not allowed (%)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, qname, TG_OP;
    elsif TG_OP = 'TRUNCATE' then
        perform pgq.insert_event(qname, ev_type, '{}', ev_extra1, ev_extra2, ev_extra3, ev_extra4);
        return null;
    end if;

    -- process table columns
    declare
        attr record;
        pkey_sql_buf text[];
        qcol text;
        data_sql_buf text[];
        ignore_sql text;
        ignore_sql_buf text[];
        pkey_change_sql text;
        pkey_col_changes int4 := 0;
        valexp text;
    begin
        for attr in
            select k.attnum, k.attname, k.atttypid
                from pg_attribute k
                where k.attrelid = TG_RELID and k.attnum > 0 and not k.attisdropped
                order by k.attnum
        loop
            qcol := quote_ident(attr.attname);
            if attr.attname = any (ignore_list) then
                ignore_sql_buf := array_append(ignore_sql_buf,
                    'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                continue;
            elsif attr.attname = any (extra_ignore_list) then
                field_sql := array_prepend('select ' || quote_literal(substring(attr.attname from 6))
                                           || '::text as key, (r.' || qcol || field_sql_sfx, field_sql);
                continue;
            end if;

            -- force cast to text or not
            if attr.atttypid in ('timestamptz'::regtype::oid, 'timestamp'::regtype::oid,
                    'int8'::regtype::oid, 'int4'::regtype::oid, 'int2'::regtype::oid,
                    'date'::regtype::oid, 'boolean'::regtype::oid) then
                valexp := 'to_json(r.' || qcol || ')::text';
            else
                valexp := 'to_json(r.' || qcol || '::text)::text';
            end if;

            if attr.attname = any (pkey_list) then
                pkey_sql_buf := array_append(pkey_sql_buf,
                        'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
            end if;

            data_sql_buf := array_append(data_sql_buf,
                    'select ' || quote_literal(to_json(attr.attname) || ':')
                    || ' || coalesce(' || valexp || ', ''null'') as jpair from (select $1.*) r');
        end loop;

        -- SQL to see if pkey columns have changed
        if TG_OP = 'UPDATE' then
            pkey_change_sql := 'select count(1) from (' || array_to_string(pkey_sql_buf, ' union all ')
                            || ') cols where cols.is_changed';
            execute pkey_change_sql using OLD, NEW into pkey_col_changes;
            if pkey_col_changes > 0 then
                raise exception 'primary key update not allowed';
            end if;
        end if;

        -- SQL to see if ignored columns have changed
        if TG_OP = 'UPDATE' and array_length(ignore_list, 1) is not null then
            ignore_sql := 'select count(1) from (' || array_to_string(ignore_sql_buf, ' union all ')
                || ') cols where cols.is_changed';
            execute ignore_sql using OLD, NEW into ignore_col_changes;
        end if;

        -- SQL to load data
        data_sql := 'select ''{'' || array_to_string(array_agg(cols.jpair), '','') || ''}'' from ('
                 || array_to_string(data_sql_buf, ' union all ') || ') cols';
    end;

    -- render data
    declare
        old_data text;
    begin
        if TG_OP = 'INSERT' then
            execute data_sql using NEW into ev_data;
        elsif TG_OP = 'UPDATE' then

            -- render NEW
            execute data_sql using NEW into ev_data;

            -- render OLD when needed
            if do_backup or array_length(ignore_list, 1) is not null then
                execute data_sql using OLD into old_data;
            end if;

            -- only change was to ignored columns?
            if old_data = ev_data and ignore_col_changes > 0 then
                do_insert := false;
            end if;

            -- is backup needed?
            if do_backup then
                ev_extra2 := old_data;
            end if;
        elsif TG_OP = 'DELETE' then
            execute data_sql using OLD into ev_data;
        end if;
    end;

    -- apply magic args and columns
    declare
        col text;
        val text;
        rmain record;
        sql text;
    begin
        if do_insert and array_length(field_sql, 1) is not null then
            if TG_OP = 'DELETE' then
                rmain := OLD;
            else
                rmain := NEW;
            end if;

            sql := array_to_string(field_sql, ' union all ');
            for col, val in
                execute sql using rmain
            loop
                if col = 'ev_type' then
                    ev_type := val;
                elsif col = 'ev_extra1' then
                    ev_extra1 := val;
                elsif col = 'ev_extra2' then
                    ev_extra2 := val;
                elsif col = 'ev_extra3' then
                    ev_extra3 := val;
                elsif col = 'ev_extra4' then
                    ev_extra4 := val;
                elsif col = 'when' then
                    if val is null then
                        do_insert := false;
                    end if;
                end if;
            end loop;
        end if;
    end;

    -- insert final values
    if do_insert then
        perform pgq.insert_event(qname, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
    end if;

    if do_skip or TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
        return null;
    elsif TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$ language plpgsql;



create or replace function pgq.logutriga() returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: pgq.logutriga()
--
--      Trigger function that puts row data in urlencoded form into queue.
--
-- Purpose:
--	Used as producer for several PgQ standard consumers (cube_dispatcher, 
--      queue_mover, table_dispatcher).  Basically for cases where the
--      consumer wants to parse the event and look at the actual column values.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optional arguments:
--      SKIP                - The actual operation should be skipped (BEFORE trigger)
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--      colname=EXPR        - Override field value with SQL expression.  Can reference table
--                            columns.  colname can be: ev_type, ev_data, ev_extra1 .. ev_extra4
--      when=EXPR           - If EXPR returns false, don't insert event.
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
declare
    qname text;
    ev_type text;
    ev_data text;
    ev_extra1 text;
    ev_extra2 text;
    ev_extra3 text;
    ev_extra4 text;
    do_skip boolean := false;
    do_backup boolean := false;
    do_insert boolean := true;
    do_deny boolean := false;
    extra_ignore_list text[];
    full_ignore_list text[];
    ignore_list text[] := '{}';
    pkey_list text[];
    pkey_str text;
    field_sql_sfx text;
    field_sql text[] := '{}';
    data_sql text;
    ignore_col_changes int4 := 0;
begin
    if TG_NARGS < 1 then
        raise exception 'Trigger needs queue name';
    end if;
    qname := TG_ARGV[0];

    -- standard output
    ev_extra1 := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- prepare to handle magic fields
    field_sql_sfx := ')::text as val from (select $1.*) r';
    extra_ignore_list := array['_pgq_ev_type', '_pgq_ev_extra1', '_pgq_ev_extra2',
                               '_pgq_ev_extra3', '_pgq_ev_extra4']::text[];

    -- parse trigger args
    declare
        got boolean;
        argpair text[];
        i integer;
    begin
        for i in 1 .. TG_NARGS-1 loop
            if TG_ARGV[i] in ('skip', 'SKIP') then
                do_skip := true;
            elsif TG_ARGV[i] = 'backup' then
                do_backup := true;
            elsif TG_ARGV[i] = 'deny' then
                do_deny := true;
            else
                got := false;
                for argpair in select regexp_matches(TG_ARGV[i], '^([^=]+)=(.*)') loop
                    got := true;
                    if argpair[1] = 'pkey' then
                        pkey_str := argpair[2];
                        pkey_list := string_to_array(pkey_str, ',');
                    elsif argpair[1] = 'ignore' then
                        ignore_list := string_to_array(argpair[2], ',');
                    elsif argpair[1] ~ '^ev_(type|extra[1-4])$' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (' || argpair[2] || field_sql_sfx);
                    elsif argpair[1] = 'when' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (case when (' || argpair[2]
                                                  || ')::boolean then ''proceed'' else null end' || field_sql_sfx);
                    else
                        got := false;
                    end if;
                end loop;
                if not got then
                    raise exception 'bad argument: %', TG_ARGV[i];
                end if;
            end if;
        end loop;
    end;

    full_ignore_list := ignore_list || extra_ignore_list;

    if pkey_str is null then
        select array_agg(pk.attname)
            from (select k.attname from pg_index i, pg_attribute k
                    where i.indrelid = TG_RELID
                        and k.attrelid = i.indexrelid and i.indisprimary
                        and k.attnum > 0 and not k.attisdropped
                    order by k.attnum) pk
            into pkey_list;
        if pkey_list is null then
            pkey_list := '{}';
            pkey_str := '';
        else
            pkey_str := array_to_string(pkey_list, ',');
        end if;
    end if;
    if pkey_str = '' and TG_OP in ('UPDATE', 'DELETE') then
        raise exception 'Update/Delete on table without pkey';
    end if;

    if TG_OP = 'INSERT' then
        ev_type := 'I:' || pkey_str;
    elsif TG_OP = 'UPDATE' then
        ev_type := 'U:' || pkey_str;
    elsif TG_OP = 'DELETE' then
        ev_type := 'D:' || pkey_str;
    elsif TG_OP = 'TRUNCATE' then
        ev_type := 'R';
    else
        raise exception 'TG_OP not supported: %', TG_OP;
    end if;

    if current_setting('session_replication_role') = 'local' then
        if TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
            return null;
        elsif TG_OP = 'DELETE' then
            return OLD;
        else
            return NEW;
        end if;
    elsif do_deny then
        raise exception 'Table ''%.%'' to queue ''%'': change not allowed (%)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, qname, TG_OP;
    elsif TG_OP = 'TRUNCATE' then
        perform pgq.insert_event(qname, ev_type, '', ev_extra1, ev_extra2, ev_extra3, ev_extra4);
        return null;
    end if;

    -- process table columns
    declare
        attr record;
        pkey_sql_buf text[];
        qcol text;
        data_sql_buf text[];
        ignore_sql text;
        ignore_sql_buf text[];
        pkey_change_sql text;
        pkey_col_changes int4 := 0;
        valexp text;
    begin
        for attr in
            select k.attnum, k.attname, k.atttypid
                from pg_attribute k
                where k.attrelid = TG_RELID and k.attnum > 0 and not k.attisdropped
                order by k.attnum
        loop
            qcol := quote_ident(attr.attname);
            if attr.attname = any (ignore_list) then
                ignore_sql_buf := array_append(ignore_sql_buf,
                    'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                continue;
            elsif attr.attname = any (extra_ignore_list) then
                field_sql := array_prepend('select ' || quote_literal(substring(attr.attname from 6))
                                           || '::text as key, (r.' || qcol || field_sql_sfx, field_sql);
                continue;
            end if;

            if attr.atttypid = 'boolean'::regtype::oid then
                valexp := 'case r.' || qcol || ' when true then ''t'' when false then ''f'' else null end';
            else
                valexp := 'r.' || qcol || '::text';
            end if;

            if attr.attname = any (pkey_list) then
                pkey_sql_buf := array_append(pkey_sql_buf,
                        'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
            end if;

            data_sql_buf := array_append(data_sql_buf,
                    'select pgq._urlencode(' || quote_literal(attr.attname)
                    || ') || coalesce(''='' || pgq._urlencode(' || valexp
                    || '), '''') as upair from (select $1.*) r');
        end loop;

        -- SQL to see if pkey columns have changed
        if TG_OP = 'UPDATE' then
            pkey_change_sql := 'select count(1) from (' || array_to_string(pkey_sql_buf, ' union all ')
                            || ') cols where cols.is_changed';
            execute pkey_change_sql using OLD, NEW into pkey_col_changes;
            if pkey_col_changes > 0 then
                raise exception 'primary key update not allowed';
            end if;
        end if;

        -- SQL to see if ignored columns have changed
        if TG_OP = 'UPDATE' and array_length(ignore_list, 1) is not null then
            ignore_sql := 'select count(1) from (' || array_to_string(ignore_sql_buf, ' union all ')
                || ') cols where cols.is_changed';
            execute ignore_sql using OLD, NEW into ignore_col_changes;
        end if;

        -- SQL to load data
        data_sql := 'select array_to_string(array_agg(cols.upair), ''&'') from ('
                 || array_to_string(data_sql_buf, ' union all ') || ') cols';
    end;

    -- render data
    declare
        old_data text;
    begin
        if TG_OP = 'INSERT' then
            execute data_sql using NEW into ev_data;
        elsif TG_OP = 'UPDATE' then

            -- render NEW
            execute data_sql using NEW into ev_data;

            -- render OLD when needed
            if do_backup or array_length(ignore_list, 1) is not null then
                execute data_sql using OLD into old_data;
            end if;

            -- only change was to ignored columns?
            if old_data = ev_data and ignore_col_changes > 0 then
                do_insert := false;
            end if;

            -- is backup needed?
            if do_backup then
                ev_extra2 := old_data;
            end if;
        elsif TG_OP = 'DELETE' then
            execute data_sql using OLD into ev_data;
        end if;
    end;

    -- apply magic args and columns
    declare
        col text;
        val text;
        rmain record;
        sql text;
    begin
        if do_insert and array_length(field_sql, 1) is not null then
            if TG_OP = 'DELETE' then
                rmain := OLD;
            else
                rmain := NEW;
            end if;

            sql := array_to_string(field_sql, ' union all ');
            for col, val in
                execute sql using rmain
            loop
                if col = 'ev_type' then
                    ev_type := val;
                elsif col = 'ev_extra1' then
                    ev_extra1 := val;
                elsif col = 'ev_extra2' then
                    ev_extra2 := val;
                elsif col = 'ev_extra3' then
                    ev_extra3 := val;
                elsif col = 'ev_extra4' then
                    ev_extra4 := val;
                elsif col = 'when' then
                    if val is null then
                        do_insert := false;
                    end if;
                end if;
            end loop;
        end if;
    end;

    -- insert final values
    if do_insert then
        perform pgq.insert_event(qname, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
    end if;

    if do_skip or TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
        return null;
    elsif TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$ language plpgsql;

create or replace function pgq._urlencode(val text)
returns text as $$
    select replace(string_agg(pair[1] || regexp_replace(encode(convert_to(pair[2], 'utf8'), 'hex'), '..', E'%\\&', 'g'), ''), '%20', '+')
        from regexp_matches($1, '([-_.a-zA-Z0-9]*)([^-_.a-zA-Z0-9]*)', 'g') pair
$$ language sql strict immutable;



create or replace function pgq.sqltriga() returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: pgq.logutriga()
--
--      Trigger function that puts row data in SQL-fragment form into queue.
--
-- Purpose:
--      Anciant way to implement replication.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optional arguments:
--      SKIP                - The actual operation should be skipped (BEFORE trigger)
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--      colname=EXPR        - Override field value with SQL expression.  Can reference table
--                            columns.  colname can be: ev_type, ev_data, ev_extra1 .. ev_extra4
--      when=EXPR           - If EXPR returns false, don't insert event.
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.logutriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
declare
    qname text;
    ev_type text;
    ev_data text;
    ev_extra1 text;
    ev_extra2 text;
    ev_extra3 text;
    ev_extra4 text;
    do_skip boolean := false;
    do_backup boolean := false;
    do_insert boolean := true;
    do_deny boolean := false;
    extra_ignore_list text[];
    full_ignore_list text[];
    ignore_list text[] := '{}';
    pkey_list text[];
    pkey_str text;
    field_sql_sfx text;
    field_sql text[] := '{}';
    data_sql text;
    ignore_col_changes int4 := 0;
begin
    if TG_NARGS < 1 then
        raise exception 'Trigger needs queue name';
    end if;
    qname := TG_ARGV[0];

    -- standard output
    ev_extra1 := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- prepare to handle magic fields
    field_sql_sfx := ')::text as val from (select $1.*) r';
    extra_ignore_list := array['_pgq_ev_type', '_pgq_ev_extra1', '_pgq_ev_extra2',
                               '_pgq_ev_extra3', '_pgq_ev_extra4']::text[];

    -- parse trigger args
    declare
        got boolean;
        argpair text[];
        i integer;
    begin
        for i in 1 .. TG_NARGS-1 loop
            if TG_ARGV[i] in ('skip', 'SKIP') then
                do_skip := true;
            elsif TG_ARGV[i] = 'backup' then
                do_backup := true;
            elsif TG_ARGV[i] = 'deny' then
                do_deny := true;
            else
                got := false;
                for argpair in select regexp_matches(TG_ARGV[i], '^([^=]+)=(.*)') loop
                    got := true;
                    if argpair[1] = 'pkey' then
                        pkey_str := argpair[2];
                        pkey_list := string_to_array(pkey_str, ',');
                    elsif argpair[1] = 'ignore' then
                        ignore_list := string_to_array(argpair[2], ',');
                    elsif argpair[1] ~ '^ev_(type|extra[1-4])$' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (' || argpair[2] || field_sql_sfx);
                    elsif argpair[1] = 'when' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (case when (' || argpair[2]
                                                  || ')::boolean then ''proceed'' else null end' || field_sql_sfx);
                    else
                        got := false;
                    end if;
                end loop;
                if not got then
                    raise exception 'bad argument: %', TG_ARGV[i];
                end if;
            end if;
        end loop;
    end;

    full_ignore_list := ignore_list || extra_ignore_list;

    if pkey_str is null then
        select array_agg(pk.attname)
            from (select k.attname from pg_index i, pg_attribute k
                    where i.indrelid = TG_RELID
                        and k.attrelid = i.indexrelid and i.indisprimary
                        and k.attnum > 0 and not k.attisdropped
                    order by k.attnum) pk
            into pkey_list;
        if pkey_list is null then
            pkey_list := '{}';
            pkey_str := '';
        else
            pkey_str := array_to_string(pkey_list, ',');
        end if;
    end if;
    if pkey_str = '' and TG_OP in ('UPDATE', 'DELETE') then
        raise exception 'Update/Delete on table without pkey';
    end if;

    if TG_OP = 'INSERT' then
        ev_type := 'I';
    elsif TG_OP = 'UPDATE' then
        ev_type := 'U';
    elsif TG_OP = 'DELETE' then
        ev_type := 'D';
    elsif TG_OP = 'TRUNCATE' then
        ev_type := 'R';
    else
        raise exception 'TG_OP not supported: %', TG_OP;
    end if;

    if current_setting('session_replication_role') = 'local' then
        if TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
            return null;
        elsif TG_OP = 'DELETE' then
            return OLD;
        else
            return NEW;
        end if;
    elsif do_deny then
        raise exception 'Table ''%.%'' to queue ''%'': change not allowed (%)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, qname, TG_OP;
    elsif TG_OP = 'TRUNCATE' then
        perform pgq.insert_event(qname, ev_type, '', ev_extra1, ev_extra2, ev_extra3, ev_extra4);
        return null;
    end if;

    -- process table columns
    declare
        attr record;
        pkey_sql_buf text[];
        qcol text;
        data_sql_buf text[];
        ignore_sql text;
        ignore_sql_buf text[];
        pkey_change_sql text;
        pkey_col_changes int4 := 0;
        valexp text;
        sql1_buf text[] := '{}'; -- I:cols, U:vals, D:-
        sql2_buf text[] := '{}'; -- I:vals, U:pks, D:pks
        sql1_buf_fallback text[] := '{}';
        val_sql text;
        has_changed boolean;
    begin
        for attr in
            select k.attnum, k.attname, k.atttypid
                from pg_attribute k
                where k.attrelid = TG_RELID and k.attnum > 0 and not k.attisdropped
                order by k.attnum
        loop
            qcol := quote_ident(attr.attname);
            if attr.attname = any (ignore_list) then
                ignore_sql_buf := array_append(ignore_sql_buf,
                    'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                continue;
            elsif attr.attname = any (extra_ignore_list) then
                field_sql := array_prepend('select ' || quote_literal(substring(attr.attname from 6))
                                           || '::text as key, (r.' || qcol || field_sql_sfx, field_sql);
                continue;
            end if;

            if attr.atttypid = 'boolean'::regtype::oid then
                valexp := 'case r.' || qcol || ' when true then ''t'' when false then ''f'' else null end';
            else
                valexp := 'r.' || qcol || '::text';
            end if;

            if attr.attname = any (pkey_list) then
                pkey_sql_buf := array_append(pkey_sql_buf,
                        'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                if TG_OP in ('UPDATE', 'DELETE') then
                    sql2_buf := array_append(sql2_buf, 'select ' || quote_literal(qcol)
                        || ' || coalesce(''='' || quote_literal(' || valexp|| '), '' is null'') as val'
                        || ' from (select $1.*) r');
                    if array_length(sql1_buf_fallback, 1) is null then
                        sql1_buf_fallback := array_append(sql1_buf_fallback, 'select ' || quote_literal(qcol || '=')
                            || ' || quote_nullable(' || valexp || ') as val'
                            || ' from (select $1.*) r');
                    end if;
                    continue;
                end if;
            end if;

            if TG_OP = 'INSERT' then
                sql1_buf := array_append(sql1_buf, qcol);
                sql2_buf := array_append(sql2_buf, 'select coalesce(quote_literal(' || valexp || '), ''null'') as val'
                    || ' from (select $1.*) r');
            elsif TG_OP = 'UPDATE' then
                execute 'select quote_nullable(rold.' || qcol || ') <> quote_nullable(rnew.' || qcol || ') as has_changed'
                    || ' from (select $1.*) rold, (select $2.*) rnew'
                    using OLD, NEW into has_changed;
                if has_changed then
                    sql1_buf := array_append(sql1_buf, 'select ' || quote_literal(qcol || '=')
                        || ' || quote_nullable(' || valexp || ') as val'
                        || ' from (select $1.*) r');
                end if;
            end if;
        end loop;

        -- SQL to see if pkey columns have changed
        if TG_OP = 'UPDATE' then
            pkey_change_sql := 'select count(1) from (' || array_to_string(pkey_sql_buf, ' union all ')
                            || ') cols where cols.is_changed';
            execute pkey_change_sql using OLD, NEW into pkey_col_changes;
            if pkey_col_changes > 0 then
                raise exception 'primary key update not allowed';
            end if;
        end if;

        -- SQL to see if ignored columns have changed
        if TG_OP = 'UPDATE' and array_length(ignore_list, 1) is not null then
            ignore_sql := 'select count(1) from (' || array_to_string(ignore_sql_buf, ' union all ')
                || ') cols where cols.is_changed';
            execute ignore_sql using OLD, NEW into ignore_col_changes;
        end if;

        -- SQL to load data
        if TG_OP = 'INSERT' then
            data_sql := 'select array_to_string(array[''('', '
                || quote_literal(array_to_string(sql1_buf, ','))
                || ', '') values ('','
                || '(select array_to_string(array_agg(s.val), '','') from (' || array_to_string(sql2_buf, ' union all ') || ') s)'
                || ', '')'''
                || '], '''')';
        elsif TG_OP = 'UPDATE' then
            if array_length(sql1_buf, 1) is null then
                sql1_buf := sql1_buf_fallback;
            end if;
            data_sql := 'select array_to_string(array['
                || '(select array_to_string(array_agg(s.val), '','') from (' || array_to_string(sql1_buf, ' union all ') || ') s)'
                || ', '' where '','
                || '(select array_to_string(array_agg(s.val), '' and '') from (' || array_to_string(sql2_buf, ' union all ') || ') s)'
                || '], '''')';
        else
            data_sql := 'select array_to_string(array['
                || '(select array_to_string(array_agg(s.val), '' and '') from (' || array_to_string(sql2_buf, ' union all ') || ') s)'
                || '], '''')';
        end if;
    end;

    -- render data
    declare
        old_data text;
    begin
        if TG_OP = 'INSERT' then
            execute data_sql using NEW into ev_data;
        elsif TG_OP = 'UPDATE' then

            -- render NEW
            execute data_sql using NEW into ev_data;

            -- render OLD when needed
            if do_backup or array_length(ignore_list, 1) is not null then
                execute data_sql using OLD into old_data;
            end if;

            -- only change was to ignored columns?
            if old_data = ev_data and ignore_col_changes > 0 then
                do_insert := false;
            end if;

            -- is backup needed?
            if do_backup then
                ev_extra2 := old_data;
            end if;
        elsif TG_OP = 'DELETE' then
            execute data_sql using OLD into ev_data;
        end if;
    end;

    -- apply magic args and columns
    declare
        col text;
        val text;
        rmain record;
        sql text;
    begin
        if do_insert and array_length(field_sql, 1) is not null then
            if TG_OP = 'DELETE' then
                rmain := OLD;
            else
                rmain := NEW;
            end if;

            sql := array_to_string(field_sql, ' union all ');
            for col, val in
                execute sql using rmain
            loop
                if col = 'ev_type' then
                    ev_type := val;
                elsif col = 'ev_extra1' then
                    ev_extra1 := val;
                elsif col = 'ev_extra2' then
                    ev_extra2 := val;
                elsif col = 'ev_extra3' then
                    ev_extra3 := val;
                elsif col = 'ev_extra4' then
                    ev_extra4 := val;
                elsif col = 'when' then
                    if val is null then
                        do_insert := false;
                    end if;
                end if;
            end loop;
        end if;
    end;

    -- insert final values
    if do_insert then
        perform pgq.insert_event(qname, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
    end if;

    if do_skip or TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
        return null;
    elsif TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$ language plpgsql;







grant usage on schema pgq to public;

-- old default grants
grant select on table pgq.consumer to public;
grant select on table pgq.queue to public;
grant select on table pgq.tick to public;
grant select on table pgq.queue to public;
grant select on table pgq.subscription to public;
grant select on table pgq.event_template to public;
grant select on table pgq.retry_queue to public;






-- 1.public --
REVOKE ALL ON FUNCTION pgq.seq_getval(text),
    pgq.get_queue_info(),
    pgq.get_queue_info(text),
    pgq.get_consumer_info(),
    pgq.get_consumer_info(text),
    pgq.get_consumer_info(text, text),
    pgq.quote_fqname(text),
    pgq.version()
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;

-- 2.consumer --
REVOKE ALL ON FUNCTION pgq.batch_event_sql(bigint),
    pgq.batch_event_tables(bigint),
    pgq.find_tick_helper(int4, int8, timestamptz, int8, int8, interval),
    pgq.register_consumer(text, text),
    pgq.register_consumer_at(text, text, bigint),
    pgq.unregister_consumer(text, text),
    pgq.next_batch_info(text, text),
    pgq.next_batch(text, text),
    pgq.next_batch_custom(text, text, interval, int4, interval),
    pgq.get_batch_events(bigint),
    pgq.get_batch_info(bigint),
    pgq.get_batch_cursor(bigint, text, int4, text),
    pgq.get_batch_cursor(bigint, text, int4),
    pgq.event_retry(bigint, bigint, timestamptz),
    pgq.event_retry(bigint, bigint, integer),
    pgq.batch_retry(bigint, integer),
    pgq.force_tick(text),
    pgq.finish_batch(bigint)
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;
REVOKE ALL ON FUNCTION pgq.batch_event_sql(bigint),
    pgq.batch_event_tables(bigint),
    pgq.find_tick_helper(int4, int8, timestamptz, int8, int8, interval),
    pgq.register_consumer(text, text),
    pgq.register_consumer_at(text, text, bigint),
    pgq.unregister_consumer(text, text),
    pgq.next_batch_info(text, text),
    pgq.next_batch(text, text),
    pgq.next_batch_custom(text, text, interval, int4, interval),
    pgq.get_batch_events(bigint),
    pgq.get_batch_info(bigint),
    pgq.get_batch_cursor(bigint, text, int4, text),
    pgq.get_batch_cursor(bigint, text, int4),
    pgq.event_retry(bigint, bigint, timestamptz),
    pgq.event_retry(bigint, bigint, integer),
    pgq.batch_retry(bigint, integer),
    pgq.force_tick(text),
    pgq.finish_batch(bigint)
  FROM public CASCADE;

-- 3.producer --
REVOKE ALL ON FUNCTION pgq.insert_event(text, text, text),
    pgq.insert_event(text, text, text, text, text, text, text),
    pgq.current_event_table(text),
    pgq.jsontriga(),
    pgq.sqltriga(),
    pgq.logutriga()
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;
REVOKE ALL ON FUNCTION pgq.insert_event(text, text, text),
    pgq.insert_event(text, text, text, text, text, text, text),
    pgq.current_event_table(text),
    pgq.jsontriga(),
    pgq.sqltriga(),
    pgq.logutriga()
  FROM public CASCADE;

-- 4.admin --
REVOKE ALL ON FUNCTION pgq.ticker(text, bigint, timestamptz, bigint),
    pgq.ticker(text),
    pgq.ticker(),
    pgq.maint_retry_events(),
    pgq.maint_rotate_tables_step1(text),
    pgq.maint_rotate_tables_step2(),
    pgq.maint_tables_to_vacuum(),
    pgq.maint_operations(),
    pgq.upgrade_schema(),
    pgq.grant_perms(text),
    pgq._grant_perms_from(text,text,text,text),
    pgq.tune_storage(text),
    pgq.seq_setval(text, int8),
    pgq.create_queue(text),
    pgq.drop_queue(text, bool),
    pgq.drop_queue(text),
    pgq.set_queue_config(text, text, text),
    pgq.insert_event_raw(text, bigint, timestamptz, integer, integer, text, text, text, text, text, text),
    pgq.event_retry_raw(text, text, timestamptz, bigint, timestamptz, integer, text, text, text, text, text, text)
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;
REVOKE ALL ON FUNCTION pgq.ticker(text, bigint, timestamptz, bigint),
    pgq.ticker(text),
    pgq.ticker(),
    pgq.maint_retry_events(),
    pgq.maint_rotate_tables_step1(text),
    pgq.maint_rotate_tables_step2(),
    pgq.maint_tables_to_vacuum(),
    pgq.maint_operations(),
    pgq.upgrade_schema(),
    pgq.grant_perms(text),
    pgq._grant_perms_from(text,text,text,text),
    pgq.tune_storage(text),
    pgq.seq_setval(text, int8),
    pgq.create_queue(text),
    pgq.drop_queue(text, bool),
    pgq.drop_queue(text),
    pgq.set_queue_config(text, text, text),
    pgq.insert_event_raw(text, bigint, timestamptz, integer, integer, text, text, text, text, text, text),
    pgq.event_retry_raw(text, text, timestamptz, bigint, timestamptz, integer, text, text, text, text, text, text)
  FROM public CASCADE;

-- 5.event.tables --
REVOKE ALL ON TABLE pgq.event_template
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;

-- 5.meta.tables --
REVOKE ALL ON TABLE pgq.consumer, pgq.queue, pgq.tick, pgq.subscription
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;

-- 6.retry.event --
REVOKE ALL ON TABLE pgq.retry_queue
  FROM pgq_reader, pgq_writer, pgq_admin, public CASCADE;

-- 1.public --
GRANT execute ON FUNCTION pgq.seq_getval(text),
    pgq.get_queue_info(),
    pgq.get_queue_info(text),
    pgq.get_consumer_info(),
    pgq.get_consumer_info(text),
    pgq.get_consumer_info(text, text),
    pgq.quote_fqname(text),
    pgq.version()
  TO public;

-- 2.consumer --
GRANT execute ON FUNCTION pgq.batch_event_sql(bigint),
    pgq.batch_event_tables(bigint),
    pgq.find_tick_helper(int4, int8, timestamptz, int8, int8, interval),
    pgq.register_consumer(text, text),
    pgq.register_consumer_at(text, text, bigint),
    pgq.unregister_consumer(text, text),
    pgq.next_batch_info(text, text),
    pgq.next_batch(text, text),
    pgq.next_batch_custom(text, text, interval, int4, interval),
    pgq.get_batch_events(bigint),
    pgq.get_batch_info(bigint),
    pgq.get_batch_cursor(bigint, text, int4, text),
    pgq.get_batch_cursor(bigint, text, int4),
    pgq.event_retry(bigint, bigint, timestamptz),
    pgq.event_retry(bigint, bigint, integer),
    pgq.batch_retry(bigint, integer),
    pgq.force_tick(text),
    pgq.finish_batch(bigint)
  TO pgq_reader;

-- 3.producer --
GRANT execute ON FUNCTION pgq.insert_event(text, text, text),
    pgq.insert_event(text, text, text, text, text, text, text),
    pgq.current_event_table(text),
    pgq.jsontriga(),
    pgq.sqltriga(),
    pgq.logutriga()
  TO pgq_writer;

-- 4.admin --
GRANT execute ON FUNCTION pgq.ticker(text, bigint, timestamptz, bigint),
    pgq.ticker(text),
    pgq.ticker(),
    pgq.maint_retry_events(),
    pgq.maint_rotate_tables_step1(text),
    pgq.maint_rotate_tables_step2(),
    pgq.maint_tables_to_vacuum(),
    pgq.maint_operations(),
    pgq.upgrade_schema(),
    pgq.grant_perms(text),
    pgq._grant_perms_from(text,text,text,text),
    pgq.tune_storage(text),
    pgq.seq_setval(text, int8),
    pgq.create_queue(text),
    pgq.drop_queue(text, bool),
    pgq.drop_queue(text),
    pgq.set_queue_config(text, text, text),
    pgq.insert_event_raw(text, bigint, timestamptz, integer, integer, text, text, text, text, text, text),
    pgq.event_retry_raw(text, text, timestamptz, bigint, timestamptz, integer, text, text, text, text, text, text)
  TO pgq_admin;

-- 5.event.tables --
GRANT select ON TABLE pgq.event_template
  TO pgq_reader;
GRANT select, insert, truncate ON TABLE pgq.event_template
  TO pgq_admin;

-- 5.meta.tables --
GRANT select ON TABLE pgq.consumer, pgq.queue, pgq.tick, pgq.subscription
  TO pgq_reader;
GRANT select, insert, update, delete ON TABLE pgq.consumer, pgq.queue, pgq.tick, pgq.subscription
  TO pgq_admin;
GRANT select ON TABLE pgq.consumer, pgq.queue, pgq.tick, pgq.subscription
  TO public;

-- 6.retry.event --
GRANT select, insert, update, delete ON TABLE pgq.retry_queue
  TO pgq_admin;

