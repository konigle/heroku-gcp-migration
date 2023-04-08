


-- ----------------------------------------------------------------------
-- File: Tables
--
--      Schema 'pgq_node', contains tables for cascaded pgq.
--
-- Event types for cascaded queue:
--      pgq.location-info       - ev_data: node_name, extra1: queue_name, extra2: location, extra3: dead
--                                It contains updated node connect string.
--
--      pgq.global-watermark    - ev_data: tick_id,  extra1: queue_name
--                                Root node sends minimal tick_id that must be kept.
--
--      pgq.tick-id             - ev_data: tick_id,  extra1: queue_name
--                                Partition node inserts its tick-id into combined queue.
--
-- ----------------------------------------------------------------------

create schema pgq_node;

-- ----------------------------------------------------------------------
-- Table: pgq_node.node_location
--
--      Static table that just lists all members in set.
--
-- Columns:
--      queue_name      - cascaded queue name
--      node_name       - node name
--      node_location   - libpq connect string for connecting to node
--      dead            - whether the node is offline
-- ----------------------------------------------------------------------
create table pgq_node.node_location (
    queue_name      text not null,
    node_name       text not null,
    node_location   text not null,
    dead            boolean not null default false,

    primary key (queue_name, node_name)
);

-- ----------------------------------------------------------------------
-- Table: pgq_node.node_info
--
--      Local node info.
--
-- Columns:
--      queue_name          - cascaded queue name
--      node_type           - local node type
--      node_name           - local node name
--      worker_name         - consumer name that maintains this node
--      combined_queue      - on 'leaf' the target combined set name
--      node_attrs          - urlencoded fields for worker
--
-- Node types:
--      root            - data + batches is generated here
--      branch          - replicates full queue contents and maybe contains some tables
--      leaf            - does not replicate queue / or uses combined queue for that
-- ----------------------------------------------------------------------
create table pgq_node.node_info (
    queue_name      text not null primary key,
    node_type       text not null,
    node_name       text not null,
    worker_name     text,
    combined_queue  text,
    node_attrs      text,

    foreign key (queue_name, node_name) references pgq_node.node_location,
    check (node_type in ('root', 'branch', 'leaf')),
    check (case when node_type = 'root'   then  (worker_name is not null and combined_queue is null)
                when node_type = 'branch' then  (worker_name is not null and combined_queue is null)
                when node_type = 'leaf'   then  (worker_name is not null)
                else false end)
);

-- ----------------------------------------------------------------------
-- Table: pgq_node.local_state
--
--      All cascaded consumers (both worker and non-worker)
--      keep their state here.
--
-- Columns:
--      queue_name      - cascaded queue name
--      consumer_name   - cascaded consumer name
--      provider_node   - node name the consumer reads from
--      last_tick_id    - last committed tick id on this node
--      cur_error       - reason why current batch failed
--      paused          - whether consumer should wait
--      uptodate        - if consumer has seen new state
-- ----------------------------------------------------------------------
create table pgq_node.local_state (
    queue_name      text not null,
    consumer_name   text not null,
    provider_node   text not null,
    last_tick_id    bigint not null,
    cur_error       text,
    paused          boolean not null default false,
    uptodate        boolean not null default false,

    primary key (queue_name, consumer_name),
    foreign key (queue_name) references pgq_node.node_info,
    foreign key (queue_name, provider_node) references pgq_node.node_location
);

-- ----------------------------------------------------------------------
-- Table: pgq_node.subscriber_info
--
--      List of nodes that subscribe to local node.
--
-- Columns:
--      queue_name      - cascaded queue name
--      subscriber_node - node name that uses this node as provider.
--      worker_name     - consumer name that maintains remote node
-- ----------------------------------------------------------------------
create table pgq_node.subscriber_info (
    queue_name          text not null,
    subscriber_node     text not null,
    worker_name         text not null,
    watermark_name      text not null,

    primary key (queue_name, subscriber_node),
    foreign key (queue_name) references pgq_node.node_info,
    foreign key (queue_name, subscriber_node) references pgq_node.node_location,
    foreign key (worker_name) references pgq.consumer (co_name),
    foreign key (watermark_name) references pgq.consumer (co_name)
);



-- File: Functions
--
--      Database functions for cascaded pgq.
--
-- Cascaded consumer flow:
--
--  - (1) [target] call pgq_node.get_consumer_state()
--  - (2) If .paused is true, sleep, go to (1).
--    This is allows to control consumer remotely.
--  - (3) If .uptodate is false, call pgq_node.set_consumer_uptodate(true).
--    This allows remote controller to know that consumer has seen the changes.
--  - (4) [source] call pgq.next_batch().  If returns NULL, sleep, goto (1)
--  - (5) [source] if batch already done, call pgq.finish_batch(), go to (1)
--  - (6) [source] read events
--  - (7) [target] process events, call pgq_node.set_consumer_completed() in same tx.
--  - (8) [source] call pgq.finish_batch()
--
-- Cascaded worker flow:
--
-- Worker is consumer that also copies to queue contents to local node (branch),
-- so it can act as provider to other nodes.  There can be only one worker per
-- node.  Or zero if node is leaf.  In addition to cascaded consumer logic above, it must -
--      - [branch] copy all events to local queue and create ticks
--      - [merge-leaf] copy all events to combined-queue
--      - [branch] publish local watermark upwards to provider so it reaches root.
--      - [branch] apply global watermark event to local node
--      - [merge-leaf] wait-behind on combined-branch (failover combined-root).
--        It's last_tick_id is set by combined-branch worker, it must call
--        pgq.next_batch()+pgq.finish_batch() without processing events
--        when behind, but not move further.  When the combined-branch
--        becomes root, it will be in right position to continue updating.
--



create or replace function pgq_node.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    cnt int4 = 0;
begin
    -- node_info.node_attrs
    perform 1 from information_schema.columns
      where table_schema = 'pgq_node'
        and table_name = 'node_info'
        and column_name = 'node_attrs';
    if not found then
        alter table pgq_node.node_info add column node_attrs text;
        cnt := cnt + 1;
    end if;

    return cnt;
end;
$$ language plpgsql;


select pgq_node.upgrade_schema();

-- Group: Global Node Map


create or replace function pgq_node.register_location(
    in i_queue_name text,
    in i_node_name text,
    in i_node_location text,
    in i_dead boolean,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.register_location(4)
--
--      Add new node location.
--
-- Parameters:
--      i_queue_name - queue name
--      i_node_name - node name
--      i_node_location - node connect string
--      i_dead - dead flag for node
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
-- ----------------------------------------------------------------------
declare
    node record;
begin
    select node_type = 'root' as is_root into node
      from pgq_node.node_info where queue_name = i_queue_name
       for update;
    -- may return 0 rows

    perform 1 from pgq_node.node_location
     where queue_name = i_queue_name
       and node_name = i_node_name;
    if found then
        update pgq_node.node_location
           set node_location = coalesce(i_node_location, node_location),
               dead = i_dead
         where queue_name = i_queue_name
           and node_name = i_node_name;
    elsif i_node_location is not null then
        insert into pgq_node.node_location (queue_name, node_name, node_location, dead)
        values (i_queue_name, i_node_name, i_node_location, i_dead);
    end if;

    if node.is_root then
        perform pgq.insert_event(i_queue_name, 'pgq.location-info',
                                 i_node_name, i_queue_name, i_node_location, i_dead::text, null)
           from pgq_node.node_info n
         where n.queue_name = i_queue_name;
    end if;

    select 200, 'Location registered' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.unregister_location(
    in i_queue_name text,
    in i_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.unregister_location(2)
--
--      Drop unreferenced node.
--
-- Parameters:
--      i_queue_name - queue name
--      i_node_name - node to drop
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      301 - Location not found
--      403 - Cannot drop node's own or parent location
-- ----------------------------------------------------------------------
declare
    _queue_name  text;
    _wm_consumer text;
    _global_wm   bigint;
    sub          record;
    node         record;
begin
    select n.node_name, n.node_type, s.provider_node
        into node
        from pgq_node.node_info n
        left join pgq_node.local_state s
        on (s.consumer_name = n.worker_name
            and s.queue_name = n.queue_name)
        where n.queue_name = i_queue_name;
    if found then
        if node.node_name = i_node_name then
            select 403, 'Cannot drop node''s own location' into ret_code, ret_note;
            return;
        end if;
        if node.provider_node = i_node_name then
            select 403, 'Cannot drop location of node''s parent' into ret_code, ret_note;
            return;
        end if;
    end if;

    --
    -- There may be obsolete subscriptions around
    -- drop them silently.
    --
    perform pgq_node.unregister_subscriber(i_queue_name, i_node_name);

    --
    -- Actual removal
    --
    delete from pgq_node.node_location
     where queue_name = i_queue_name
       and node_name = i_node_name;

    if found then
        select 200, 'Ok' into ret_code, ret_note;
    else
        select 301, 'Location not found: ' || i_queue_name || '/' || i_node_name
          into ret_code, ret_note;
    end if;

    if node.node_type = 'root' then
        perform pgq.insert_event(i_queue_name, 'pgq.unregister-location',
                                 i_node_name, i_queue_name, null, null, null)
           from pgq_node.node_info n
         where n.queue_name = i_queue_name;
    end if;

    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.get_queue_locations(
    in i_queue_name text,

    out node_name text,
    out node_location text,
    out dead boolean
) returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_queue_locations(1)
--
--      Get node list for the queue.
--
-- Parameters:
--      i_queue_name    - queue name
--
-- Returns:
--      node_name       - node name
--      node_location   - libpq connect string for the node
--      dead            - whether the node should be considered dead
-- ----------------------------------------------------------------------
begin
    for node_name, node_location, dead in
        select l.node_name, l.node_location, l.dead
          from pgq_node.node_location l
         where l.queue_name = i_queue_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;



-- Group: Node operations


create or replace function pgq_node.create_node(
    in i_queue_name text,
    in i_node_type text,
    in i_node_name text,
    in i_worker_name text,
    in i_provider_name text,
    in i_global_watermark bigint,
    in i_combined_queue text,
    out ret_code int4,
    out ret_note  text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.create_node(7)
--
--      Initialize node.
--
-- Parameters:
--      i_node_name - cascaded queue name
--      i_node_type - node type
--      i_node_name - node name
--      i_worker_name - worker consumer name
--      i_provider_name - provider node name for non-root nodes
--      i_global_watermark - global lowest tick_id
--      i_combined_queue - merge-leaf: target queue
--
-- Returns:
--      200 - Ok
--      401 - node already initialized
--      ???? - maybe we coud use more error codes ?
--
-- Node Types:
--      root - master node
--      branch - subscriber node that can be provider to others
--      leaf - subscriber node that cannot be provider to others
-- Calls:
--      None
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
declare
    _wm_consumer text;
    _global_wm bigint;
begin
    perform 1 from pgq_node.node_info where queue_name = i_queue_name;
    if found then
        select 401, 'Node already initialized' into ret_code, ret_note;
        return;
    end if;

    _wm_consumer := '.global_watermark';

    if i_node_type = 'root' then
        if coalesce(i_provider_name, i_global_watermark::text,
                    i_combined_queue) is not null then
            select 401, 'unexpected args for '||i_node_type into ret_code, ret_note;
            return;
        end if;

        perform pgq.create_queue(i_queue_name);
        perform pgq.register_consumer(i_queue_name, _wm_consumer);
        _global_wm := (select last_tick from pgq.get_consumer_info(i_queue_name, _wm_consumer));
    elsif i_node_type = 'branch' then
        if i_provider_name is null then
            select 401, 'provider not set for '||i_node_type into ret_code, ret_note;
            return;
        end if;
        if i_global_watermark is null then
            select 401, 'global watermark not set for '||i_node_type into ret_code, ret_note;
            return;
        end if;
        perform pgq.create_queue(i_queue_name);
        update pgq.queue
            set queue_external_ticker = true,
                queue_disable_insert = true
            where queue_name = i_queue_name;
        if i_global_watermark > 1 then
            perform pgq.ticker(i_queue_name, i_global_watermark, now(), 1);
        end if;
        perform pgq.register_consumer_at(i_queue_name, _wm_consumer, i_global_watermark);
        _global_wm := i_global_watermark;
    elsif i_node_type = 'leaf' then
        _global_wm := i_global_watermark;
        if i_combined_queue is not null then
            perform 1 from pgq.get_queue_info(i_combined_queue);
            if not found then
                select 401, 'non-existing queue on leaf side: '||i_combined_queue
                into ret_code, ret_note;
                return;
            end if;
        end if;
    else
        select 401, 'bad node type: '||i_node_type
          into ret_code, ret_note;
    end if;

    insert into pgq_node.node_info
      (queue_name, node_type, node_name,
       worker_name, combined_queue)
    values (i_queue_name, i_node_type, i_node_name,
       i_worker_name, i_combined_queue);

    if i_node_type <> 'root' then
        select f.ret_code, f.ret_note into ret_code, ret_note
          from pgq_node.register_consumer(i_queue_name, i_worker_name,
                    i_provider_name, _global_wm) f;
    else
        select f.ret_code, f.ret_note into ret_code, ret_note
          from pgq_node.register_consumer(i_queue_name, i_worker_name,
                    i_node_name, _global_wm) f;
    end if;
        if ret_code <> 200 then
            return;
        end if;

    select 200, 'Node "' || i_node_name || '" initialized for queue "'
           || i_queue_name || '" with type "' || i_node_type || '"'
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.drop_node(
    in i_queue_name text,
    in i_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.drop_node(2)
--
--      Drop node. This needs to be run on all the members of a set
--      to properly get rid of the node.
--
-- Parameters:
--      i_queue_name - queue name
--      i_node_name - node_name
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      304 - No such queue
--      406 - That is a provider
-- Calls:
--      None
-- Tables directly manipulated:
--      None
------------------------------------------------------------------------
declare
    _is_local   boolean;
    _is_prov    boolean;
begin
    select (n.node_name = i_node_name),
           (select s.provider_node = i_node_name
              from pgq_node.local_state s
              where s.queue_name = i_queue_name
                and s.consumer_name = n.worker_name)
        into _is_local, _is_prov
        from pgq_node.node_info n
        where n.queue_name = i_queue_name;

    if not found then
        -- proceed with cleaning anyway, as there schenarios
        -- where some data is left around
        _is_prov := false;
        _is_local := true;
    end if;

    -- drop local state
    if _is_local then
        delete from pgq_node.subscriber_info
         where queue_name = i_queue_name;

        delete from pgq_node.local_state
         where queue_name = i_queue_name;

        delete from pgq_node.node_info
         where queue_name = i_queue_name
            and node_name = i_node_name;

        perform pgq.drop_queue(queue_name, true)
           from pgq.queue where queue_name = i_queue_name;

        delete from pgq_node.node_location
         where queue_name = i_queue_name
           and node_name <> i_node_name;
    elsif _is_prov then
        select 405, 'Cannot drop provider node: ' || i_node_name into ret_code, ret_note;
        return;
    else
        perform pgq_node.unregister_subscriber(i_queue_name, i_node_name);
    end if;

    -- let the unregister_location send event if needed
    select f.ret_code, f.ret_note
        from pgq_node.unregister_location(i_queue_name, i_node_name) f
        into ret_code, ret_note;

    select 200, 'Node dropped: ' || i_node_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;


-- \i functions/pgq_node.rename_node.sql


drop function if exists pgq_node.get_node_info(text);

create or replace function pgq_node.get_node_info(
    in i_queue_name text,

    out ret_code int4,
    out ret_note text,
    out node_type text,
    out node_name text,
    out global_watermark bigint,
    out local_watermark bigint,
    out provider_node text,
    out provider_location text,

    out combined_queue text,
    out combined_type text,

    out worker_name text,
    out worker_paused bool,
    out worker_uptodate bool,
    out worker_last_tick bigint,
    out node_attrs text,

    out target_for text[]
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_node_info(1)
--
--      Get local node info for cascaded queue.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      node_type - local node type
--      node_name - local node name
--      global_watermark - queue's global watermark
--      local_watermark - queue's local watermark, for this and below nodes
--      provider_node - provider node name
--      provider_location - provider connect string
--      combined_queue - queue name for target set
--      combined_type - node type of target set
--      worker_name - consumer name that maintains this node
--      worker_paused - is worker paused
--      worker_uptodate - is worker seen the changes
--      worker_last_tick - last committed tick_id by worker
--      node_attrs - urlencoded dict of random attrs for worker (eg. sync_watermark)
-- ----------------------------------------------------------------------
declare
    sql text;
begin
    select 100, 'Ok', n.node_type, n.node_name,
           c.node_type, c.queue_name, w.provider_node, l.node_location,
           n.worker_name, w.paused, w.uptodate, w.last_tick_id,
           n.node_attrs
      into ret_code, ret_note, node_type, node_name,
           combined_type, combined_queue, provider_node, provider_location,
           worker_name, worker_paused, worker_uptodate, worker_last_tick,
           node_attrs
      from pgq_node.node_info n
           left join pgq_node.node_info c on (c.queue_name = n.combined_queue)
           left join pgq_node.local_state w on (w.queue_name = n.queue_name and w.consumer_name = n.worker_name)
           left join pgq_node.node_location l on (l.queue_name = w.queue_name and l.node_name = w.provider_node)
      where n.queue_name = i_queue_name;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    if node_type in ('root', 'branch') then
        select min(case when consumer_name = '.global_watermark' then null else last_tick end),
               min(case when consumer_name = '.global_watermark' then last_tick else null end)
          into local_watermark, global_watermark
          from pgq.get_consumer_info(i_queue_name);
        if local_watermark is null then
            select t.tick_id into local_watermark
              from pgq.tick t, pgq.queue q
             where t.tick_queue = q.queue_id
               and q.queue_name = i_queue_name
             order by 1 desc
             limit 1;
        end if;

        select array_agg(x.queue_name) into target_for
          from (
            select q.queue_name
              from pgq_node.node_info q
             where q.combined_queue = i_queue_name
             order by q.queue_name
          ) x;
    else
        local_watermark := worker_last_tick;
    end if;

    if node_type = 'root' then
        select tick_id from pgq.tick t, pgq.queue q
         where q.queue_name = i_queue_name
           and t.tick_queue = q.queue_id
         order by t.tick_queue desc, t.tick_id desc
         limit 1
         into worker_last_tick;
    end if;

    return;
end;
$$ language plpgsql security definer;



create or replace function pgq_node.is_root_node(i_queue_name text)
returns bool as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.is_root_node(1)
--
--      Checs if node is root.
--
-- Parameters:
--      i_queue_name  - queue name
-- Returns:
--      true - if this this the root node for queue 
-- ----------------------------------------------------------------------
declare
    res bool;
begin
    select n.node_type = 'root' into res
      from pgq_node.node_info n
      where n.queue_name = i_queue_name;
    if not found then
        raise exception 'queue does not exist: %', i_queue_name;
    end if;
    return res;
end;
$$ language plpgsql;



create or replace function pgq_node.is_leaf_node(i_queue_name text)
returns bool as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.is_leaf_node(1)
--
--      Checs if node is leaf.
--
-- Parameters:
--      i_queue_name  - queue name
-- Returns:
--      true - if this this the leaf node for queue 
-- ----------------------------------------------------------------------
declare
    res bool;
begin
    select n.node_type = 'leaf' into res
      from pgq_node.node_info n
      where n.queue_name = i_queue_name;
    if not found then
        raise exception 'queue does not exist: %', i_queue_name;
    end if;
    return res;
end;
$$ language plpgsql;




create or replace function pgq_node.get_subscriber_info(
    in i_queue_name text,

    out node_name text,
    out worker_name text,
    out node_watermark int8)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_subscriber_info(1)
--
--      Get subscriber list for the local node.
--
--      It may be out-of-date, due to in-progress
--      administrative change.
--      Node's local provider info ( pgq_node.get_node_info() or pgq_node.get_worker_state(1) )
--      is the authoritative source.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      node_name       - node name that uses current node as provider
--      worker_name     - consumer that maintains remote node
--      local_watermark - lowest tick_id on subscriber
-- ----------------------------------------------------------------------
declare
    _watermark_name text;
begin
    for node_name, worker_name, _watermark_name in
        select s.subscriber_node, s.worker_name, s.watermark_name
          from pgq_node.subscriber_info s
         where s.queue_name = i_queue_name
         order by 1
    loop
        select last_tick into node_watermark
            from pgq.get_consumer_info(i_queue_name, _watermark_name);
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.get_consumer_info(
    in i_queue_name text,

    out consumer_name text,
    out provider_node text,
    out last_tick_id int8,
    out paused boolean,
    out uptodate boolean,
    out cur_error text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_consumer_info(1)
--
--      Get consumer list that work on the local node.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      consumer_name   - cascaded consumer name
--      provider_node   - node from where the consumer reads from
--      last_tick_id    - last committed tick
--      paused          - if consumer is paused
--      uptodate        - if consumer is uptodate
--      cur_error       - failure reason
-- ----------------------------------------------------------------------
begin
    for consumer_name, provider_node, last_tick_id, paused, uptodate, cur_error in
        select s.consumer_name, s.provider_node, s.last_tick_id,
               s.paused, s.uptodate, s.cur_error
            from pgq_node.local_state s
            where s.queue_name = i_queue_name
            order by 1
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;





create or replace function pgq_node.demote_root(
    in i_queue_name text,
    in i_step int4,
    in i_new_provider text,
    out ret_code int4,
    out ret_note text,
    out last_tick int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.demote_root(3)
--
--      Multi-step root demotion to branch.
--
--      Must be be called for each step in sequence:
--
--      Step 1 - disable writing to queue.
--      Step 2 - wait until writers go away, do tick.
--      Step 3 - change type, register.
--
-- Parameters:
--      i_queue_name    - queue name
--      i_step          - step number
--      i_new_provider  - new provider node
-- Returns:
--      200 - success
--      404 - node not initialized for queue 
--      301 - node is not root
-- ----------------------------------------------------------------------
declare
    n_type      text;
    w_name      text;
    sql         text;
    ev_id       int8;
    ev_tbl      text;
begin
    select node_type, worker_name into n_type, w_name
        from pgq_node.node_info
        where queue_name = i_queue_name
        for update;
    if not found then
        select 404, 'Node not initialized for queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;

    if n_type != 'root' then
        select 301, 'Node not root'
          into ret_code, ret_note;
        return;
    end if;
    if i_step > 1 then
        select queue_data_pfx
            into ev_tbl
            from pgq.queue
            where queue_name = i_queue_name
                and queue_disable_insert
                and queue_external_ticker;
        if not found then
            raise exception 'steps in wrong order';
        end if;
    end if;

    if i_step = 1 then
        update pgq.queue
            set queue_disable_insert = true,
                queue_external_ticker = true
            where queue_name = i_queue_name;
        if not found then
            select 404, 'Huh, no queue?: ' || i_queue_name
              into ret_code, ret_note;
            return;
        end if;
        select 200, 'Step 1: Writing disabled for: ' || i_queue_name
          into ret_code, ret_note;
    elsif i_step = 2 then
        set local session_replication_role = 'replica';

        -- lock parent table to stop updates, allow reading
        sql := 'lock table ' || ev_tbl || ' in exclusive mode';
        execute sql;
        

        select nextval(queue_tick_seq), nextval(queue_event_seq)
            into last_tick, ev_id
            from pgq.queue
            where queue_name = i_queue_name;

        perform pgq.ticker(i_queue_name, last_tick, now(), ev_id);

        select 200, 'Step 2: Inserted last tick: ' || i_queue_name
            into ret_code, ret_note;
    elsif i_step = 3 then
        -- change type, point worker to new provider
        select t.tick_id into last_tick
            from pgq.tick t, pgq.queue q
            where q.queue_name = i_queue_name
                and t.tick_queue = q.queue_id
            order by t.tick_queue desc, t.tick_id desc
            limit 1;
        update pgq_node.node_info
            set node_type = 'branch'
            where queue_name = i_queue_name;
        update pgq_node.local_state
            set provider_node = i_new_provider,
                last_tick_id = last_tick,
                uptodate = false
            where queue_name = i_queue_name
                and consumer_name = w_name;
        select 200, 'Step 3: Demoted root to branch: ' || i_queue_name
          into ret_code, ret_note;
    else
        raise exception 'incorrect step number';
    end if;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.promote_branch(
    in i_queue_name text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.promote_branch(1)
--
--      Promote branch node to root.
--
-- Parameters:
--      i_queue_name  - queue name
--
-- Returns:
--      200 - success
--      404 - node not initialized for queue 
--      301 - node is not branch
-- ----------------------------------------------------------------------
declare
    n_name      text;
    n_type      text;
    w_name      text;
    last_tick   bigint;
    sql         text;
begin
    select node_name, node_type, worker_name into n_name, n_type, w_name
        from pgq_node.node_info
        where queue_name = i_queue_name
        for update;
    if not found then
        select 404, 'Node not initialized for queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;

    if n_type != 'branch' then
        select 301, 'Node not branch'
          into ret_code, ret_note;
        return;
    end if;

    update pgq.queue
        set queue_disable_insert = false,
            queue_external_ticker = false
        where queue_name = i_queue_name;

    -- change type, point worker to itself
    select t.tick_id into last_tick
        from pgq.tick t, pgq.queue q
        where q.queue_name = i_queue_name
            and t.tick_queue = q.queue_id
        order by t.tick_queue desc, t.tick_id desc
        limit 1;

    -- make tick seq larger than last tick
    perform pgq.seq_setval(queue_tick_seq, last_tick)
        from pgq.queue where queue_name = i_queue_name;

    update pgq_node.node_info
        set node_type = 'root'
        where queue_name = i_queue_name;

    update pgq_node.local_state
        set provider_node = n_name,
            last_tick_id = last_tick,
            uptodate = false
        where queue_name = i_queue_name
            and consumer_name = w_name;

    select 200, 'Branch node promoted to root'
      into ret_code, ret_note;

    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.set_node_attrs(
    in i_queue_name text,
    in i_node_attrs text,
    out ret_code int4,
    out ret_note  text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.create_attrs(2)
--
--      Set node attributes.
--
-- Parameters:
--      i_node_name - cascaded queue name
--      i_node_attrs - urlencoded node attrs
--
-- Returns:
--      200 - ok
--      404 - node not found
-- ----------------------------------------------------------------------
begin
    update pgq_node.node_info
        set node_attrs = i_node_attrs
        where queue_name = i_queue_name;
    if not found then
        select 404, 'Node not found' into ret_code, ret_note;
        return;
    end if;

    select 200, 'Node attributes updated'
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;



-- Group: Provider side operations - worker


create or replace function pgq_node.register_subscriber(
    in i_queue_name text,
    in i_remote_node_name text,
    in i_remote_worker_name text,
    in i_custom_tick_id int8,
    out ret_code int4,
    out ret_note text,
    out global_watermark bigint)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.register_subscriber(4)
--
--      Subscribe remote node to local node at custom position.
--      Should be used when changing provider for existing node.
--
-- Parameters:
--      i_node_name - set name
--      i_remote_node_name - node name
--      i_remote_worker_name - consumer name
--      i_custom_tick_id - tick id [optional]
--
-- Returns:
--      ret_code - error code
--      ret_note - description
--      global_watermark - minimal watermark
-- ----------------------------------------------------------------------
declare
    n record;
    node_wm_name text;
    node_pos bigint;
begin
    select node_type into n
      from pgq_node.node_info where queue_name = i_queue_name
       for update;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    select last_tick into global_watermark
      from pgq.get_consumer_info(i_queue_name, '.global_watermark');

    if n.node_type not in ('root', 'branch') then
        select 401, 'Cannot subscribe to ' || n.node_type || ' node'
          into ret_code, ret_note;
        return;
    end if;

    node_wm_name := '.' || i_remote_node_name || '.watermark';
    node_pos := coalesce(i_custom_tick_id, global_watermark);

    perform pgq.register_consumer_at(i_queue_name, node_wm_name, global_watermark);

    perform pgq.register_consumer_at(i_queue_name, i_remote_worker_name, node_pos);

    insert into pgq_node.subscriber_info (queue_name, subscriber_node, worker_name, watermark_name)
        values (i_queue_name, i_remote_node_name, i_remote_worker_name, node_wm_name);

    select 200, 'Subscriber registered: '||i_remote_node_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.unregister_subscriber(
    in i_queue_name text,
    in i_remote_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.unregister_subscriber(2)
--
--      Unsubscribe remote node from local node.
--
-- Parameters:
--      i_queue_name - set name
--      i_remote_node_name - node name
--
-- Returns:
--      ret_code - error code
--      ret_note - description
-- ----------------------------------------------------------------------
declare
    n_wm_name text;
    worker_name text;
begin
    n_wm_name := '.' || i_remote_node_name || '.watermark';
    select s.worker_name into worker_name from pgq_node.subscriber_info s
        where queue_name = i_queue_name and subscriber_node = i_remote_node_name;
    if not found then
        select 304, 'Subscriber not found' into ret_code, ret_note;
        return;
    end if;

    delete from pgq_node.subscriber_info
        where queue_name = i_queue_name
            and subscriber_node = i_remote_node_name;

    perform pgq.unregister_consumer(i_queue_name, n_wm_name);
    perform pgq.unregister_consumer(i_queue_name, worker_name);

    select 200, 'Subscriber unregistered: '||i_remote_node_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.set_subscriber_watermark(
    in i_queue_name text,
    in i_node_name text,
    in i_watermark bigint,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_subscriber_watermark(3)
--
--      Notify provider about subscribers lowest watermark.
--
--      Called on provider at interval by each worker  
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_node_name - subscriber node name
--      i_watermark - tick_id
--
-- Returns:
--      ret_code    - error code
--      ret_note    - description
-- ----------------------------------------------------------------------
declare
    n       record;
    wm_name text;
begin
    wm_name := '.' || i_node_name || '.watermark';
    select * into n from pgq.get_consumer_info(i_queue_name, wm_name);
    if not found then
        select 404, 'node '||i_node_name||' not subscribed to queue ', i_queue_name
            into ret_code, ret_note;
        return;
    end if;

    -- todo: check if wm sane?
    if i_watermark < n.last_tick then
        select 405, 'watermark must not be moved backwards'
            into ret_code, ret_note;
        return;
    elsif i_watermark = n.last_tick then
        select 100, 'watermark already set'
            into ret_code, ret_note;
        return;
    end if;

    perform pgq.register_consumer_at(i_queue_name, wm_name, i_watermark);

    select 200, wm_name || ' set to ' || i_watermark::text
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




-- Group: Subscriber side operations - worker


create or replace function pgq_node.get_worker_state(
    in i_queue_name text,

    out ret_code int4,
    out ret_note text,

    out node_type text,
    out node_name text,
    out completed_tick bigint,
    out provider_node text,
    out provider_location text,
    out paused boolean,
    out uptodate boolean,
    out cur_error text,

    out worker_name text,
    out global_watermark bigint,
    out local_watermark bigint,
    out local_queue_top bigint,
    out combined_queue text,
    out combined_type text
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_worker_state(1)
--
--      Get info for consumer that maintains local node.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      node_type - local node type
--      node_name - local node name
--      completed_tick - last committed tick
--      provider_node - provider node name
--      provider_location - connect string to provider node
--      paused - this node should not do any work
--      uptodate - if consumer has loaded last changes
--      cur_error - failure reason

--      worker_name - consumer name that maintains this node
--      global_watermark - queue's global watermark
--      local_watermark - queue's local watermark, for this and below nodes
--      local_queue_top - last tick in local queue
--      combined_queue - queue name for target set
--      combined_type - node type of target setA
-- ----------------------------------------------------------------------
begin
    select n.node_type, n.node_name, n.worker_name, n.combined_queue
      into node_type, node_name, worker_name, combined_queue
      from pgq_node.node_info n
     where n.queue_name = i_queue_name;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;
    select s.last_tick_id, s.provider_node, s.paused, s.uptodate, s.cur_error
      into completed_tick, provider_node, paused, uptodate, cur_error
      from pgq_node.local_state s
     where s.queue_name = i_queue_name
       and s.consumer_name = worker_name;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || worker_name
          into ret_code, ret_note;
        return;
    end if;
    select 100, 'Ok', p.node_location
      into ret_code, ret_note, provider_location
      from pgq_node.node_location p
     where p.queue_name = i_queue_name
      and p.node_name = provider_node;
    if not found then
        select 404, 'Unknown provider node: ' || i_queue_name || '/' || provider_node
          into ret_code, ret_note;
        return;
    end if;

    if combined_queue is not null then
        select n.node_type into combined_type
          from pgq_node.node_info n
         where n.queue_name = get_worker_state.combined_queue;
        if not found then
            select 404, 'Combinde queue node not found: ' || combined_queue
              into ret_code, ret_note;
            return;
        end if;
    end if;

    if node_type in ('root', 'branch') then
        select min(case when consumer_name = '.global_watermark' then null else last_tick end),
               min(case when consumer_name = '.global_watermark' then last_tick else null end)
          into local_watermark, global_watermark
          from pgq.get_consumer_info(i_queue_name);
        if local_watermark is null then
            select t.tick_id into local_watermark
              from pgq.tick t, pgq.queue q
             where t.tick_queue = q.queue_id
               and q.queue_name = i_queue_name
             order by 1 desc
             limit 1;
        end if;

        select tick_id from pgq.tick t, pgq.queue q
         where q.queue_name = i_queue_name
           and t.tick_queue = q.queue_id
         order by t.tick_queue desc, t.tick_id desc
         limit 1 into local_queue_top;
    else
        local_watermark := completed_tick;
    end if;

    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.set_global_watermark(
    in i_queue_name text,
    in i_watermark bigint,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_global_watermark(2)
--
--      Move global watermark on branch/leaf, publish on root.
--
-- Parameters:
--      i_queue_name    - queue name
--      i_watermark     - global tick_id that is processed everywhere.
--                        NULL on root, then local wm is published.
-- ----------------------------------------------------------------------
declare
    this        record;
    _wm         bigint;
    wm_consumer text;
begin
    wm_consumer = '.global_watermark';

    select node_type, queue_name, worker_name into this
        from pgq_node.node_info
        where queue_name = i_queue_name
        for update;
    if not found then
        select 404, 'Queue' || i_queue_name || ' not found'
          into ret_code, ret_note;
        return;
    end if;

    _wm = i_watermark;
    if this.node_type = 'root' then
        if i_watermark is null then
            select f.ret_code, f.ret_note, f.local_watermark
                into ret_code, ret_note, _wm
                from pgq_node.get_node_info(i_queue_name) f;
            if ret_code >= 300 then
                return;
            end if;
            if _wm is null then
                raise exception 'local_watermark=NULL from get_node_info()?';
            end if;
        end if;

        -- move watermark
        perform pgq.register_consumer_at(i_queue_name, wm_consumer, _wm);

        -- send event downstream
        perform pgq.insert_event(i_queue_name, 'pgq.global-watermark', _wm::text,
                                 i_queue_name, null, null, null);
        -- update root workers pos to avoid it getting stale
        update pgq_node.local_state
            set last_tick_id = _wm
            where queue_name = i_queue_name
                and consumer_name = this.worker_name;
    elsif this.node_type = 'branch' then
        if i_watermark is null then
            select 500, 'bad usage: wm=null on branch node'
                into ret_code, ret_note;
            return;
        end if;

        -- tick can be missing if we are processing
        -- old batches that set watermark outside
        -- current range
        perform 1 from pgq.tick t, pgq.queue q
          where q.queue_name = i_queue_name
            and t.tick_queue = q.queue_id
            and t.tick_id = _wm;
        if not found then
            select 200, 'Skipping global watermark update to ' || _wm::text
                into ret_code, ret_note;
            return;
        end if;

        -- move watermark
        perform pgq.register_consumer_at(i_queue_name, wm_consumer, _wm);
    else
        select 100, 'Ignoring global watermark in leaf'
            into ret_code, ret_note;
        return;
    end if;

    select 200, 'Global watermark set to ' || _wm::text
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;





create or replace function pgq_node.set_partition_watermark(
    in i_combined_queue_name text,
    in i_part_queue_name text,
    in i_watermark bigint,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_partition_watermark(3)
--
--      Move merge-leaf position on combined-branch.
--
-- Parameters:
--      i_combined_queue_name - local combined queue name
--      i_part_queue_name     - local part queue name (merge-leaf)
--      i_watermark         - partition tick_id that came inside combined-root batch
--
-- Returns:
--      200 - success
--      201 - no partition queue
--      401 - worker registration not found
-- ----------------------------------------------------------------------
declare
    n record;
begin
    -- check if combined-branch exists
    select c.node_type, p.worker_name into n
        from pgq_node.node_info c, pgq_node.node_info p
        where p.queue_name = i_part_queue_name
          and c.queue_name = i_combined_queue_name
          and p.combined_queue = c.queue_name
          and p.node_type = 'leaf'
          and c.node_type = 'branch';
    if not found then
        select 201, 'Part-queue does not exist' into ret_code, ret_note;
        return;
    end if;

    update pgq_node.local_state
       set last_tick_id = i_watermark
     where queue_name = i_part_queue_name
       and consumer_name = n.worker_name;
    if not found then
        select 401, 'Worker registration not found' into ret_code, ret_note;
        return;
    end if;

    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




-- Group: Subscriber side operations - any consumer


create or replace function pgq_node.register_consumer(
    in i_queue_name text,
    in i_consumer_name text,
    in i_provider_node text,
    in i_custom_tick_id int8,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.register_consumer(4)
--
--      Subscribe plain cascaded consumer to a target node.
--      That means it's planning to read from remote node
--      and write to local node.
--
-- Parameters:
--      i_queue_name - set name
--      i_consumer_name - cascaded consumer name
--      i_provider_node - node name
--      i_custom_tick_id - tick id
--
-- Returns:
--      ret_code - error code
--      200 - ok
--      201 - already registered
--      401 - no such queue
--      ret_note - description
-- ----------------------------------------------------------------------
declare
    n record;
    node_wm_name text;
    node_pos bigint;
begin
    select node_type into n
      from pgq_node.node_info where queue_name = i_queue_name
       for update;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    perform 1 from pgq_node.local_state
      where queue_name = i_queue_name
        and consumer_name = i_consumer_name;
    if found then
        update pgq_node.local_state
           set provider_node = i_provider_node,
               last_tick_id = i_custom_tick_id
         where queue_name = i_queue_name
           and consumer_name = i_consumer_name;
        select 201, 'Consumer already registered: ' || i_queue_name
               || '/' || i_consumer_name  into ret_code, ret_note;
        return;
    end if;

    insert into pgq_node.local_state (queue_name, consumer_name, provider_node, last_tick_id)
           values (i_queue_name, i_consumer_name, i_provider_node, i_custom_tick_id);

    select 200, 'Consumer '||i_consumer_name||' registered on queue '||i_queue_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.unregister_consumer(
    in i_queue_name text,
    in i_consumer_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.unregister_consumer(2)
--
--      Unregister cascaded consumer from local node.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_consumer_name - cascaded consumer name
--
-- Returns:
--      ret_code - error code
--      200 - ok
--      404 - no such queue
--      ret_note - description
-- ----------------------------------------------------------------------
begin
    perform 1 from pgq_node.node_info where queue_name = i_queue_name
       for update;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    delete from pgq_node.local_state
      where queue_name = i_queue_name
        and consumer_name = i_consumer_name;

    select 200, 'Consumer '||i_consumer_name||' unregistered from '||i_queue_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.get_consumer_state(
    in i_queue_name text,
    in i_consumer_name text,

    out ret_code int4,
    out ret_note text,

    out node_type text,
    out node_name text,
    out completed_tick bigint,
    out provider_node text,
    out provider_location text,
    out paused boolean,
    out uptodate boolean,
    out cur_error text
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_consumer_state(2)
--
--      Get info for cascaded consumer that targets local node.
--
-- Parameters:
--      i_node_name  - cascaded queue name
--      i_consumer_name - cascaded consumer name
--
-- Returns:
--      node_type - local node type
--      node_name - local node name
--      completed_tick - last committed tick
--      provider_node - provider node name
--      provider_location - connect string to provider node
--      paused - this node should not do any work
--      uptodate - if consumer has loaded last changes
--      cur_error - failure reason
-- ----------------------------------------------------------------------
begin
    select n.node_type, n.node_name
      into node_type, node_name
      from pgq_node.node_info n
    where n.queue_name = i_queue_name;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;
    select s.last_tick_id, s.provider_node, s.paused, s.uptodate, s.cur_error
      into completed_tick, provider_node, paused, uptodate, cur_error
      from pgq_node.local_state s
     where s.queue_name = i_queue_name
       and s.consumer_name = i_consumer_name;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
        return;
    end if;
    select 100, 'Ok', p.node_location
      into ret_code, ret_note, provider_location
      from pgq_node.node_location p
     where p.queue_name = i_queue_name
      and p.node_name = provider_node;
    if not found then
        select 404, 'Unknown provider node: ' || i_queue_name || '/' || provider_node
          into ret_code, ret_note;
        return;
    end if;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.change_consumer_provider(
    in i_queue_name text,
    in i_consumer_name text,
    in i_new_provider text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.change_consumer_provider(3)
--
--      Change provider for this consumer.
--
-- Parameters:
--      i_queue_name  - queue name
--      i_consumer_name  - consumer name
--      i_new_provider - node name for new provider
-- Returns:
--      ret_code - error code
--      200 - ok
--      404 - no such consumer or new node
--      ret_note - description
-- ----------------------------------------------------------------------
begin
    perform 1 from pgq_node.node_location
      where queue_name = i_queue_name
        and node_name = i_new_provider;
    if not found then
        select 404, 'New node not found: ' || i_new_provider
          into ret_code, ret_note;
        return;
    end if;

    update pgq_node.local_state
       set provider_node = i_new_provider,
           uptodate = false
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
        return;
    end if;
    select 200, 'Consumer provider node set to : ' || i_new_provider
      into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;




create or replace function pgq_node.set_consumer_uptodate(
    in i_queue_name text,
    in i_consumer_name text,
    in i_uptodate boolean,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_uptodate(3)
--
--      Set consumer uptodate flag.....
--
-- Parameters:
--      i_queue_name - queue name
--      i_consumer_name - consumer name
--      i_uptodate - new flag state
--
-- Returns:
--      200 - ok
--      404 - consumer not known
-- ----------------------------------------------------------------------
begin
    update pgq_node.local_state
       set uptodate = i_uptodate
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if found then
        select 200, 'Consumer uptodate = ' || i_uptodate::int4::text
               into ret_code, ret_note;
    else
        select 404, 'Consumer not known: '
               || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;





create or replace function pgq_node.set_consumer_paused(
    in i_queue_name text,
    in i_consumer_name text,
    in i_paused boolean,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_paused(3)
--
--      Set consumer paused flag.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_consumer_name - cascaded consumer name
--      i_paused   - new flag state
-- Returns:
--      200 - ok
--      201 - already paused
--      404 - consumer not found
-- ----------------------------------------------------------------------
declare
    old_flag    boolean;
    word        text;
begin
    if i_paused then
        word := 'paused';
    else
        word := 'resumed';
    end if;

    select paused into old_flag
        from pgq_node.local_state
        where queue_name = i_queue_name
          and consumer_name = i_consumer_name
        for update;
    if not found then
        select 404, 'Unknown consumer: ' || i_consumer_name
            into ret_code, ret_note;
    elsif old_flag = i_paused then
        select 201, 'Consumer ' || i_consumer_name || ' already ' || word
            into ret_code, ret_note;
    else
        update pgq_node.local_state
            set paused = i_paused,
                uptodate = false
            where queue_name = i_queue_name
            and consumer_name = i_consumer_name;

        select 200, 'Consumer '||i_consumer_name||' tagged as '||word into ret_code, ret_note;
    end if;
    return;

end;
$$ language plpgsql security definer;





create or replace function pgq_node.set_consumer_completed(
    in i_queue_name text,
    in i_consumer_name text,
    in i_tick_id int8,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_completed(3)
--
--      Set last completed tick id for the cascaded consumer
--      that it has committed to local node.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_consumer_name - cascaded consumer name
--      i_tick_id   - tick id
-- Returns:
--      200 - ok
--      404 - consumer not known
-- ----------------------------------------------------------------------
begin
    update pgq_node.local_state
       set last_tick_id = i_tick_id,
           cur_error = NULL
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if found then
        select 100, 'Consumer ' || i_consumer_name || ' completed tick = ' || i_tick_id::text
            into ret_code, ret_note;
    else
        select 404, 'Consumer not known: '
               || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;





create or replace function pgq_node.set_consumer_error(
    in i_queue_name text,
    in i_consumer_name text,
    in i_error_msg text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_error(3)
--
--      If batch processing fails, consumer can store it's last error in db.
-- Returns:
--      100 - ok
--      101 - consumer not known
-- ----------------------------------------------------------------------
begin
    update pgq_node.local_state
       set cur_error = i_error_msg
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if found then
        select 100, 'Consumer ' || i_consumer_name || ' error = ' || i_error_msg
            into ret_code, ret_note;
    else
        select 101, 'Consumer not known, ignoring: '
               || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;




-- Group: Maintenance operations


create or replace function pgq_node.maint_watermark(i_queue_name text)
returns int4 as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.maint_watermark(1)
--
--      Move global watermark on root node.
--
-- Returns:
--      0 - tells pgqd to call just once
-- ----------------------------------------------------------------------
declare
    _lag interval;
begin
    perform 1 from pgq_node.node_info
      where queue_name = i_queue_name
        and node_type = 'root'
      for update;
    if not found then
        return 0;
    end if;

    select lag into _lag from pgq.get_consumer_info(i_queue_name, '.global_watermark');
    if _lag >= '5 minutes'::interval then
        perform pgq_node.set_global_watermark(i_queue_name, NULL);
    end if;

    return 0;
end;
$$ language plpgsql;




create or replace function pgq_node.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.version(0)
--
--      Returns version string for pgq_node.
-- ----------------------------------------------------------------------
declare
    _vers text;
begin
    select extversion from pg_catalog.pg_extension
        where extname = 'pgq_node' into _vers;
    return _vers;
end;
$$ language plpgsql;






grant usage on schema pgq_node to public;






-- 1.public.fns --
REVOKE ALL ON FUNCTION pgq_node.is_root_node(text),
    pgq_node.is_leaf_node(text),
    pgq_node.version()
  FROM pgq_writer, pgq_admin, pgq_reader, public CASCADE;

-- 2.consumer.fns --
REVOKE ALL ON FUNCTION pgq_node.register_consumer(text, text, text, int8),
    pgq_node.unregister_consumer(text, text),
    pgq_node.change_consumer_provider(text, text, text),
    pgq_node.set_consumer_uptodate(text, text, boolean),
    pgq_node.set_consumer_paused(text, text, boolean),
    pgq_node.set_consumer_completed(text, text, int8),
    pgq_node.set_consumer_error(text, text, text)
  FROM pgq_writer, pgq_admin, pgq_reader, public CASCADE;
REVOKE ALL ON FUNCTION pgq_node.register_consumer(text, text, text, int8),
    pgq_node.unregister_consumer(text, text),
    pgq_node.change_consumer_provider(text, text, text),
    pgq_node.set_consumer_uptodate(text, text, boolean),
    pgq_node.set_consumer_paused(text, text, boolean),
    pgq_node.set_consumer_completed(text, text, int8),
    pgq_node.set_consumer_error(text, text, text)
  FROM public CASCADE;

-- 3.worker.fns --
REVOKE ALL ON FUNCTION pgq_node.create_node(text, text, text, text, text, bigint, text),
    pgq_node.drop_node(text, text),
    pgq_node.demote_root(text, int4, text),
    pgq_node.promote_branch(text),
    pgq_node.set_node_attrs(text, text),
    pgq_node.get_worker_state(text),
    pgq_node.set_global_watermark(text, bigint),
    pgq_node.set_partition_watermark(text, text, bigint)
  FROM pgq_writer, pgq_admin, pgq_reader, public CASCADE;
REVOKE ALL ON FUNCTION pgq_node.create_node(text, text, text, text, text, bigint, text),
    pgq_node.drop_node(text, text),
    pgq_node.demote_root(text, int4, text),
    pgq_node.promote_branch(text),
    pgq_node.set_node_attrs(text, text),
    pgq_node.get_worker_state(text),
    pgq_node.set_global_watermark(text, bigint),
    pgq_node.set_partition_watermark(text, text, bigint)
  FROM public CASCADE;

-- 4.admin.fns --
REVOKE ALL ON FUNCTION pgq_node.register_location(text, text, text, boolean),
    pgq_node.unregister_location(text, text),
    pgq_node.upgrade_schema(),
    pgq_node.maint_watermark(text)
  FROM pgq_writer, pgq_admin, pgq_reader, public CASCADE;
REVOKE ALL ON FUNCTION pgq_node.register_location(text, text, text, boolean),
    pgq_node.unregister_location(text, text),
    pgq_node.upgrade_schema(),
    pgq_node.maint_watermark(text)
  FROM public CASCADE;

-- 4.remote.fns --
REVOKE ALL ON FUNCTION pgq_node.get_consumer_info(text),
    pgq_node.get_consumer_state(text, text),
    pgq_node.get_queue_locations(text),
    pgq_node.get_node_info(text),
    pgq_node.get_subscriber_info(text),
    pgq_node.register_subscriber(text, text, text, int8),
    pgq_node.unregister_subscriber(text, text),
    pgq_node.set_subscriber_watermark(text, text, bigint)
  FROM pgq_writer, pgq_admin, pgq_reader, public CASCADE;
REVOKE ALL ON FUNCTION pgq_node.get_consumer_info(text),
    pgq_node.get_consumer_state(text, text),
    pgq_node.get_queue_locations(text),
    pgq_node.get_node_info(text),
    pgq_node.get_subscriber_info(text),
    pgq_node.register_subscriber(text, text, text, int8),
    pgq_node.unregister_subscriber(text, text),
    pgq_node.set_subscriber_watermark(text, text, bigint)
  FROM public CASCADE;

-- 5.tables --
REVOKE ALL ON TABLE pgq_node.node_location, pgq_node.node_info, pgq_node.local_state, pgq_node.subscriber_info
  FROM pgq_writer, pgq_admin, pgq_reader, public CASCADE;

-- 1.public.fns --
GRANT execute ON FUNCTION pgq_node.is_root_node(text),
    pgq_node.is_leaf_node(text),
    pgq_node.version()
  TO public;

-- 2.consumer.fns --
GRANT execute ON FUNCTION pgq_node.register_consumer(text, text, text, int8),
    pgq_node.unregister_consumer(text, text),
    pgq_node.change_consumer_provider(text, text, text),
    pgq_node.set_consumer_uptodate(text, text, boolean),
    pgq_node.set_consumer_paused(text, text, boolean),
    pgq_node.set_consumer_completed(text, text, int8),
    pgq_node.set_consumer_error(text, text, text)
  TO pgq_writer;
GRANT execute ON FUNCTION pgq_node.register_consumer(text, text, text, int8),
    pgq_node.unregister_consumer(text, text),
    pgq_node.change_consumer_provider(text, text, text),
    pgq_node.set_consumer_uptodate(text, text, boolean),
    pgq_node.set_consumer_paused(text, text, boolean),
    pgq_node.set_consumer_completed(text, text, int8),
    pgq_node.set_consumer_error(text, text, text)
  TO pgq_admin;

-- 3.worker.fns --
GRANT execute ON FUNCTION pgq_node.create_node(text, text, text, text, text, bigint, text),
    pgq_node.drop_node(text, text),
    pgq_node.demote_root(text, int4, text),
    pgq_node.promote_branch(text),
    pgq_node.set_node_attrs(text, text),
    pgq_node.get_worker_state(text),
    pgq_node.set_global_watermark(text, bigint),
    pgq_node.set_partition_watermark(text, text, bigint)
  TO pgq_admin;

-- 4.admin.fns --
GRANT execute ON FUNCTION pgq_node.register_location(text, text, text, boolean),
    pgq_node.unregister_location(text, text),
    pgq_node.upgrade_schema(),
    pgq_node.maint_watermark(text)
  TO pgq_admin;

-- 4.remote.fns --
GRANT execute ON FUNCTION pgq_node.get_consumer_info(text),
    pgq_node.get_consumer_state(text, text),
    pgq_node.get_queue_locations(text),
    pgq_node.get_node_info(text),
    pgq_node.get_subscriber_info(text),
    pgq_node.register_subscriber(text, text, text, int8),
    pgq_node.unregister_subscriber(text, text),
    pgq_node.set_subscriber_watermark(text, text, bigint)
  TO pgq_writer;
GRANT execute ON FUNCTION pgq_node.get_consumer_info(text),
    pgq_node.get_consumer_state(text, text),
    pgq_node.get_queue_locations(text),
    pgq_node.get_node_info(text),
    pgq_node.get_subscriber_info(text),
    pgq_node.register_subscriber(text, text, text, int8),
    pgq_node.unregister_subscriber(text, text),
    pgq_node.set_subscriber_watermark(text, text, bigint)
  TO pgq_admin;
GRANT execute ON FUNCTION pgq_node.get_consumer_info(text),
    pgq_node.get_consumer_state(text, text),
    pgq_node.get_queue_locations(text),
    pgq_node.get_node_info(text),
    pgq_node.get_subscriber_info(text),
    pgq_node.register_subscriber(text, text, text, int8),
    pgq_node.unregister_subscriber(text, text),
    pgq_node.set_subscriber_watermark(text, text, bigint)
  TO pgq_reader;

-- 5.tables --
GRANT select ON TABLE pgq_node.node_location, pgq_node.node_info, pgq_node.local_state, pgq_node.subscriber_info
  TO pgq_writer;
GRANT select, insert, update, delete ON TABLE pgq_node.node_location, pgq_node.node_info, pgq_node.local_state, pgq_node.subscriber_info
  TO pgq_admin;
GRANT select ON TABLE pgq_node.node_location, pgq_node.node_info, pgq_node.local_state, pgq_node.subscriber_info
  TO pgq_reader;

