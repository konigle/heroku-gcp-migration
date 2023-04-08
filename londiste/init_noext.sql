\i pgq_pl_only.sql
\i pgq_node.sql
\i londiste.sql
CREATE OR REPLACE FUNCTION pgq.version()
RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    RETURN '3.4.1';
END;
$$;

