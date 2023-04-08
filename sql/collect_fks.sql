-- ----------------------------------------------------------------------
-- creating new schema so that we can cleanup easily
-- ----------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS heroku_migration;

-- ----------------------------------------------------------------------
-- collect all the active foreign keys for the user defined tables
-- and store in a new table. This will be used to restore the FKs after
-- the migration is complete
-- ----------------------------------------------------------------------
CREATE TABLE heroku_migration.all_user_foreign_keys AS
SELECT conrelid::pg_catalog.regclass AS ontable, conname,
       pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
       'active'::text AS state
FROM pg_catalog.pg_constraint r
         JOIN pg_class c ON c.oid = r.conrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE r.contype = 'f'
  AND n.nspname NOT LIKE 'pg_%'
  AND n.nspname NOT IN ('heroku_ext', 'information_schema', 'pgq', 'pgq_node', 'londiste')
;