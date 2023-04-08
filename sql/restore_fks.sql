DO $$
DECLARE
    fk_table regclass;
    fk_conname text;
    fk_condef text;
    cmd text;
BEGIN
    FOR fk_table, fk_conname, fk_condef IN
      SELECT ontable, conname, condef
        FROM heroku_migration.all_user_foreign_keys
    LOOP
        cmd := format('ALTER TABLE %I ADD CONSTRAINT %I %s NOT VALID', fk_table, fk_conname, fk_condef);
        RAISE WARNING 'cmd %: %', clock_timestamp(), cmd;
        EXECUTE cmd;
        UPDATE heroku_migration.all_user_foreign_keys
            SET state = 'not valid'
        WHERE ontable = fk_table;
        COMMIT;
    END LOOP;
END;
$$;