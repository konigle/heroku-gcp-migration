DO $$
DECLARE
    fk_table regclass;
    fk_conname text;
    cmd text;
BEGIN
    FOR fk_table, fk_conname IN
        SELECT ontable, conname
            FROM heroku_migration.all_user_foreign_keys
            WHERE state = 'not valid'
    LOOP
        cmd := format('ALTER TABLE %I VALIDATE CONSTRAINT %I', fk_table, fk_conname);
        RAISE WARNING 'cmd %: %', clock_timestamp(), cmd;
        EXECUTE cmd;
        UPDATE heroku_migration.all_user_foreign_keys
            SET state = 'valid'
        WHERE ontable = fk_table;
        COMMIT;
    END LOOP;
END; $$;