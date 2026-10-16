-- ============================================================================
-- purge_match.sql — delete one match (combo_id) from every raw table.
-- ============================================================================
-- Use for matches that should never have been loaded (cancelled / abandoned /
-- postponed — discovery now skips these) or to wipe a match before a clean
-- reload. Run as the warehouse owner, then rebuild dbt:
--
--   docker exec -i the_data_xi_postgres psql -U the_data_xi -d the_data_xi \
--     -v combo=2026-10-10RayAth -f - < include/scripts/purge_match.sql
-- ============================================================================
\set ON_ERROR_STOP on
select set_config('purge.combo', :'combo', false);

do $$
declare t text; n bigint; total bigint := 0;
begin
  for t in
    select table_name from information_schema.columns
    where table_schema = 'raw' and column_name = 'combo_id'
    order by table_name
  loop
    execute format('delete from raw.%I where combo_id = %L', t, current_setting('purge.combo'));
    get diagnostics n = row_count;
    if n > 0 then raise notice '%: % rows', t, n; end if;
    total := total + n;
  end loop;
  raise notice 'purged % rows for %', total, current_setting('purge.combo');
end $$;
