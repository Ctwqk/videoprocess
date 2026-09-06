#!/usr/bin/env bash
set -euo pipefail

# Rehearsal only: a fresh disposable PG16 container, never a DB URL.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT_DIR/deploy/swarm/bootstrap-deploy-migrator.sql"
CONTAINER=vp-codex-bootstrap-closeout
remote() { ssh -o BatchMode=yes -o ConnectTimeout=10 -J 10.0.0.127 10.0.0.150 "$@"; }
psql_test() {
  remote "docker exec -i $CONTAINER psql -X -v ON_ERROR_STOP=1 -At -U ${2:-vp} -d ${1:-videoprocess}"
}
remote "docker run -d --name $CONTAINER --label vp.codex.purpose=bootstrap-closeout --network none --tmpfs /var/lib/postgresql/data:rw -e POSTGRES_USER=vp -e POSTGRES_DB=videoprocess -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16" >/dev/null
cleanup() { remote "docker rm -f $CONTAINER" >/dev/null; }
trap cleanup EXIT
for attempt in {1..30}; do
  if remote "docker exec $CONTAINER pg_isready -U vp -d videoprocess" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
[[ "$(psql_test <<<"SELECT current_setting('server_version_num')::int / 10000;")" == 16 ]]
[[ "$(psql_test <<<"SELECT oid FROM pg_roles WHERE rolname = 'vp';")" == 10 ]]
[[ "$(psql_test <<<"SELECT count(*) FROM pg_roles WHERE rolname = 'vp_deploy_migrator';")" == 0 ]]

psql_test >/dev/null <<'SQL'
CREATE ROLE vp_deploy_migrator LOGIN INHERIT CREATEROLE;
CREATE ROLE vp_control_role_owner LOGIN INHERIT CREATEROLE;
CREATE ROLE fixture_unrelated;
CREATE ROLE fixture_downstream;
GRANT vp TO vp_deploy_migrator WITH ADMIN FALSE;
GRANT vp TO vp_deploy_migrator WITH INHERIT TRUE;
GRANT vp TO vp_deploy_migrator WITH SET FALSE;
DO $$
DECLARE name text; i int;
BEGIN
  FOREACH name IN ARRAY ARRAY[
    'vp_worker_operator_runtime', 'vp_orchestrator_control_runtime',
    'vp_staging_janitor_runtime', 'vp_marker_readiness_runtime',
    'vp_marker_janitor_runtime', 'vp_marker_repair_runtime'
  ] LOOP
    EXECUTE format('CREATE ROLE %I NOLOGIN NOINHERIT', name);
    IF name LIKE 'vp_marker_%' THEN
      EXECUTE format('GRANT %I TO vp_deploy_migrator WITH ADMIN TRUE', name);
      EXECUTE format('GRANT %I TO vp_deploy_migrator WITH INHERIT FALSE', name);
      EXECUTE format('GRANT %I TO vp_deploy_migrator WITH SET FALSE', name);
    END IF;
  END LOOP;
  FOR i IN 1..3 LOOP
    EXECUTE format('CREATE TABLE public.%I (id integer)', 'source table ' || i);
  END LOOP;
  FOR i IN 1..4 LOOP
    EXECUTE format('CREATE INDEX %I ON public.%I (id)', 'source_idx_' || i,
                   'source table ' || (1 + (i - 1) % 3));
  END LOOP;
  FOR i IN 1..2 LOOP
    EXECUTE format('CREATE TABLE public.%I (id integer)', 'deploy_table_' || i);
    EXECUTE format('ALTER TABLE public.%I OWNER TO vp_deploy_migrator', 'deploy_table_' || i);
  END LOOP;
  FOR i IN 1..3 LOOP
    EXECUTE format('CREATE INDEX %I ON public.%I (id)', 'deploy_idx_' || i,
                   'deploy_table_' || (1 + (i - 1) % 2));
  END LOOP;
END $$;
SET ROLE vp_deploy_migrator;
DO $$
DECLARE name text;
BEGIN
  FOREACH name IN ARRAY ARRAY['vp_marker_readiness_runtime',
    'vp_marker_janitor_runtime', 'vp_marker_repair_runtime'] LOOP
    EXECUTE format('GRANT %I TO vp_control_role_owner WITH ADMIN TRUE', name);
    EXECUTE format('GRANT %I TO vp_control_role_owner WITH INHERIT FALSE', name);
    EXECUTE format('GRANT %I TO vp_control_role_owner WITH SET FALSE', name);
  END LOOP;
END $$;
RESET ROLE;
CREATE TYPE public.artifact_kind AS ENUM ('fixture');
CREATE TYPE public.job_status AS ENUM ('fixture');
CREATE TYPE public.node_status AS ENUM ('fixture');
CREATE FUNCTION public.autoflow_plan_authority_fence() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE FUNCTION public.fixture_deploy_function() RETURNS int
LANGUAGE sql AS $$ SELECT 1 $$;
ALTER FUNCTION public.fixture_deploy_function() OWNER TO vp_deploy_migrator;
CREATE SCHEMA fixture_untouched;
CREATE TABLE fixture_untouched.sentinel (id serial PRIMARY KEY);
CREATE FUNCTION fixture_untouched.sentinel() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;
CREATE TYPE fixture_untouched.sentinel_enum AS ENUM ('untouched');
SQL

# Snapshot only non-secret catalogs; failed runs must leave them byte-identical.
snapshot() {
  psql_test <<'SQL'
SELECT 'database', oid, datname, datdba FROM pg_database ORDER BY oid;
SELECT 'schema', oid, nspname, nspowner FROM pg_namespace ORDER BY oid;
SELECT 'relation', oid, relname, relowner, relacl FROM pg_class ORDER BY oid;
SELECT 'function', oid, proname, proowner, proacl FROM pg_proc ORDER BY oid;
SELECT 'type', oid, typname, typowner, typacl FROM pg_type ORDER BY oid;
SELECT 'membership', roleid, member, grantor, admin_option, inherit_option, set_option
FROM pg_auth_members ORDER BY roleid, member, grantor;
SELECT 'settings', setdatabase, setrole, setconfig FROM pg_db_role_setting ORDER BY 2, 3;
SELECT 'role', oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
rolcanlogin, rolreplication, rolbypassrls FROM pg_roles ORDER BY oid;
SQL
}
expect_failure() {
  local label="$1" expected="$2" database="${3:-videoprocess}" user="${4:-vp}"
  local before after output
  before="$(snapshot)"
  if output="$(psql_test "$database" "$user" <"$SCRIPT" 2>&1)"; then
    printf 'FAIL: accepted %s\n' "$label" >&2
    exit 1
  fi
  [[ "$output" == *"$expected"* ]] || { printf '%s\n' "$output" >&2; exit 1; }
  after="$(snapshot)"
  [[ "$before" == "$after" ]] || { printf 'FAIL: rollback: %s\n' "$label" >&2; exit 1; }
  printf 'PASS: %s; unchanged catalogs\n' "$label"
}

# Also gives a useful red result before the bootstrap implementation exists.
[[ -f "$SCRIPT" ]] || { printf 'FAIL: bootstrap SQL is missing\n' >&2; exit 1; }
expect_failure 'wrong database' 'bootstrap precondition' postgres
expect_failure 'wrong session user' 'bootstrap precondition' videoprocess vp_deploy_migrator
psql_test >/dev/null <<<"ALTER ROLE vp_worker_operator_runtime INHERIT;"
expect_failure 'stable role must be NOINHERIT' 'bootstrap precondition'
psql_test >/dev/null <<<"ALTER ROLE vp_worker_operator_runtime NOINHERIT;"
psql_test >/dev/null <<'SQL'
GRANT fixture_unrelated TO vp_deploy_migrator WITH INHERIT TRUE;
GRANT fixture_unrelated TO vp_deploy_migrator WITH SET FALSE;
SQL
expect_failure 'hidden inherited parent' 'bootstrap precondition'
psql_test >/dev/null <<'SQL'
GRANT fixture_unrelated TO vp_deploy_migrator WITH INHERIT FALSE;
GRANT fixture_unrelated TO vp_deploy_migrator WITH SET TRUE;
SQL
expect_failure 'hidden SET ROLE parent' 'bootstrap precondition'
psql_test >/dev/null <<<"REVOKE fixture_unrelated FROM vp_deploy_migrator;"
psql_test >/dev/null <<<"ALTER TYPE public.artifact_kind RENAME TO unexpected_enum;"
expect_failure 'enum drift' 'bootstrap precondition'
psql_test >/dev/null <<<"ALTER TYPE public.unexpected_enum RENAME TO artifact_kind;"
psql_test >/dev/null <<<"ALTER TABLE public.\"source table 1\" OWNER TO fixture_unrelated;"
expect_failure 'ownership drift' 'bootstrap precondition'
psql_test >/dev/null <<<"ALTER TABLE public.\"source table 1\" OWNER TO vp;"
psql_test >/dev/null <<<"GRANT vp_worker_operator_runtime TO fixture_unrelated;"
expect_failure 'unexpected control membership' 'bootstrap precondition'
psql_test >/dev/null <<<"REVOKE vp_worker_operator_runtime FROM fixture_unrelated;"
psql_test >/dev/null <<<"GRANT vp_marker_readiness_runtime TO vp_deploy_migrator WITH SET TRUE;"
expect_failure 'marker option drift' 'bootstrap precondition'
psql_test >/dev/null <<<"GRANT vp_marker_readiness_runtime TO vp_deploy_migrator WITH SET FALSE;"

# A dependent legacy grant makes REVOKE RESTRICT fail after ownership changes.
psql_test >/dev/null <<'SQL'
SET ROLE vp_control_role_owner;
GRANT vp_marker_readiness_runtime TO fixture_downstream WITH INHERIT FALSE;
RESET ROLE;
SQL
expect_failure 'late dependent-grant failure rolls back everything' 'dependent privileges exist'
psql_test >/dev/null <<'SQL'
SET ROLE vp_control_role_owner;
REVOKE vp_marker_readiness_runtime FROM fixture_downstream;
RESET ROLE;
SQL

psql_test <"$SCRIPT"
psql_test <<'SQL'
DO $$
DECLARE deploy oid := 'vp_deploy_migrator'::regrole;
BEGIN
  IF (SELECT datdba FROM pg_database WHERE datname = 'videoprocess') <> deploy
    OR (SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace
        AND relkind = 'r' AND relowner = deploy) <> 5
    OR (SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace
        AND relkind = 'i' AND relowner = deploy) <> 7
    OR EXISTS (SELECT FROM pg_proc WHERE pronamespace = 'public'::regnamespace AND proowner <> deploy)
    OR (SELECT count(*) FROM pg_type WHERE typnamespace = 'public'::regnamespace
        AND typtype = 'e' AND typowner = deploy) <> 3
  THEN RAISE EXCEPTION 'incorrect transferred ownership'; END IF;
  IF (SELECT count(*) FROM pg_auth_members WHERE member = deploy AND grantor = 10
      AND admin_option AND NOT inherit_option AND NOT set_option) <> 6
    OR EXISTS (SELECT FROM pg_auth_members WHERE roleid = 10 AND member = deploy)
    OR EXISTS (SELECT FROM pg_auth_members WHERE member = deploy AND (inherit_option OR set_option))
    OR EXISTS (SELECT FROM pg_auth_members WHERE member = 'vp_control_role_owner'::regrole)
  THEN RAISE EXCEPTION 'incorrect final memberships'; END IF;
  IF EXISTS (SELECT FROM pg_database WHERE datname IN ('postgres', 'template0', 'template1') AND datdba <> 10)
    OR (SELECT nspowner FROM pg_namespace WHERE nspname = 'public') <> 'pg_database_owner'::regrole
    OR EXISTS (SELECT FROM pg_class WHERE relnamespace = 'fixture_untouched'::regnamespace AND relowner <> 10)
    OR EXISTS (SELECT FROM pg_proc WHERE pronamespace = 'fixture_untouched'::regnamespace AND proowner <> 10)
    OR EXISTS (SELECT FROM pg_type WHERE typnamespace = 'fixture_untouched'::regnamespace AND typowner <> 10)
  THEN RAISE EXCEPTION 'out-of-scope ownership changed'; END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE oid = deploy AND rolcanlogin AND rolinherit
      AND rolcreaterole AND NOT rolsuper AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls)
  THEN RAISE EXCEPTION 'deploy attributes changed'; END IF;
END $$;
SELECT 'PASS: ownership, six canonical grants, legacy revokes, role attributes, untouched objects';
SQL
[[ "$(psql_test videoprocess vp_deploy_migrator <<<"SHOW lock_timeout;")" == 60s ]]
printf 'PASS: fresh deploy session lock_timeout=60s\n'
expect_failure 'one-shot rerun fails closed' 'bootstrap precondition'
