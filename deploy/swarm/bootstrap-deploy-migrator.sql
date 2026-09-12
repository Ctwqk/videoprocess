-- ONE-SHOT PG16 production bootstrap. Review and run manually, as the original
-- bootstrap login vp (OID 10), connected directly to database videoprocess:
--   psql -X --set=ON_ERROR_STOP=1 --dbname=videoprocess --username=vp --file=deploy/swarm/bootstrap-deploy-migrator.sql
-- Supply the approved connection outside this file. No credentials are read here.
-- Stop concurrent migrations/role provisioning for this maintenance transaction.
-- This accepts ONLY the inventoried pre-transfer state; a rerun fails closed.
-- Never use REASSIGN OWNED: vp also owns postgres/template databases.
\set ON_ERROR_STOP on

BEGIN;
SET LOCAL lock_timeout = '60s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog;

DO $preflight$
DECLARE
    deploy oid;
    marker text;
    marker_oid oid;
    legacy_owner oid;
BEGIN
    IF current_setting('server_version_num')::integer / 10000 <> 16
       OR current_database() <> 'videoprocess'
       OR session_user <> 'vp' OR current_user <> 'vp'
       OR NOT EXISTS (SELECT FROM pg_roles WHERE oid = 10 AND rolname = 'vp' AND rolsuper)
    THEN
        RAISE EXCEPTION 'bootstrap precondition: require PG16, videoprocess, session/current user vp (bootstrap OID 10)';
    END IF;

    SELECT oid INTO deploy FROM pg_roles
    WHERE rolname = 'vp_deploy_migrator' AND rolcanlogin AND rolinherit
      AND rolcreaterole AND NOT rolsuper AND NOT rolcreatedb
      AND NOT rolreplication AND NOT rolbypassrls;
    SELECT oid INTO legacy_owner FROM pg_roles WHERE rolname = 'vp_control_role_owner';
    IF deploy IS NULL OR legacy_owner IS NULL THEN
        RAISE EXCEPTION 'bootstrap precondition: missing roles or unsafe deploy attributes';
    END IF;
    IF (SELECT count(*) FROM pg_roles WHERE rolname IN (
        'vp_worker_operator_runtime', 'vp_orchestrator_control_runtime',
        'vp_staging_janitor_runtime', 'vp_marker_readiness_runtime',
        'vp_marker_janitor_runtime', 'vp_marker_repair_runtime')
        AND NOT rolcanlogin AND NOT rolinherit AND NOT rolsuper AND NOT rolcreatedb
        AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls) <> 6
    THEN
        RAISE EXCEPTION 'bootstrap precondition: missing or privileged stable runtime role';
    END IF;
    IF (SELECT count(*) FROM pg_auth_members WHERE roleid = 10 AND member = deploy) <> 1
       OR NOT EXISTS (SELECT FROM pg_auth_members WHERE roleid = 10 AND member = deploy
                      AND grantor = 10 AND NOT admin_option AND inherit_option AND NOT set_option)
    THEN
        RAISE EXCEPTION 'bootstrap precondition: vp -> deploy membership differs from inventory';
    END IF;
    IF EXISTS (SELECT FROM pg_auth_members WHERE member = deploy AND roleid <> 10
               AND (inherit_option OR set_option))
    THEN
        RAISE EXCEPTION 'bootstrap precondition: deploy has a usable parent other than vp';
    END IF;
    IF EXISTS (SELECT FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid
               WHERE r.rolname IN ('vp_worker_operator_runtime',
                   'vp_orchestrator_control_runtime', 'vp_staging_janitor_runtime'))
    THEN
        RAISE EXCEPTION 'bootstrap precondition: control runtime roles already have memberships';
    END IF;
    FOREACH marker IN ARRAY ARRAY['vp_marker_readiness_runtime',
        'vp_marker_janitor_runtime', 'vp_marker_repair_runtime'] LOOP
        SELECT oid INTO marker_oid FROM pg_roles WHERE rolname = marker;
        IF (SELECT count(*) FROM pg_auth_members
            WHERE roleid = marker_oid AND member = deploy) <> 1
           OR NOT EXISTS (SELECT FROM pg_auth_members WHERE roleid = marker_oid
               AND member = deploy AND grantor = 10
               AND admin_option AND NOT inherit_option AND NOT set_option)
           OR (SELECT count(*) FROM pg_auth_members
               WHERE roleid = marker_oid AND member = legacy_owner) <> 1
           OR NOT EXISTS (SELECT FROM pg_auth_members WHERE roleid = marker_oid
               AND member = legacy_owner AND grantor = deploy
               AND admin_option AND NOT inherit_option AND NOT set_option)
        THEN
            RAISE EXCEPTION 'bootstrap precondition: noncanonical marker memberships for %', marker;
        END IF;
    END LOOP;

    IF (SELECT datdba FROM pg_database WHERE datname = 'videoprocess') IS DISTINCT FROM 10::oid
       OR (SELECT nspowner FROM pg_namespace WHERE nspname = 'public')
          IS DISTINCT FROM 'pg_database_owner'::regrole::oid
    THEN
        RAISE EXCEPTION 'bootstrap precondition: database/schema ownership drift';
    END IF;
    -- Index ownership follows its table. Reject unsupported relation kinds
    -- and third-party owners; the pre-apply audit supplies the exact inventory.
    IF EXISTS (SELECT FROM pg_class WHERE relnamespace = 'public'::regnamespace
               AND (relkind NOT IN ('r', 'i') OR relowner NOT IN (10, deploy)))
       OR NOT EXISTS (SELECT FROM pg_class WHERE relnamespace = 'public'::regnamespace
                      AND relkind = 'r' AND relowner = 10)
    THEN
        RAISE EXCEPTION 'bootstrap precondition: public relation kind/owner outside allowlist or no vp tables';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_proc
        WHERE oid = to_regprocedure('public.autoflow_plan_authority_fence()')
          AND proowner = 10 AND prokind = 'f')
       OR EXISTS (SELECT FROM pg_proc WHERE pronamespace = 'public'::regnamespace
           AND oid <> to_regprocedure('public.autoflow_plan_authority_fence()') AND proowner <> deploy)
    THEN
        RAISE EXCEPTION 'bootstrap precondition: public function ownership drift';
    END IF;
    IF (SELECT count(*) FROM pg_type WHERE typnamespace = 'public'::regnamespace
        AND typtype = 'e') <> 3
       OR (SELECT count(*) FROM pg_type WHERE typnamespace = 'public'::regnamespace
           AND typtype = 'e' AND typowner = 10
           AND typname IN ('artifact_kind', 'job_status', 'node_status')) <> 3
       OR EXISTS (SELECT FROM pg_type WHERE typnamespace = 'public'::regnamespace
           AND typowner = 10 AND typrelid = 0 AND typelem = 0 AND typtype <> 'e')
    THEN
        RAISE EXCEPTION 'bootstrap precondition: standalone enum/type inventory drift';
    END IF;
END
$preflight$;

ALTER DATABASE videoprocess OWNER TO vp_deploy_migrator;
-- public remains owned by pg_database_owner, now implicitly the deploy role.
DO $tables$
DECLARE target record;
BEGIN
    FOR target IN SELECT relname FROM pg_class
        WHERE relnamespace = 'public'::regnamespace AND relkind = 'r' AND relowner = 10
        ORDER BY oid
    LOOP
        EXECUTE format('ALTER TABLE ONLY public.%I OWNER TO vp_deploy_migrator', target.relname);
    END LOOP;
END
$tables$;
ALTER FUNCTION public.autoflow_plan_authority_fence() OWNER TO vp_deploy_migrator;
ALTER TYPE public.artifact_kind OWNER TO vp_deploy_migrator;
ALTER TYPE public.job_status OWNER TO vp_deploy_migrator;
ALTER TYPE public.node_status OWNER TO vp_deploy_migrator;

-- Apply all three membership options atomically at COMMIT.
GRANT vp_worker_operator_runtime, vp_orchestrator_control_runtime, vp_staging_janitor_runtime
    TO vp_deploy_migrator WITH INHERIT FALSE GRANTED BY vp;
GRANT vp_worker_operator_runtime, vp_orchestrator_control_runtime, vp_staging_janitor_runtime
    TO vp_deploy_migrator WITH SET FALSE GRANTED BY vp;
GRANT vp_worker_operator_runtime, vp_orchestrator_control_runtime, vp_staging_janitor_runtime
    TO vp_deploy_migrator WITH ADMIN TRUE GRANTED BY vp;

-- Applies to fresh deploy sessions, including advisory-lock waits. SET ROLE
-- does not load role defaults, so this transaction also has SET LOCAL above.
ALTER ROLE vp_deploy_migrator SET lock_timeout = '60s';

-- Remove only the deploy-granted legacy edges, as their actual grantor.
-- RESTRICT deliberately aborts on dependent grants; never cascade here.
SET LOCAL ROLE vp_deploy_migrator;
REVOKE vp_marker_readiness_runtime, vp_marker_janitor_runtime, vp_marker_repair_runtime
    FROM vp_control_role_owner GRANTED BY vp_deploy_migrator RESTRICT;
RESET ROLE;

-- Last authority mutation: remove inherited bootstrap ownership/privileges.
REVOKE vp FROM vp_deploy_migrator GRANTED BY vp RESTRICT;

DO $postflight$
DECLARE deploy oid := 'vp_deploy_migrator'::regrole;
BEGIN
    IF current_user <> 'vp'
       OR (SELECT datdba FROM pg_database WHERE datname = 'videoprocess') <> deploy
       OR (SELECT nspowner FROM pg_namespace WHERE nspname = 'public') <> 'pg_database_owner'::regrole
       OR EXISTS (SELECT FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relowner <> deploy)
       OR EXISTS (SELECT FROM pg_proc WHERE pronamespace = 'public'::regnamespace AND proowner <> deploy)
       OR EXISTS (SELECT FROM pg_type WHERE typnamespace = 'public'::regnamespace AND typowner <> deploy)
       OR EXISTS (SELECT FROM pg_auth_members WHERE roleid = 10 AND member = deploy)
       OR EXISTS (SELECT FROM pg_auth_members WHERE member = deploy AND (inherit_option OR set_option))
       OR EXISTS (SELECT FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid
           WHERE r.rolname IN ('vp_marker_readiness_runtime', 'vp_marker_janitor_runtime',
               'vp_marker_repair_runtime') AND m.member = 'vp_control_role_owner'::regrole)
       OR (SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid
           WHERE r.rolname IN ('vp_worker_operator_runtime', 'vp_orchestrator_control_runtime',
               'vp_staging_janitor_runtime', 'vp_marker_readiness_runtime',
               'vp_marker_janitor_runtime', 'vp_marker_repair_runtime')
           AND m.member = deploy AND m.grantor = 10
           AND m.admin_option AND NOT m.inherit_option AND NOT m.set_option) <> 6
       OR NOT EXISTS (SELECT FROM pg_db_role_setting WHERE setrole = deploy
           AND setdatabase = 0 AND 'lock_timeout=60s' = ANY(setconfig))
    THEN
        RAISE EXCEPTION 'bootstrap postcondition failed; rolling back all changes';
    END IF;
END
$postflight$;
COMMIT;
