"""Catalog-only worker signaling bootstrap. This module emits SQL; it never connects.

Run with ``python -m app.services.worker_session_signal_sql`` and give the output
to the original OID-10 administrator in the intended database, under the deploy
writer lock. Ordinary migrations only verify this installation.
"""

from __future__ import annotations


SCHEMA = "vp_worker_session_private"
LOCK_TIMEOUT = "2s"
SERVICES_SQL = """(
    'vp-ffmpeg-worker-go-swarm', 'vp-ffmpeg-worker-gpu-swarm',
    'vp-vision-worker-swarm', 'vp-youtube-publisher-swarm'
)"""


def canonical_role_sql(service: str, generation: str) -> str:
    return f"""('vp_worker_' || pg_catalog.substr(pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to({service}, 'UTF8')
            || pg_catalog.decode('00', 'hex')
            || pg_catalog.convert_to(({generation})::pg_catalog.text, 'UTF8')), 'hex'), 1, 20))"""


# The installation binds database and owner OIDs, not mutable names. These bodies
# must never gain a dependency on an application-owned object (including types).
VALIDATE_BODY = f"""
DECLARE
    v_database pg_catalog.oid := @DATABASE_OID@;
    v_owner pg_catalog.oid := @OWNER_OID@;
    v_target pg_catalog.oid;
    v_name pg_catalog.text;
BEGIN
    IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'worker_signal_isolation_invalid';
    END IF;
    IF p_service_name IS NULL OR p_service_name NOT IN {SERVICES_SQL}
       OR p_generation IS NULL OR p_generation <= 0 THEN
        RAISE EXCEPTION 'worker_signal_identity_invalid';
    END IF;
    v_name := {canonical_role_sql("p_service_name", "p_generation")};
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended('vp-worker-database-acl-dcl', 0));
    -- Prevent role rename/drop/recreation, LOGIN/attribute and edge changes
    -- through the final signal. These short shared-catalog locks also pin owner.
    LOCK TABLE pg_catalog.pg_database, pg_catalog.pg_authid,
               pg_catalog.pg_auth_members IN SHARE MODE;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_database
        WHERE oid = v_database AND datname = pg_catalog.current_database()
          AND datdba = v_owner
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE oid = 10 AND rolsuper
          AND rolname = current_user
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles AS owner
        WHERE owner.oid = v_owner AND (
            (owner.oid = 10 AND owner.rolsuper)
            OR (owner.oid <> 10 AND owner.rolcanlogin AND owner.rolinherit
                AND owner.rolcreaterole AND NOT owner.rolsuper
                AND NOT owner.rolcreatedb AND NOT owner.rolreplication
                AND NOT owner.rolbypassrls AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_auth_members AS parent
                    WHERE parent.member = owner.oid
                      AND (parent.inherit_option OR parent.set_option)
                ))
        )
    ) THEN
        RAISE EXCEPTION 'worker_signal_owner_invalid';
    END IF;
    SELECT oid INTO v_target FROM pg_catalog.pg_roles
    WHERE rolname = v_name AND NOT rolcanlogin AND rolinherit
      AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb
      AND NOT rolreplication AND NOT rolbypassrls
      AND oid <> v_owner AND oid <> 10;
    IF v_target IS NULL THEN
        RAISE EXCEPTION 'worker_signal_target_invalid';
    END IF;
    IF (v_owner <> 10 AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members
        WHERE roleid = v_target AND member = v_owner AND grantor = 10
          AND admin_option AND NOT inherit_option AND NOT set_option
    )) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members
        WHERE (roleid = v_target OR member = v_target OR grantor = v_target)
          AND NOT (v_owner <> 10 AND roleid = v_target AND member = v_owner
              AND grantor = 10 AND admin_option
              AND NOT inherit_option AND NOT set_option)
    ) THEN
        RAISE EXCEPTION 'worker_signal_creator_invalid';
    END IF;
    RETURN v_target;
END;
"""

RETIRE_BODY = f"""
DECLARE
    v_target pg_catalog.oid;
    v_backend pg_catalog.record;
    v_count pg_catalog.int4 := 0;
BEGIN
    v_target := {SCHEMA}.validate_target(p_service_name, p_generation);
    PERFORM pg_catalog.pg_stat_clear_snapshot();
    FOR v_backend IN
        SELECT pid, backend_start FROM pg_catalog.pg_stat_activity
        WHERE datid = @DATABASE_OID@ AND usesysid = v_target
          AND backend_type = 'client backend'
          AND pid <> pg_catalog.pg_backend_pid()
    LOOP
        PERFORM pg_catalog.pg_stat_clear_snapshot();
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_stat_activity
            WHERE pid = v_backend.pid AND backend_start = v_backend.backend_start
              AND datid = @DATABASE_OID@ AND usesysid = v_target
              AND backend_type = 'client backend'
              AND pid <> pg_catalog.pg_backend_pid()
        ) THEN
            IF pg_catalog.pg_terminate_backend(v_backend.pid) THEN
                v_count := v_count + 1;
            END IF;
        END IF;
    END LOOP;
    RETURN v_count;
END;
"""


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _bound_body(body: str) -> str:
    return (
        f"pg_catalog.replace(pg_catalog.replace({_literal(body)}, "
        "'@DATABASE_OID@', v_database::pg_catalog.text), '@OWNER_OID@', v_owner::pg_catalog.text)"
    )


def _acl_check(acl: str, *, schema: bool) -> str:
    # Compare sets plus cardinality, accepting no extra grantee or grant option.
    expected = (
        "SELECT 10::oid AS grantor, 10::oid AS grantee, 'USAGE'::text AS privilege_type, "
        "false AS is_grantable UNION SELECT 10, 10, 'CREATE', false "
        "UNION SELECT 10, v_owner, 'USAGE', false"
        if schema
        else "SELECT 10::oid AS grantor, 10::oid AS grantee, 'EXECUTE'::text AS privilege_type, "
        "false AS is_grantable UNION SELECT 10, v_owner, 'EXECUTE', false"
    )
    return f"""({acl} IS NOT NULL
        AND (SELECT pg_catalog.count(*) FROM pg_catalog.aclexplode({acl}))
            = (SELECT pg_catalog.count(*) FROM ({expected}) AS expected)
        AND NOT EXISTS (
        (SELECT * FROM pg_catalog.aclexplode({acl}) EXCEPT ({expected}))
        UNION ALL
        (({expected}) EXCEPT SELECT * FROM pg_catalog.aclexplode({acl}))
    ))"""


VERIFY_SQL = f"""
DO $verify_worker_signal$
DECLARE
    v_database pg_catalog.oid;
    v_owner pg_catalog.oid;
    v_schema pg_catalog.oid;
BEGIN
    SELECT oid, datdba INTO STRICT v_database, v_owner
    FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database();
    SELECT oid INTO v_schema FROM pg_catalog.pg_namespace AS n
    WHERE n.nspname = '{SCHEMA}' AND n.nspowner = 10
      AND {_acl_check("n.nspacl", schema=True)};
    IF v_schema IS NULL OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles WHERE oid = 10 AND rolsuper
    ) OR (SELECT pg_catalog.count(*) FROM pg_catalog.pg_proc
          WHERE pronamespace = v_schema) <> 2 THEN
        RAISE EXCEPTION 'worker_signal_bootstrap_missing_or_drifted';
    END IF;
    IF (SELECT pg_catalog.count(*) FROM pg_catalog.pg_proc AS p
        JOIN pg_catalog.pg_language AS l ON l.oid = p.prolang
        WHERE p.pronamespace = v_schema AND p.proowner = 10
          AND l.lanname = 'plpgsql' AND l.lanowner = 10
          AND p.prosecdef AND NOT p.proleakproof AND NOT p.proisstrict
          AND NOT p.proretset AND p.prokind = 'f'
          AND p.provolatile = 'v' AND p.proparallel = 'u'
          AND p.proconfig = ARRAY['search_path=pg_catalog', 'lock_timeout={LOCK_TIMEOUT}']
          AND p.proargtypes = '25 20'::pg_catalog.oidvector
          AND p.proargnames = ARRAY['p_service_name', 'p_generation']
          AND p.proallargtypes IS NULL AND p.proargmodes IS NULL
          AND p.pronargdefaults = 0 AND p.prosupport = 0
          AND {_acl_check("p.proacl", schema=False)}
          AND ((p.proname = 'validate_target' AND p.prorettype = 26
                AND p.prosrc = {_bound_body(VALIDATE_BODY)})
            OR (p.proname = 'retire' AND p.prorettype = 23
                AND p.prosrc = {_bound_body(RETIRE_BODY)}))
    ) <> 2 THEN
        RAISE EXCEPTION 'worker_signal_bootstrap_missing_or_drifted';
    END IF;
END;
$verify_worker_signal$;
"""


INSTALL_SQL = f"""
DO $install_worker_signal$
DECLARE
    v_database pg_catalog.oid;
    v_owner pg_catalog.oid;
    v_owner_name pg_catalog.text;
    v_function pg_catalog.text;
    v_acl pg_catalog.record;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                   WHERE oid = 10 AND rolsuper AND rolname = current_user) THEN
        RAISE EXCEPTION 'worker_signal_original_bootstrap_required';
    END IF;
    SELECT d.oid, d.datdba, r.rolname INTO STRICT v_database, v_owner, v_owner_name
    FROM pg_catalog.pg_database AS d
    JOIN pg_catalog.pg_roles AS r ON r.oid = d.datdba
    WHERE d.datname = pg_catalog.current_database();
    -- Never adopt or overwrite an existing namespace, even for root rehearsals.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '{SCHEMA}') THEN
        RETURN;
    END IF;
    CREATE SCHEMA {SCHEMA};
    -- Remove even custom default ACLs; do not change cluster-wide defaults.
    FOR v_acl IN SELECT DISTINCT a.grantee FROM pg_catalog.pg_namespace AS n,
        LATERAL pg_catalog.aclexplode(n.nspacl) AS a WHERE n.nspname = '{SCHEMA}'
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA {SCHEMA} FROM %s',
            CASE WHEN v_acl.grantee = 0 THEN 'PUBLIC'
                 ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(v_acl.grantee)) END);
    END LOOP;
    REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC;
    EXECUTE pg_catalog.format('GRANT ALL ON SCHEMA {SCHEMA} TO %I', current_user);
    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA {SCHEMA} TO %I', v_owner_name);
    EXECUTE pg_catalog.format(
        'CREATE FUNCTION {SCHEMA}.validate_target(p_service_name pg_catalog.text, '
        'p_generation pg_catalog.int8) RETURNS pg_catalog.oid '
        'LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog '
        'SET lock_timeout = ''{LOCK_TIMEOUT}'' AS %L',
        {_bound_body(VALIDATE_BODY)});
    EXECUTE pg_catalog.format(
        'CREATE FUNCTION {SCHEMA}.retire(p_service_name pg_catalog.text, '
        'p_generation pg_catalog.int8) RETURNS pg_catalog.int4 '
        'LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog '
        'SET lock_timeout = ''{LOCK_TIMEOUT}'' AS %L',
        {_bound_body(RETIRE_BODY)});
    FOREACH v_function IN ARRAY ARRAY['validate_target', 'retire'] LOOP
        FOR v_acl IN SELECT DISTINCT a.grantee FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace,
            LATERAL pg_catalog.aclexplode(COALESCE(p.proacl,
                pg_catalog.acldefault('f', p.proowner))) AS a
            WHERE n.nspname = '{SCHEMA}' AND p.proname = v_function
        LOOP
            EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION {SCHEMA}.%I(text,bigint) FROM %s',
                v_function, CASE WHEN v_acl.grantee = 0 THEN 'PUBLIC'
                    ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(v_acl.grantee)) END);
        END LOOP;
        EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION {SCHEMA}.%I(text,bigint) TO %I',
            v_function, current_user);
        EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION {SCHEMA}.%I(text,bigint) TO %I',
            v_function, v_owner_name);
    END LOOP;
END;
$install_worker_signal$;
"""


def bootstrap_sql() -> str:
    """Return one atomic, rerunnable installation; drift aborts without repair."""
    return (
        "BEGIN;\nSET LOCAL search_path = pg_catalog;\n"
        + INSTALL_SQL
        + VERIFY_SQL
        + "COMMIT;\n"
    )


if __name__ == "__main__":
    print(bootstrap_sql(), end="")
