import runpy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services import worker_role_cli_common as common


@pytest.mark.parametrize(("explicit", "ci_url", "expected"), [
    (None, "postgresql+asyncpg://ci/test", "postgresql+asyncpg://ci/test"),
    ("", "postgresql+asyncpg://ci/test", "postgresql+asyncpg://ci/test"),
    ("postgresql://override/test", "postgresql://ci/test", "postgresql://override/test"),
    (None, None, ""),
])
def test_postgres_integration_url_falls_back_to_ci(monkeypatch, explicit, ci_url, expected):
    for name, value in (
        ("WORKER_ROLE_LIFECYCLE_POSTGRES_TEST_URL", explicit),
        ("CHANNEL_OPS_POSTGRES_TEST_URL", ci_url),
    ):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    integration = runpy.run_path(str(
        Path(__file__).with_name("test_worker_role_cli_common_postgres.py")
    ))
    assert integration["POSTGRES_URL"] == expected
    assert integration["pytestmark"].args == (not bool(expected),)


def safe_role(*, login=False):
    return {
        "rolcanlogin": login,
        "rolinherit": login,
        "rolsuper": False,
        "rolcreatedb": False,
        "rolcreaterole": False,
        "rolreplication": False,
        "rolbypassrls": False,
    }


def connection(**overrides):
    values = {
        "execute": AsyncMock(),
        "fetch": AsyncMock(return_value=[]),
        "fetchrow": AsyncMock(return_value=None),
        "fetchval": AsyncMock(return_value=False),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize("login", [False, True])
@pytest.mark.parametrize("attribute", tuple(safe_role()))
async def test_attribute_guard_rejects_each_noncanonical_flag(login, attribute):
    attributes = safe_role(login=login)
    attributes[attribute] = not attributes[attribute]
    conn = connection(fetchrow=AsyncMock(return_value=attributes))
    with pytest.raises(common.WorkerRoleCommonError, match="attributes invalid"):
        await common.require_safe_role_attributes(conn, "managed", login=login)
    conn.execute.assert_not_called()


async def test_attribute_guard_rejects_missing_role():
    with pytest.raises(common.WorkerRoleCommonError, match="attributes invalid"):
        await common.require_safe_role_attributes(connection(), "missing", login=False)


@pytest.mark.parametrize("login", [False, True])
async def test_attribute_guard_preserves_safe_attributes(login):
    conn = connection(fetchrow=AsyncMock(return_value=safe_role(login=login)))
    await common.require_safe_role_attributes(conn, "managed", login=login)
    conn.execute.assert_not_called()


async def test_existing_stable_role_does_not_alter_restricted_attributes():
    conn = connection(
        fetchrow=AsyncMock(return_value=safe_role()),
        fetchval=AsyncMock(side_effect=[False, "test_db"]),
    )
    await common.ensure_stable_role(
        conn, "stable", setting_prefix="test", authorized_members=(),
    )
    for call in conn.execute.call_args_list:
        sql = call.args[0]
        if "ALTER ROLE" in sql:
            assert not {"NOSUPERUSER", "NOREPLICATION", "NOBYPASSRLS"} & set(sql.split())


async def test_existing_login_does_not_alter_restricted_attributes():
    canonical = {
        "edge_count": 1,
        "admin_is_canonical": True,
        "inherit_is_canonical": True,
        "set_is_canonical": True,
        "grantor_is_canonical": True,
    }
    conn = connection(
        fetchrow=AsyncMock(side_effect=[safe_role(login=True), canonical]),
        fetchval=AsyncMock(side_effect=[True, False, "test_db"]),
    )
    await common.harden_existing_login_role(conn, "login", "stable")
    for call in conn.execute.call_args_list:
        sql = call.args[0]
        if "ALTER ROLE" in sql:
            assert not {"NOSUPERUSER", "NOREPLICATION", "NOBYPASSRLS"} & set(sql.split())


def creator_edge(**overrides):
    edge = {
        "membership_oid": 100,
        "granted_role": "stable",
        "member_role": "deploy",
        "grantor_role": "bootstrap",
        "grantor_oid": 10,
        "grantor_is_superuser": True,
        "admin_option": True,
        "inherit_option": False,
        "set_option": False,
        "member_is_current_principal": True,
        "creator_principal_is_safe": True,
    }
    edge.update(overrides)
    return edge


async def test_membership_cleanup_preserves_creator_and_revokes_other_edges():
    creator = creator_edge()
    other = creator_edge(
        membership_oid=101, member_role="other", grantor_role="deploy",
        grantor_oid=30, grantor_is_superuser=False,
        member_is_current_principal=False, creator_principal_is_safe=False,
    )
    conn = connection(fetch=AsyncMock(side_effect=[[creator, other], [creator]]))
    await common.revoke_role_membership_authority(conn, ("stable",))
    conn.execute.assert_awaited_once_with(
        'REVOKE "stable" FROM "other" GRANTED BY "deploy" CASCADE'
    )


@pytest.mark.parametrize("drift", [
    {"admin_option": False}, {"inherit_option": True}, {"set_option": True},
    {"grantor_oid": 11}, {"grantor_is_superuser": False},
    {"creator_principal_is_safe": False},
    {"granted_role": "outside", "grantor_role": "stable"},
])
async def test_membership_cleanup_rejects_unsafe_creator_variants(drift):
    conn = connection(fetch=AsyncMock(return_value=[creator_edge(**drift)]))
    with pytest.raises(common.WorkerRoleCommonError, match="creator membership invalid"):
        await common.revoke_role_membership_authority(conn, ("stable",))
    conn.execute.assert_not_called()


async def test_membership_cleanup_rejects_revoke_that_does_not_converge():
    conn = connection(
        fetch=AsyncMock(return_value=[creator_edge(member_is_current_principal=False)]),
        fetchval=AsyncMock(return_value=True),
    )
    with pytest.raises(common.WorkerRoleCommonError, match="convergence failed"):
        await common.revoke_role_membership_authority(conn, ("stable",))


async def test_reset_revokes_role_column_acl(monkeypatch):
    monkeypatch.setattr(common, "_converge_public_privileges", AsyncMock())
    conn = connection(fetch=AsyncMock(return_value=[{
        "relname": "items", "attname": "secret", "privilege_type": "UPDATE",
    }]))
    await common.reset_public_privileges(conn, "stable")
    assert 'REVOKE UPDATE ("secret") ON TABLE public."items" FROM "stable"' in [
        call.args[0] for call in conn.execute.call_args_list
    ]


async def test_public_convergence_rejects_residual_acl():
    conn = connection(fetchval=AsyncMock(side_effect=["test_db", True]))
    with pytest.raises(common.WorkerRoleCommonError, match="PUBLIC privileges remain"):
        await common._converge_public_privileges(conn)
