from __future__ import annotations

import importlib
import json
import os
from pathlib import Path

import pytest


def _module():
    return importlib.import_module(
        "app.services.worker_marker_control_role_cli"
    )


def test_generation_role_names_are_deterministic_bounded_and_separate() -> None:
    module = _module()

    names = module.role_names_for_generation(
        "release-2026-07-28-worker-marker-control"
    )

    assert names.stable == {
        "readiness": "vp_marker_readiness_runtime",
        "janitor": "vp_marker_janitor_runtime",
        "repair": "vp_marker_repair_runtime",
    }
    assert set(names.versioned) == {"readiness", "janitor", "repair"}
    assert len(set(names.versioned.values())) == 3
    assert all(
        role.startswith(f"vp_marker_{purpose}_")
        and len(role.encode("utf-8")) <= 63
        for purpose, role in names.versioned.items()
    )
    assert names == module.role_names_for_generation(
        "release-2026-07-28-worker-marker-control"
    )
    assert names != module.role_names_for_generation(
        "release-2026-07-28-worker-marker-control-next"
    )


@pytest.mark.parametrize(
    "generation",
    (
        "",
        "-leading",
        "UPPERCASE",
        "contains_underscore",
        "a" * 64,
        "../escape",
    ),
)
def test_cli_rejects_invalid_generation_without_reading_credentials(
    generation: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    module = _module()
    monkeypatch.setenv(
        "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE",
        str(tmp_path / "does-not-exist"),
    )
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://must-not-be-read:secret@production.invalid/db",
    )

    result = module.main(
        [
            "provision",
            "--generation",
            generation,
            "--state-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "reason_code": "marker_control_invalid_arguments",
        "status": "error",
    }
    assert "secret" not in captured.out


def test_cli_requires_the_owner_url_file_environment_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    module = _module()
    monkeypatch.delenv(
        "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE",
        raising=False,
    )
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://must-not-be-read:secret@production.invalid/db",
    )

    result = module.main(
        [
            "provision",
            "--generation",
            "release-1",
            "--state-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert result == 3
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "reason_code": "marker_control_owner_url_file_invalid",
        "status": "error",
    }
    assert "secret" not in captured.out


def test_cli_reports_a_sanitized_operation_stage(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    module = _module()
    owner_url_file = tmp_path / "owner-database-url"
    owner_url_file.write_text(
        "postgresql://owner:must-not-leak@database/videoprocess\n",
        encoding="utf-8",
    )
    owner_url_file.chmod(0o400)
    monkeypatch.setenv(
        "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE",
        str(owner_url_file),
    )

    async def fail_provision(*_args, **_kwargs) -> None:
        raise module.MarkerControlOperationError("database_privileges")

    monkeypatch.setattr(module, "_provision", fail_provision)

    result = module.main(
        [
            "provision",
            "--generation",
            "release-1",
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    captured = capsys.readouterr()
    assert result == 4
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "reason_code": "marker_control_operation_failed",
        "stage": "database_privileges",
        "status": "error",
    }
    assert "must-not-leak" not in captured.out


def test_credential_paths_are_generation_scoped_and_independent(
    tmp_path,
) -> None:
    module = _module()

    paths = module.credential_paths(tmp_path, "release-1")

    assert paths == {
        "readiness": (
            tmp_path
            / "release-1"
            / "worker-marker-readiness-database-url"
        ),
        "janitor": (
            tmp_path / "release-1" / "worker-marker-janitor-database-url"
        ),
        "repair": (
            tmp_path / "release-1" / "worker-marker-repair-database-url"
        ),
    }
    assert len(set(paths.values())) == 3


@pytest.mark.asyncio
async def test_existing_safe_stable_role_does_not_require_admin_option() -> None:
    module = _module()

    class Postgres16Connection:
        async def fetchrow(self, _query: str, role_name: str):
            return {
                "rolname": role_name,
                "rolcanlogin": False,
                "rolinherit": False,
                "rolsuper": False,
                "rolcreatedb": False,
                "rolcreaterole": False,
                "rolreplication": False,
                "rolbypassrls": False,
            }

        async def execute(self, query: str, *_args):
            if "ALTER ROLE" in query:
                raise module.asyncpg.InsufficientPrivilegeError(
                    "must have admin option on role"
                )
            return "SELECT 1"

        async def fetch(self, _query: str, _role_name: str):
            return []

    await module._ensure_stable_role(
        Postgres16Connection(),
        "vp_marker_readiness_runtime",
    )


@pytest.mark.asyncio
async def test_existing_unsafe_stable_role_fails_closed() -> None:
    module = _module()

    class UnsafeRoleConnection:
        async def fetchrow(self, _query: str, role_name: str):
            return {
                "rolname": role_name,
                "rolcanlogin": True,
                "rolinherit": False,
                "rolsuper": False,
                "rolcreatedb": False,
                "rolcreaterole": False,
                "rolreplication": False,
                "rolbypassrls": False,
            }

        async def execute(self, _query: str, *_args):
            raise AssertionError("unsafe role must not be changed")

        async def fetch(self, _query: str, _role_name: str):
            return []

    with pytest.raises(module.MarkerControlRoleError):
        await module._ensure_stable_role(
            UnsafeRoleConnection(),
            "vp_marker_readiness_runtime",
        )


@pytest.mark.asyncio
async def test_deploy_migrator_delegates_only_marker_role_administration() -> None:
    module = _module()

    class Postgres16Connection:
        def __init__(self) -> None:
            self.memberships: dict[tuple[str, str], dict[str, bool]] = {}

        async def fetchrow(self, _query: str, role_name: str):
            if role_name == "vp_control_role_owner":
                return {
                    "rolname": role_name,
                    "rolcanlogin": True,
                    "rolinherit": True,
                    "rolsuper": False,
                    "rolcreatedb": False,
                    "rolcreaterole": True,
                    "rolreplication": False,
                    "rolbypassrls": False,
                }
            raise AssertionError(f"unexpected role lookup: {role_name}")

        async def fetch(
            self,
            query: str,
            role_names: list[str],
            *_arguments,
        ):
            if "pg_catalog.pg_has_role" in query:
                return [
                    {
                        "granted": role_name,
                        "is_member": True,
                        "has_usage": False,
                        "can_set": False,
                    }
                    for role_name in role_names
                ]
            if "FROM pg_catalog.pg_roles" in query:
                return [
                    {
                        "rolname": role_name,
                        "rolcanlogin": False,
                        "rolinherit": False,
                        "rolsuper": False,
                        "rolcreatedb": False,
                        "rolcreaterole": False,
                        "rolreplication": False,
                        "rolbypassrls": False,
                    }
                    for role_name in role_names
                ]
            if "FROM pg_catalog.pg_auth_members" in query:
                return [
                    {
                        "granted": granted,
                        "member": member,
                        **options,
                    }
                    for (granted, member), options in self.memberships.items()
                ]
            raise AssertionError("unexpected database query")

        async def execute(self, query: str):
            prefix = 'GRANT "'
            separator = '" TO "'
            suffix = '" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE'
            if not query.startswith(prefix) or not query.endswith(suffix):
                raise AssertionError(f"unexpected database command: {query}")
            granted, member = query[len(prefix) : -len(suffix)].split(
                separator,
                1,
            )
            self.memberships[(granted, member)] = {
                "admin_option": True,
                "inherit_option": False,
                "set_option": False,
            }
            return "GRANT ROLE"

    connection = Postgres16Connection()

    await module._delegate_stable_role_administration(connection)

    assert connection.memberships == {
        (
            "vp_marker_janitor_runtime",
            "vp_control_role_owner",
        ): {
            "admin_option": True,
            "inherit_option": False,
            "set_option": False,
        },
        (
            "vp_marker_readiness_runtime",
            "vp_control_role_owner",
        ): {
            "admin_option": True,
            "inherit_option": False,
            "set_option": False,
        },
        (
            "vp_marker_repair_runtime",
            "vp_control_role_owner",
        ): {
            "admin_option": True,
            "inherit_option": False,
            "set_option": False,
        },
    }


@pytest.mark.asyncio
async def test_delegation_rejects_unsafe_stable_role_before_grant() -> None:
    module = _module()

    class UnsafeRoleConnection:
        def __init__(self) -> None:
            self.commands: list[str] = []

        async def fetchrow(self, _query: str, role_name: str):
            return {
                "rolname": role_name,
                "rolcanlogin": True,
                "rolinherit": True,
                "rolsuper": False,
                "rolcreatedb": False,
                "rolcreaterole": True,
                "rolreplication": False,
                "rolbypassrls": False,
            }

        async def fetch(
            self,
            query: str,
            role_names: list[str],
            *_arguments,
        ):
            if "FROM pg_catalog.pg_roles" in query:
                return [
                    {
                        "rolname": role_names[0],
                        "rolcanlogin": True,
                        "rolinherit": False,
                        "rolsuper": False,
                        "rolcreatedb": False,
                        "rolcreaterole": False,
                        "rolreplication": False,
                        "rolbypassrls": False,
                    }
                ]
            return []

        async def execute(self, query: str):
            self.commands.append(query)
            return "GRANT ROLE"

    connection = UnsafeRoleConnection()

    with pytest.raises(module.MarkerControlRoleError):
        await module._delegate_stable_role_administration(connection)

    assert connection.commands == []


@pytest.mark.asyncio
async def test_delegation_rejects_inheritable_grant_from_another_grantor() -> None:
    module = _module()

    class ConflictingGrantConnection:
        async def fetchrow(self, _query: str, role_name: str):
            return {
                "rolname": role_name,
                "rolcanlogin": True,
                "rolinherit": True,
                "rolsuper": False,
                "rolcreatedb": False,
                "rolcreaterole": True,
                "rolreplication": False,
                "rolbypassrls": False,
            }

        async def fetch(
            self,
            query: str,
            role_names: list[str],
            *_arguments,
        ):
            role_name = role_names[0]
            if "pg_catalog.pg_has_role" in query:
                return [
                    {
                        "granted": role_name,
                        "is_member": True,
                        "has_usage": True,
                        "can_set": False,
                    }
                ]
            if "FROM pg_catalog.pg_roles" in query:
                return [
                    {
                        "rolname": role_name,
                        "rolcanlogin": False,
                        "rolinherit": False,
                        "rolsuper": False,
                        "rolcreatedb": False,
                        "rolcreaterole": False,
                        "rolreplication": False,
                        "rolbypassrls": False,
                    }
                ]
            return [
                {
                    "granted": role_name,
                    "member": "vp_control_role_owner",
                    "admin_option": False,
                    "inherit_option": True,
                    "set_option": True,
                },
                {
                    "granted": role_name,
                    "member": "vp_control_role_owner",
                    "admin_option": True,
                    "inherit_option": False,
                    "set_option": False,
                },
            ]

        async def execute(self, _query: str):
            return "GRANT ROLE"

    with pytest.raises(module.MarkerControlRoleError):
        await module._delegate_stable_role_administration(
            ConflictingGrantConnection()
        )


@pytest.mark.asyncio
async def test_delegation_rejects_effective_inheritance_through_role_chain() -> None:
    module = _module()

    class IndirectGrantConnection:
        async def fetchrow(self, _query: str, role_name: str):
            return {
                "rolname": role_name,
                "rolcanlogin": True,
                "rolinherit": True,
                "rolsuper": False,
                "rolcreatedb": False,
                "rolcreaterole": True,
                "rolreplication": False,
                "rolbypassrls": False,
            }

        async def fetch(
            self,
            query: str,
            role_names: list[str],
            *_arguments,
        ):
            role_name = role_names[0]
            if "pg_catalog.pg_has_role" in query:
                return [
                    {
                        "granted": role_name,
                        "is_member": True,
                        "has_usage": True,
                        "can_set": False,
                    }
                ]
            if "FROM pg_catalog.pg_roles" in query:
                return [
                    {
                        "rolname": role_name,
                        "rolcanlogin": False,
                        "rolinherit": False,
                        "rolsuper": False,
                        "rolcreatedb": False,
                        "rolcreaterole": False,
                        "rolreplication": False,
                        "rolbypassrls": False,
                    }
                ]
            if "FROM pg_catalog.pg_auth_members" in query:
                return [
                    {
                        "granted": role_name,
                        "member": "vp_control_role_owner",
                        "admin_option": True,
                        "inherit_option": False,
                        "set_option": False,
                    }
                ]
            raise AssertionError("unexpected database query")

        async def execute(self, _query: str):
            return "GRANT ROLE"

    with pytest.raises(module.MarkerControlRoleError):
        await module._delegate_stable_role_administration(
            IndirectGrantConnection()
        )


def test_owner_url_file_mode_is_revalidated_after_open(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _module()
    owner_url_file = tmp_path / "owner-database-url"
    owner_url_file.write_text(
        "postgresql://postgres:postgres@127.0.0.1/videoprocess\n",
        encoding="utf-8",
    )
    owner_url_file.chmod(0o400)
    monkeypatch.setenv(
        "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE",
        str(owner_url_file),
    )
    real_open = os.open

    def change_mode_then_open(path, flags, *args, **kwargs):
        if Path(path) == owner_url_file:
            owner_url_file.chmod(0o600)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", change_mode_then_open)

    with pytest.raises(module.MarkerControlOwnerURLFileError):
        module._load_owner_database_url()


def test_generation_cleanup_rejects_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    module = _module()
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir(mode=0o700)
    generation = "release-1"
    (state_dir / generation).symlink_to(
        outside_dir,
        target_is_directory=True,
    )
    protected_files = []
    for filename in module.CREDENTIAL_FILENAMES.values():
        path = outside_dir / filename
        path.write_text("must-survive\n", encoding="utf-8")
        protected_files.append(path)
    temporary = outside_dir / ".worker-marker-repair-database-url.swap"
    temporary.write_text("must-survive\n", encoding="utf-8")
    protected_files.append(temporary)

    with pytest.raises(module.MarkerControlRoleError):
        module._remove_generation_credentials(state_dir, generation)

    assert all(
        path.read_text(encoding="utf-8") == "must-survive\n"
        for path in protected_files
    )
