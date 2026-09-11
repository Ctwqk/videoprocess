from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from app.services.worker_control_role_cli import role_names_for_generation


GENERATION = "c-0123456789abcdef0123"
TARGET = "postgresql+asyncpg://owner:unused@db.test:5432/testdb"


@pytest.fixture
def mounted_url(tmp_path, monkeypatch):
    principal = role_names_for_generation(GENERATION).versioned["orchestrator"]
    url = make_url(TARGET).set(username=principal, password="fixture-secret")
    path = tmp_path / "orchestrator-url"
    path.write_text(url.render_as_string(hide_password=False))
    path.chmod(0o400)
    monkeypatch.setenv("WORKER_ORCHESTRATOR_DATABASE_URL_FILE", str(path))
    monkeypatch.setenv("WORKER_ORCHESTRATOR_CONTROL_GENERATION", GENERATION)
    return path, principal


class Engine:
    def __init__(self, *, valid=True, error=None):
        self.valid, self.error = valid, error
        self.disposed = False
        self.calls = []
        self.sync_engine = create_engine("sqlite://")

    @asynccontextmanager
    async def connect(self):
        yield self

    async def scalar(self, statement, parameters):
        self.calls.append((str(statement), parameters))
        if self.error:
            raise self.error
        return self.valid

    async def dispose(self):
        self.disposed = True


def module():
    return importlib.import_module("app.orchestrator.registered_db")


@pytest.mark.asyncio
async def test_actual_factory_uses_only_qualified_mounted_login(
    mounted_url, monkeypatch
):
    mod = module()
    runtime = mod.RegisteredDatabase()
    engine = Engine()
    created = []

    def create(url, **options):
        created.append((url, options))
        return engine

    monkeypatch.setattr(mod, "create_async_engine", create)
    with pytest.raises(mod.RegisteredDatabaseError):
        runtime.session()
    await runtime.start(TARGET)
    assert runtime.ready and runtime.generation == GENERATION
    assert created[0][0].username == mounted_url[1]
    assert created[0][0].username != "owner"
    assert created[0][1]["hide_parameters"] is True
    query, parameters = engine.calls[0]
    assert "session_user" in query and "current_user" in query
    assert parameters["principal"] == mounted_url[1]
    assert parameters["database"] == "testdb"
    assert runtime.session().bind is engine
    with pytest.raises(mod.RegisteredDatabaseError):
        await runtime.start(TARGET)
    await runtime.close()
    assert engine.disposed and not runtime.ready
    with pytest.raises(mod.RegisteredDatabaseError):
        runtime.session()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "alteration",
    [
        "missing",
        "mode",
        "symlink",
        "generation",
        "login",
        "database",
        "host",
        "options",
        "sqlite",
    ],
)
async def test_invalid_mount_never_opens_owner_or_restricted_connection(
    mounted_url, monkeypatch, alteration
):
    mod = module()
    path, _ = mounted_url
    if alteration == "missing":
        monkeypatch.delenv("WORKER_ORCHESTRATOR_DATABASE_URL_FILE")
    elif alteration == "mode":
        path.chmod(0o600)
    elif alteration == "symlink":
        link = path.with_suffix(".link")
        link.symlink_to(path)
        monkeypatch.setenv("WORKER_ORCHESTRATOR_DATABASE_URL_FILE", str(link))
    elif alteration == "generation":
        monkeypatch.setenv("WORKER_ORCHESTRATOR_CONTROL_GENERATION", "bad")
    else:
        url = make_url(path.read_text())
        replacements = {
            "login": {"username": "owner"},
            "database": {"database": "other"},
            "host": {"host": "other.test"},
            "options": {"query": {"options": "-c role=owner"}},
            "sqlite": {"drivername": "sqlite+aiosqlite"},
        }
        path.chmod(0o600)
        path.write_text(
            url.set(**replacements[alteration]).render_as_string(hide_password=False)
        )
        path.chmod(0o400)
    monkeypatch.setattr(
        mod,
        "create_async_engine",
        lambda *a, **k: pytest.fail("invalid configuration connected"),
    )
    with pytest.raises(mod.RegisteredDatabaseError) as exc:
        await mod.RegisteredDatabase().start(TARGET)
    assert "fixture-secret" not in str(exc.value)
    assert exc.value.__suppress_context__


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [False, None, "true", RuntimeError("fixture-secret"), asyncio.CancelledError()],
)
async def test_failed_qualification_disposes_pool_and_never_enables_factory(
    mounted_url, monkeypatch, result
):
    mod = module()
    engine = (
        Engine(error=result)
        if isinstance(result, BaseException)
        else Engine(valid=result)
    )
    monkeypatch.setattr(mod, "create_async_engine", lambda *a, **k: engine)
    runtime = mod.RegisteredDatabase()
    expected = (
        asyncio.CancelledError
        if isinstance(result, asyncio.CancelledError)
        else mod.RegisteredDatabaseError
    )
    with pytest.raises(expected) as exc:
        await runtime.start(TARGET)
    assert engine.disposed and not runtime.ready
    assert "fixture-secret" not in str(exc.value)
    with pytest.raises(mod.RegisteredDatabaseError):
        runtime.session()


def test_registered_services_cannot_fall_back_to_global_owner_pool():
    mod = module()
    listener = importlib.import_module("app.orchestrator.event_listener")
    engine = importlib.import_module("app.orchestrator.engine")
    for service in (
        listener._registered_event_receipts,
        engine._worker_task_dispatches,
    ):
        with pytest.raises(mod.RegisteredDatabaseError):
            service._session_factory()
