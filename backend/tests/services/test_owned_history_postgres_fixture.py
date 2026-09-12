"""Offline checks for the parent-only A2 PostgreSQL fixture boundary."""
import pytest

from tests.migrations.owned_history_postgres import checked_url, seed_document
from tests.services.test_owned_seed_inventory_history import NOW
from app.services import owned_seed_inventory_history as history


@pytest.mark.parametrize("raw, confirmation", [
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_scratch", "wrong"),
    ("postgresql+asyncpg://test:synthetic@production.invalid:55465/vp_owned_inventory_test_a2_scratch", "vp_owned_inventory_test_a2_scratch"),
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:5432/vp_owned_inventory_test_a2_scratch", "vp_owned_inventory_test_a2_scratch"),
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/postgres", "postgres"),
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_scratch?sslmode=disable", "vp_owned_inventory_test_a2_scratch"),
])
def test_fixture_rejects_ambient_or_unconfirmed_endpoint(raw, confirmation):
    with pytest.raises(ValueError, match="^explicit A2 disposable"):
        checked_url(raw, confirmation)


def test_fixture_requires_explicit_loopback_named_database():
    raw = "postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_scratch"
    assert checked_url(raw, "vp_owned_inventory_test_a2_scratch").port == 55465


@pytest.mark.parametrize("null_link", [False, True])
def test_pg_seed_retains_four_native_paths_with_model_complete_nonsecret_rows(null_link):
    rows, observations = seed_document(NOW, null_link=null_link)
    snapshot = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id="UC" + "a" * 22,
        observed_at=NOW, redis_observations=observations)
    from app.services import owned_seed_inventory as service
    sources = service._retirement_sources(snapshot, requested=True)
    import hashlib
    import uuid
    hashes = {uuid.UUID(s["id"]): hashlib.sha256(b"a" * 100).hexdigest() for s in sources["source_assets"]}
    document = service._qualified_retirement(snapshot, sources, (hashes, observations), "fixture", "fixture:only")
    cert = history.RetiredPreuploadCertificate.parse(document)
    assert cert.retained_facts.account.as_dict()["platform_account_id"] == ""
    assert len(document["terminal_graph"]["worker_task_dispatches"]) == 4
    assert "lease_secret_sha256" not in str(rows) and "token_sha256" not in str(rows)


def test_pg_seed_uses_actual_native_enum_labels():
    from sqlalchemy import Enum
    rows, _ = seed_document(NOW)
    for name, model in history.HISTORY_MODELS.items():
        for row in rows[name]:
            for column in model.__table__.columns:
                if isinstance(column.type, Enum) and row.get(column.name) is not None:
                    assert row[column.name] in column.type.enums, (name, column.name, row[column.name])


@pytest.mark.parametrize("defer_retry", [False, True])
async def test_cancel_ack_fixture_inserts_unresolved_branch_without_rewinding_history(defer_retry):
    import copy
    import hashlib
    import json
    import uuid
    from app.services import owned_seed_inventory as service
    from tests.migrations.owned_history_postgres import insert_record, seed_graph

    inserted = []

    class Owner:
        async def fetchval(self, sql):
            assert sql == "SELECT clock_timestamp()"
            return NOW

        async def execute(self, sql, *args):
            assert sql.startswith("INSERT INTO "), "fixture must not rewind a terminal row"
            if sql.startswith("INSERT INTO public.worker_task_dispatches "):
                inserted.append(json.loads(args[0]))

    owner = Owner()
    rows, pending = await seed_graph(owner, defer_cancel_retry=defer_retry)
    assert len(inserted) == (3 if defer_retry else 4)
    assert all(row["resolution_state"] in {"acknowledged", "cancelled"} for row in inserted)
    keys = {row["dispatch_key"] for row in inserted}
    _, all_observations = seed_document(NOW)
    observations = tuple(o for o in all_observations if o.dispatch_key is None or o.dispatch_key in keys)
    snapshot = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id="UC" + "a" * 22,
        observed_at=NOW, redis_observations=observations)
    sources = service._retirement_sources(snapshot, requested=True)
    hashes = {uuid.UUID(s["id"]): hashlib.sha256(b"a" * 100).hexdigest() for s in sources["source_assets"]}
    certificate = service._qualified_retirement(snapshot, sources, (hashes, observations), "fixture", "fixture:only")
    assert len(certificate["terminal_graph"]["worker_task_dispatches"]) == len(inserted)
    if not defer_retry:
        assert pending is None
        return
    original = copy.deepcopy(rows)
    assert pending["origin_receipt_id"] is not None
    assert pending["delivery_state"] == "delivered" and pending["resolution_state"] == "unresolved"
    assert pending["acknowledged_at"] is pending["cancelled_at"] is None
    assert pending["redis_message_id"] == "3000-0" and pending["dispatch_key"] not in keys
    await insert_record(owner, "worker_task_dispatches", pending)
    assert len({row["id"] for row in inserted}) == 4
    assert inserted[-1] == pending and rows == original
    fresh_rows = copy.deepcopy(rows)
    fresh_rows["worker_task_dispatches"].append(pending)
    fresh = history.OwnedHistorySnapshot.from_rows(fresh_rows, platform_channel_id=snapshot.platform_channel_id,
        observed_at=NOW, redis_observations=all_observations)
    with pytest.raises(service.OwnedInventoryError, match="^owned_inventory_retirement_changed$"):
        service._qualified_retirement(fresh, sources, (hashes, all_observations), "fixture", "fixture:only")


@pytest.mark.parametrize("exit_kind", ["close", "error"])
async def test_nested_inventory_fixture_disposes_pool_on_generator_exit(monkeypatch, exit_kind):
    from types import SimpleNamespace
    from sqlalchemy.ext.asyncio import AsyncEngine
    from tests.api.test_owned_seed_inventory import inventory_env

    disposed = []
    original = AsyncEngine.dispose

    async def observed(engine, *args, **kwargs):
        disposed.append(engine)
        await original(engine, *args, **kwargs)

    monkeypatch.setattr(AsyncEngine, "dispose", observed)
    generator = inventory_env.__wrapped__(monkeypatch, SimpleNamespace())
    engine = None
    try:
        env = await anext(generator)
        engine = env.factory.kw["bind"]
        if exit_kind == "close":
            await generator.aclose()
        else:
            with pytest.raises(ValueError, match="fixture exit"):
                await generator.athrow(ValueError("fixture exit"))
        assert engine in disposed
    finally:
        await generator.aclose()
        if engine is not None:
            await original(engine)


async def test_a2_adapter_enters_real_inventory_validation_with_exact_child_confirmation(monkeypatch):
    from types import SimpleNamespace
    from sqlalchemy.engine import make_url
    from tests.api import test_owned_seed_inventory as api_fixture
    from tests.migrations.test_owned_history_seal_postgres import a2_env

    target = make_url("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_child")
    seen = []

    class BeforeConnect(Exception):
        pass

    def intercept(url):
        seen.append(make_url(url))
        raise BeforeConnect

    monkeypatch.setenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM", "stale-anchor-confirmation")
    monkeypatch.setattr(api_fixture, "create_async_engine", intercept)
    generator = a2_env.__wrapped__(SimpleNamespace(target=target), monkeypatch)
    try:
        with pytest.raises(BeforeConnect):
            await anext(generator)
        assert seen == [target]
    finally:
        await generator.aclose()


async def test_a2_generated_children_use_the_validated_namespace_without_reusing_anchor(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from tests.migrations import owned_history_postgres as fixtures

    raw = "postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_anchor"
    anchor = checked_url(raw, "vp_owned_inventory_test_a2_anchor")
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_TEST_URL", raw)
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_CONFIRM", anchor.database)
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_SYSTEM_ID", "1234567890123456789")
    names, closed = [], []

    class BeforeCreate(Exception):
        pass

    class Admin:
        async def fetchval(self, sql):
            return {"SHOW server_version_num": 160000,
                    "SELECT system_identifier FROM pg_control_system()": 1234567890123456789,
                    "SELECT version_num FROM public.alembic_version": fixtures.HEAD}[sql]

        async def execute(self, sql):
            assert sql.startswith('CREATE DATABASE "') and sql.endswith('"')
            names.append(sql.removeprefix('CREATE DATABASE "').removesuffix('"'))
            raise BeforeCreate

        async def close(self):
            closed.append(True)

    async def connect(url, **_kwargs):
        assert url == fixtures.dsn(anchor)
        return Admin()

    monkeypatch.setattr(fixtures.asyncpg, "connect", connect)
    for _ in range(2):
        generator = fixtures.a2_pg.__wrapped__(monkeypatch, tmp_path, SimpleNamespace(param=False))
        try:
            with pytest.raises(BeforeCreate):
                await anext(generator)
        finally:
            await generator.aclose()
    assert len(set(names)) == len(closed) == 2
    assert anchor.database not in names
    for name in names:
        checked_url(anchor.set(database=name).render_as_string(hide_password=False), name)


async def test_a2_fixture_captures_seal_catalogue_then_qualifies_final_head(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from tests.migrations import owned_history_postgres as fixtures

    raw = "postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_anchor"
    anchor = checked_url(raw, "vp_owned_inventory_test_a2_anchor")
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_TEST_URL", raw)
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_CONFIRM", anchor.database)
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_SYSTEM_ID", "1234567890123456789")
    events, closed = [], []
    revision = None

    class BeforeSeed(Exception):
        pass

    class Admin:
        async def fetchval(self, sql):
            return {"SHOW server_version_num": 160000,
                    "SELECT system_identifier FROM pg_control_system()": 1234567890123456789,
                    "SELECT version_num FROM public.alembic_version": fixtures.HEAD}[sql]

        async def execute(self, sql):
            assert sql.startswith(('CREATE DATABASE "', 'DROP DATABASE "'))

        async def close(self):
            closed.append("admin")

    class Owner:
        async def execute(self, sql):
            assert sql.startswith("INSERT INTO runtime_schedules")
            events.append(("seed", revision))
            raise BeforeSeed

        async def close(self):
            closed.append("owner")

    async def connect(url, **_kwargs):
        return Admin() if url == fixtures.dsn(anchor) else Owner()

    async def migrate(target, head):
        nonlocal revision
        assert target.database != anchor.database
        revision = head
        events.append(("migrate", head))

    async def catalogue(owner):
        assert isinstance(owner, Owner)
        events.append(("catalogue", revision))
        return {}

    monkeypatch.setattr(fixtures.asyncpg, "connect", connect)
    monkeypatch.setattr(fixtures, "migrate", migrate)
    monkeypatch.setattr(fixtures, "catalogue", catalogue)
    generator = fixtures.a2_pg.__wrapped__(monkeypatch, tmp_path, SimpleNamespace(param=False))
    try:
        with pytest.raises(BeforeSeed):
            await anext(generator)
    finally:
        await generator.aclose()
    assert events == [
        ("migrate", "039_registered_consumer_guard"),
        ("catalogue", "039_registered_consumer_guard"),
        ("migrate", "040_owned_history_seal"),
        ("catalogue", "040_owned_history_seal"),
        ("migrate", "041_registered_consumer_terminal"),
        ("seed", "041_registered_consumer_terminal"),
    ]
    assert fixtures.HEAD == "041_registered_consumer_terminal"
    assert closed == ["owner", "admin"]


@pytest.mark.parametrize("metrics_retry", [False, True])
@pytest.mark.parametrize("microsecond", [0, 123456])
def test_succeeded_pg_fixture_has_complete_native_rows_and_real_history_classification(metrics_retry, microsecond):
    from sqlalchemy import Enum
    from tests.migrations import owned_history_postgres as fixtures

    now = NOW.replace(microsecond=microsecond)
    rows = fixtures.succeeded_document(now, metrics_retry=metrics_retry)
    for name, model in history.HISTORY_MODELS.items():
        for row in rows[name]:
            for column in model.__table__.columns:
                if not column.nullable:
                    assert row.get(column.name) is not None, (name, column.name)
                if isinstance(column.type, Enum) and row.get(column.name) is not None:
                    assert row[column.name] in column.type.enums
    account = rows["publishing_accounts"][0]
    assert account["platform_account_id"] == ""
    # Only the real API may qualify a legacy binding. For this offline schema
    # oracle classify the same rows directly under an explicit synthetic UC.
    account["platform_account_id"] = "UC" + "a" * 22
    result = history.assess_owned_history(history.OwnedHistorySnapshot.from_rows(
        rows, platform_channel_id=account["platform_account_id"], observed_at=now), now=now)
    assert result.block_reason is None, result.block_reason
    assert result.wait_reason == ("owned_inventory_metrics_pending" if metrics_retry else None)
    assert rows["owned_seed_inventories"] == []
