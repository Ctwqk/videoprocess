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
