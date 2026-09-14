"""Parent-only disposable PG16 qualification for immutable history pins."""
import copy
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services.registered_consumer_reconcile import decode_pins
from app.services import registered_consumer_reconcile_runtime as runtime
from app.services.worker_runtime_role_cli import role_names_for_generation
from tests.migrations import test_registered_consumer_reconcile_postgres as legacy
from tests.migrations.test_registered_consumer_reconcile_postgres import postgres_case as postgres_case


HEAD = "045_registered_consumer_history"
SIGNATURE = "public.vp_registered_consumer_reconcile_history_guard(text,uuid[],uuid[])"


@pytest.fixture
def history_case(monkeypatch, postgres_case):
    monkeypatch.setattr(legacy, "HEAD", HEAD)
    monkeypatch.setattr(legacy, "SIGNATURE", SIGNATURE)

    @asynccontextmanager
    async def opened():
        async with postgres_case() as case:
            predecessor = case.request.pins.workers[0].predecessor
            old = next(row for row in case.registrations if row.id == predecessor.registration_id)
            grant = next(row for row in case.grants if row.id == old.grant_id)
            registration_id, grant_id, instance_id = uuid4(), uuid4(), uuid4()
            generation = grant.generation - 1
            principal = role_names_for_generation(old.service_name, generation).versioned
            registered_at = old.registered_at - timedelta(hours=1)
            extra_grant = WorkerAdmissionGrant(**{
                **copy.deepcopy(legacy.row_values(grant)),
                "id": grant_id,
                "generation": generation,
                "database_principal": principal,
                "token_sha256": hashlib.sha256(str(grant_id).encode()).hexdigest(),
                "issued_at": registered_at - timedelta(hours=1),
                "activated_at": registered_at,
                "revoked_at": old.registered_at,
                "created_at": registered_at - timedelta(hours=1),
                "updated_at": old.registered_at,
            })
            extra = WorkerRegistration(**{
                **copy.deepcopy(legacy.row_values(old)),
                "id": registration_id,
                "grant_id": grant_id,
                "database_principal": principal,
                "worker_instance_id": instance_id,
                "redis_consumer_id": f"{old.worker_type}-worker@{old.worker_host}:1:{instance_id}",
                "lease_epoch": old.lease_epoch - 1,
                "registered_at": registered_at,
                "heartbeat_at": old.registered_at - timedelta(seconds=120),
                "lease_expires_at": old.registered_at - timedelta(seconds=60),
                "revoked_at": old.registered_at,
                "superseded_by": old.id,
                "lease_secret_sha256": hashlib.sha256(str(registration_id).encode()).hexdigest(),
            })
            payload = json.loads(case.request.pins.canonical_json)
            payload["version"] = 2
            for worker in payload["workers"]:
                worker["ancestors"] = []
            ancestor = {
                **payload["workers"][0]["predecessor"],
                "registration_id": str(registration_id),
                "grant_id": str(grant_id),
                "generation": generation,
                "database_principal": principal,
                "worker_instance_id": str(instance_id),
                "redis_consumer_id": extra.redis_consumer_id,
                "lease_epoch": extra.lease_epoch,
                "registered_at": registered_at.isoformat(),
            }
            payload["workers"][0]["ancestors"] = [ancestor]
            request = replace(case.request, pins=decode_pins(json.dumps(payload)))
            inserted = False
            try:
                async with case.owner.transaction():
                    await legacy.insert_row(case.owner, extra_grant)
                    await legacy.insert_row(case.owner, extra)
                inserted = True
                yield SimpleNamespace(case=case, request=request, extra=extra, grant=extra_grant)
            finally:
                if inserted:
                    async with case.owner.transaction():
                        await case.owner.execute("DELETE FROM worker_registrations WHERE id=$1", registration_id)
                        await case.owner.execute("DELETE FROM worker_admission_grants WHERE id=$1", grant_id)

    return opened


@pytest.mark.asyncio
async def test_actual_history_guard_returns_all_typed_rows_without_dml(history_case):
    async with history_case() as value:
        before = await value.case.snapshot()
        async with value.case.operator.transaction():
            facts = await runtime.read_guard(value.case.operator, value.request)
            assert len(facts.registrations) == len(facts.grants) == 9
            assert {row.id for row in facts.registrations} == {
                pin.registration_id
                for worker in value.request.pins.workers
                for pin in (worker.current, *worker.retiring)
            }
        assert await value.case.snapshot() == before
        for connection in (value.case.runtime_worker, value.case.watcher):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await runtime.read_guard(connection, value.request)
        for table in ("worker_registrations", "worker_admission_grants"):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await value.case.operator.fetch(f"SELECT id FROM {table}")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["cross_service", "reversed_epoch", "reversed_generation", "missing_chain_pin"])
async def test_actual_history_guard_refuses_changed_chain(history_case, fault):
    async with history_case() as value:
        request = value.request
        original = None
        try:
            if fault == "cross_service":
                original = ("worker_registrations", "superseded_by", value.extra.id, value.extra.superseded_by)
                replacement = request.pins.workers[1].current.registration_id
            elif fault == "reversed_epoch":
                original = ("worker_registrations", "lease_epoch", value.extra.id, value.extra.lease_epoch)
                replacement = request.pins.workers[0].current.lease_epoch + 10
            elif fault == "reversed_generation":
                original = ("worker_admission_grants", "generation", value.grant.id, value.grant.generation)
                replacement = request.pins.workers[0].current.generation + 10
            else:
                current = [worker.current.registration_id for worker in request.pins.workers]
                with pytest.raises(asyncpg.RaiseError, match="registered_reconcile_inventory_changed"):
                    await value.case.operator.fetch(
                        "SELECT * FROM public.vp_registered_consumer_reconcile_history_guard($1::text,$2::uuid[],$3::uuid[])",
                        request.control_generation, current, [value.extra.id],
                    )
                return
            table, field, row_id, _ = original
            await value.case.owner.execute(f"UPDATE {table} SET {field}=$1 WHERE id=$2", replacement, row_id)
            with pytest.raises(asyncpg.RaiseError, match="registered_reconcile_history_changed"):
                await runtime.read_guard(value.case.operator, request)
        finally:
            if original is not None:
                table, field, row_id, old = original
                await value.case.owner.execute(f"UPDATE {table} SET {field}=$1 WHERE id=$2", old, row_id)


@pytest.mark.asyncio
async def test_actual_history_guard_locks_ancestor_until_owned_transaction_ends(history_case):
    async with history_case() as value:
        transaction = value.case.operator.transaction()
        await transaction.start()
        try:
            await runtime.read_guard(value.case.operator, value.request)
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with value.case.owner.transaction():
                    await value.case.owner.execute("SET LOCAL lock_timeout='100ms'")
                    await value.case.owner.execute(
                        "UPDATE worker_registrations SET revoke_reason=revoke_reason WHERE id=$1", value.extra.id
                    )
        finally:
            await transaction.rollback()
        async with value.case.owner.transaction():
            await value.case.owner.execute("SET LOCAL lock_timeout='100ms'")
            assert await value.case.owner.execute(
                "UPDATE worker_registrations SET revoke_reason=revoke_reason WHERE id=$1", value.extra.id
            ) == "UPDATE 1"
