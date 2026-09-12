from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    NodeExecutionClaim,
    acknowledge_worker_task_delivery,
    authorize_worker_task_ack,
    claim_registered_worker_node,
    observe_worker_registration_lease,
    recover_registered_worker_node,
    require_matching_node_execution_claim,
    require_worker_registration_lease,
    require_worker_registration_margin,
    require_worker_task_ack_receipt,
)


def _claim() -> NodeExecutionClaim:
    return NodeExecutionClaim(
        job_id=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        worker_id="vision-worker@127:1:instance",
        started_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
        worker_registration_id=uuid.uuid4(),
        worker_lease_epoch=8,
    )


def _authority(claim: NodeExecutionClaim):
    return SimpleNamespace(
        job=SimpleNamespace(id=claim.job_id),
        node=SimpleNamespace(
            id=claim.node_execution_id,
            worker_id=claim.worker_id,
            started_at=claim.started_at,
            worker_registration_id=claim.worker_registration_id,
            worker_lease_epoch=claim.worker_lease_epoch,
        ),
    )


@pytest.mark.asyncio
async def test_registered_claim_uses_schema_qualified_atomic_function() -> None:
    claim = _claim()
    dispatch_key = uuid.uuid4()
    attestation_id = uuid.uuid4()
    calls: list[tuple[str, dict]] = []

    class Result:
        def one_or_none(self):
            return (claim.started_at, attestation_id)

    class Bind:
        class Dialect:
            name = "postgresql"

        dialect = Dialect()

    class Session:
        def get_bind(self):
            return Bind()

        async def execute(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return Result()

    actual_claim, actual_attestation_id = await claim_registered_worker_node(
        Session(),
        job_id=claim.job_id,
        node_execution_id=claim.node_execution_id,
        registration_id=claim.worker_registration_id,
        lease_epoch=claim.worker_lease_epoch,
        worker_id=claim.worker_id,
        redis_stream="vp:tasks:vision",
        consumer_group="vision-workers",
        message_id="1-0",
        payload_sha256="a" * 64,
        dispatch_key=dispatch_key,
    )

    assert actual_claim == claim
    assert actual_attestation_id == attestation_id
    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_claim_worker_node" in statement
    assert parameters["dispatch_key"] == dispatch_key


def test_matching_node_claim_requires_registration_id_and_epoch() -> None:
    claim = _claim()
    authority = _authority(claim)
    require_matching_node_execution_claim(authority, claim)

    authority.node.worker_lease_epoch += 1
    with pytest.raises(JobExecutionAuthorityBlocked, match="claim changed"):
        require_matching_node_execution_claim(authority, claim)


@pytest.mark.asyncio
async def test_worker_lease_fence_calls_schema_qualified_function_in_current_transaction() -> None:
    claim = _claim()
    calls: list[tuple[str, dict]] = []

    class Session:
        async def scalar(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return True

    await require_worker_registration_lease(Session(), claim)

    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_require_worker_lease" in statement
    assert parameters == {
        "registration_id": claim.worker_registration_id,
        "lease_epoch": claim.worker_lease_epoch,
    }


@pytest.mark.asyncio
async def test_worker_lease_fence_rejects_generationless_claim_without_query() -> None:
    claim = NodeExecutionClaim(
        job_id=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        worker_id="local-worker",
        started_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    class Session:
        async def scalar(self, statement, parameters):
            raise AssertionError("generationless claim must not query")

    with pytest.raises(JobExecutionAuthorityBlocked, match="registration lease"):
        await require_worker_registration_lease(Session(), claim)


@pytest.mark.asyncio
async def test_control_plane_observer_uses_separate_schema_qualified_function() -> None:
    claim = _claim()
    calls: list[tuple[str, dict]] = []

    class Session:
        async def scalar(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return True

    await observe_worker_registration_lease(Session(), claim)

    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_observe_worker_lease" in statement
    assert "public.vp_require_worker_lease" not in statement
    assert parameters == {
        "registration_id": claim.worker_registration_id,
        "lease_epoch": claim.worker_lease_epoch,
    }


@pytest.mark.asyncio
async def test_worker_submission_margin_uses_database_authoritative_function() -> None:
    claim = _claim()
    calls: list[tuple[str, dict]] = []

    class Session:
        async def scalar(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return True

    await require_worker_registration_margin(
        Session(),
        claim,
        minimum_margin_seconds=150,
    )

    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_require_worker_lease_margin" in statement
    assert "clock_timestamp" not in statement
    assert parameters == {
        "registration_id": claim.worker_registration_id,
        "lease_epoch": claim.worker_lease_epoch,
        "minimum_margin_seconds": 150,
    }


@pytest.mark.asyncio
async def test_worker_task_ack_receipt_binds_exact_delivery_and_claim() -> None:
    claim = _claim()
    calls: list[tuple[str, dict]] = []

    class Session:
        async def scalar(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return True

    payload_sha256 = "a" * 64
    dispatch_key = uuid.uuid4()
    await require_worker_task_ack_receipt(
        Session(),
        claim,
        redis_stream="vp:tasks:vision",
        consumer_group="vision-workers",
        message_id="1710000000000-7",
        payload_sha256=payload_sha256,
        dispatch_key=dispatch_key,
    )

    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_require_worker_task_ack_receipt" in statement
    assert parameters == {
        "registration_id": claim.worker_registration_id,
        "lease_epoch": claim.worker_lease_epoch,
        "worker_id": claim.worker_id,
        "worker_started_at": claim.started_at,
        "redis_stream": "vp:tasks:vision",
        "consumer_group": "vision-workers",
        "message_id": "1710000000000-7",
        "payload_sha256": payload_sha256,
        "dispatch_key": dispatch_key,
    }


@pytest.mark.asyncio
async def test_worker_task_ack_state_binds_exact_attestation_and_delivery() -> None:
    claim = _claim()
    calls: list[tuple[str, dict]] = []
    attestation_id = uuid.uuid4()
    dispatch_key = uuid.uuid4()

    class Session:
        async def scalar(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return None

    await acknowledge_worker_task_delivery(
        Session(),
        claim,
        attestation_id=attestation_id,
        redis_stream="vp:tasks:vision",
        consumer_group="vision-workers",
        message_id="1710000000000-7",
        payload_sha256="b" * 64,
        dispatch_key=dispatch_key,
    )

    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_acknowledge_worker_task_delivery" in statement
    assert parameters == {
        "attestation_id": attestation_id,
        "registration_id": claim.worker_registration_id,
        "lease_epoch": claim.worker_lease_epoch,
        "worker_id": claim.worker_id,
        "worker_started_at": claim.started_at,
        "redis_stream": "vp:tasks:vision",
        "consumer_group": "vision-workers",
        "message_id": "1710000000000-7",
        "payload_sha256": "b" * 64,
        "dispatch_key": dispatch_key,
    }


@pytest.mark.asyncio
async def test_worker_task_ack_authorization_is_durable_and_exact() -> None:
    claim = _claim()
    calls: list[tuple[str, dict]] = []
    attestation_id = uuid.uuid4()

    class Session:
        async def scalar(self, statement, parameters):
            calls.append((str(statement), dict(parameters)))
            return None

    await authorize_worker_task_ack(
        Session(),
        claim,
        attestation_id=attestation_id,
    )

    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "public.vp_authorize_worker_task_ack" in statement
    assert parameters == {
        "attestation_id": attestation_id,
        "registration_id": claim.worker_registration_id,
        "lease_epoch": claim.worker_lease_epoch,
        "worker_id": claim.worker_id,
        "worker_started_at": claim.started_at,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    ("terminal", "held_unresolved_event"),
)
async def test_registered_recovery_accepts_state_preserving_outcomes(
    outcome: str,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()

    class Bind:
        class Dialect:
            name = "postgresql"

        dialect = Dialect()

    class Session:
        def get_bind(self):
            return Bind()

        async def scalar(self, statement, parameters):
            assert "public.vp_recover_registered_worker_node" in str(
                statement
            )
            assert parameters == {
                "job_id": job_id,
                "node_execution_id": node_execution_id,
            }
            return outcome

    assert (
        await recover_registered_worker_node(
            Session(),
            job_id,
            node_execution_id,
        )
        == outcome
    )
