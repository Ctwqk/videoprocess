from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    NodeExecutionClaim,
    require_matching_node_execution_claim,
    require_worker_registration_lease,
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
