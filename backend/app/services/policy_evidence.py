"""Read stored policy evidence without resolving mutable channel configuration."""

from datetime import datetime, timezone
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import or_, select, union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import CompoundSelect, Select

from app.models.channel_agent import (
    AgentTickAudit,
    CandidateFeatureSnapshot,
    ChannelProfile,
    DecisionAuditEntry,
    DecisionPolicyVersion,
    PolicyActivationHistory,
)
from app.schemas.channel_agent import (
    CandidateFeatureSnapshotRead,
    PolicyActivationRead,
    PolicyDecisionEvidenceRead,
    PolicyStatusRead,
    PolicyVersionRead,
    TickDecisionExplanationRead,
)


async def _require_channel(db: AsyncSession, channel_id: UUID) -> None:
    if await db.scalar(select(ChannelProfile.id).where(ChannelProfile.id == channel_id)) is None:
        raise HTTPException(status_code=404, detail="Channel not found")


def _channel_versions(channel_id: UUID) -> Select[tuple[DecisionPolicyVersion]]:
    membership: CompoundSelect[tuple[UUID | None]] = union(
        select(AgentTickAudit.policy_version_id).where(AgentTickAudit.channel_profile_id == channel_id),
        select(PolicyActivationHistory.policy_version_id).where(
            PolicyActivationHistory.channel_profile_id == channel_id
        ),
    )
    return select(DecisionPolicyVersion).where(DecisionPolicyVersion.id.in_(membership)).order_by(
        DecisionPolicyVersion.created_at.desc(), DecisionPolicyVersion.id.desc()
    )


def _channel_activations(channel_id: UUID) -> Select[tuple[PolicyActivationHistory]]:
    return select(PolicyActivationHistory).where(
        PolicyActivationHistory.channel_profile_id == channel_id
    ).order_by(
        PolicyActivationHistory.effective_from.desc(),
        PolicyActivationHistory.created_at.desc(),
        PolicyActivationHistory.id.desc(),
    )


async def get_policy_status(
    db: AsyncSession, channel_id: UUID, *, now: datetime | None = None,
) -> PolicyStatusRead:
    await _require_channel(db, channel_id)
    latest = await db.scalar(_channel_versions(channel_id).limit(1))
    latest_validated = await db.scalar(
        _channel_versions(channel_id).where(DecisionPolicyVersion.status == "validated").limit(1)
    )
    as_of = now if now is not None else datetime.now(timezone.utc)
    activation = await db.scalar(
        _channel_activations(channel_id).where(
            PolicyActivationHistory.target_account_id.is_(None),
            PolicyActivationHistory.effective_from <= as_of,
            or_(PolicyActivationHistory.effective_to.is_(None), PolicyActivationHistory.effective_to > as_of),
        ).limit(1)
    )
    current = PolicyActivationRead.model_validate(activation) if activation is not None else None
    return PolicyStatusRead(
        channel_id=channel_id,
        mode=current.mode if current is not None else "off",
        latest_policy=PolicyVersionRead.model_validate(latest) if latest is not None else None,
        latest_validated_policy=(
            PolicyVersionRead.model_validate(latest_validated) if latest_validated is not None else None
        ),
        current_activation=current,
    )


async def list_policy_versions(
    db: AsyncSession, channel_id: UUID, *, limit: int = 100, offset: int = 0,
) -> list[PolicyVersionRead]:
    await _require_channel(db, channel_id)
    rows = await db.scalars(
        _channel_versions(channel_id).offset(max(offset, 0)).limit(min(max(limit, 1), 500))
    )
    return [PolicyVersionRead.model_validate(row) for row in rows]


async def get_policy_version(
    db: AsyncSession, channel_id: UUID, policy_version_id: UUID,
) -> PolicyVersionRead:
    await _require_channel(db, channel_id)
    row = await db.scalar(_channel_versions(channel_id).where(DecisionPolicyVersion.id == policy_version_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Policy version not found")
    return PolicyVersionRead.model_validate(row)


async def list_policy_activations(
    db: AsyncSession, channel_id: UUID, *, limit: int = 100, offset: int = 0,
) -> list[PolicyActivationRead]:
    await _require_channel(db, channel_id)
    rows = await db.scalars(
        _channel_activations(channel_id).offset(max(offset, 0)).limit(min(max(limit, 1), 500))
    )
    return [PolicyActivationRead.model_validate(row) for row in rows]


async def get_decision_explanation(db: AsyncSession, tick_audit_id: UUID) -> TickDecisionExplanationRead:
    tick = await db.get(AgentTickAudit, tick_audit_id)
    if tick is None:
        raise HTTPException(status_code=404, detail="Tick audit not found")
    await _require_channel(db, tick.channel_profile_id)
    policy = await db.get(DecisionPolicyVersion, tick.policy_version_id) if tick.policy_version_id else None
    snapshots = (await db.scalars(
        select(CandidateFeatureSnapshot).where(CandidateFeatureSnapshot.tick_audit_id == tick.id).order_by(
            CandidateFeatureSnapshot.candidate_id,
            CandidateFeatureSnapshot.feature_schema_version,
            CandidateFeatureSnapshot.id,
        )
    )).all()
    decisions = (await db.scalars(
        select(DecisionAuditEntry).where(
            DecisionAuditEntry.tick_audit_id == tick.id,
            DecisionAuditEntry.channel_profile_id == tick.channel_profile_id,
        ).order_by(DecisionAuditEntry.candidate_id, DecisionAuditEntry.created_at, DecisionAuditEntry.id)
    )).all()

    # Resolve links only inside this tick. Never follow an unchecked decision FK.
    by_id = {snapshot.id: snapshot for snapshot in snapshots}
    inconsistent = tick.policy_version_id is not None and policy is None
    inconsistent |= any(snapshot.policy_version_id != tick.policy_version_id for snapshot in snapshots)
    for decision in decisions:
        inconsistent |= decision.policy_version_id is not None and decision.policy_version_id != tick.policy_version_id
        if decision.feature_snapshot_id is not None:
            snapshot = by_id.get(decision.feature_snapshot_id)
            inconsistent |= (
                snapshot is None
                or snapshot.candidate_id != decision.candidate_id
                or snapshot.source_kind != decision.candidate_source
                or snapshot.policy_version_id != decision.policy_version_id
            )
    if inconsistent:
        raise HTTPException(status_code=409, detail="Inconsistent policy evidence links")

    # No list cap: a complete explanation includes accepted and rejected candidates.
    return TickDecisionExplanationRead.model_validate({
        "tick_audit_id": tick.id,
        "channel_profile_id": tick.channel_profile_id,
        "tick_id": tick.tick_id,
        "replay_status": tick.replay_status,
        "policy_version_id": tick.policy_version_id,
        "policy": PolicyVersionRead.model_validate(policy) if policy is not None else None,
        "candidate_set_hash": tick.candidate_set_hash,
        "feature_as_of": tick.feature_as_of,
        "decision_summary_json": tick.decision_summary_json,
        "snapshots": [CandidateFeatureSnapshotRead.model_validate(row) for row in snapshots],
        "decisions": [PolicyDecisionEvidenceRead.model_validate(row) for row in decisions],
    })
