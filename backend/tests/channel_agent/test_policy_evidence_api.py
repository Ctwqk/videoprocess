from __future__ import annotations

import ast
import inspect
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.api.channel_agent as api
from app.db import get_db
from app.models.channel_agent import (
    AgentTickAudit,
    CandidateFeatureSnapshot,
    ChannelProfile,
    DecisionAuditEntry,
    DecisionPolicyVersion,
    PolicyActivationHistory,
    PublishingAccount,
)

PREFIX = "/api/v1/channel-agent"
NOW = datetime(2026, 7, 26, 12, tzinfo=timezone.utc)


@pytest.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in (ChannelProfile, PublishingAccount, DecisionPolicyVersion,
                      PolicyActivationHistory, AgentTickAudit,
                      CandidateFeatureSnapshot, DecisionAuditEntry):
            await conn.run_sync(model.__table__.create)
    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        yield db
    await engine.dispose()


@pytest.fixture
async def client(session):
    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[get_db] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as value:
        yield value


async def channel(session):
    row = ChannelProfile(name="Evidence")
    session.add(row)
    await session.flush()
    return row


async def policy(session, *, number=1, status="validated", created_at=NOW):
    row = DecisionPolicyVersion(
        id=UUID(f"a0000000-0000-0000-0000-{number:012x}"), policy_key="channelops-baseline", version=f"v{number}",
        status=status, feature_schema_version="channelops-candidate-v1", reward_version="stored-reward",
        formula_json={"weights": {"freshness": 0.5}}, hard_guard_config_json={"privacy": "unlisted"},
        portfolio_config_json={"lane": "stored"}, exploration_config_json={"enabled": False},
        code_commit_sha="a" * 40, template_registry_version="legacy-unversioned",
        prompt_bundle_version="legacy-unversioned", config_hash="b" * 64,
        created_by="fixture", change_reason="baseline", created_at=created_at,
    )
    session.add(row)
    await session.flush()
    return row


async def tick(session, owner, version=None, *, state="snapshot_complete"):
    row = AgentTickAudit(
        channel_profile_id=owner.id, tick_id=str(uuid4()), replay_status=state,
        policy_version_id=version.id if version else None,
        candidate_set_hash="c" * 64 if version else None,
        feature_as_of=NOW if version else None,
    )
    session.add(row)
    await session.flush()
    return row


async def activation(session, owner, version, *, number=1, mode="shadow",
                     effective_from=NOW - timedelta(days=1), effective_to=None, account=None):
    row = PolicyActivationHistory(
        id=UUID(f"a0000000-0000-0000-0000-{number:012x}"), channel_profile_id=owner.id, policy_version_id=version.id,
        target_account_id=account, mode=mode, rollout_percentage=12.5,
        deterministic_salt="stored-salt", effective_from=effective_from, effective_to=effective_to,
        request_id=f"activation-{number}", actor="operator", reason="stored reason",
        rollback_reason=None, feature_flag_snapshot_json={"enabled": False}, created_at=NOW,
    )
    session.add(row)
    await session.flush()
    return row


async def candidate(session, audit, version, *, name="accepted", selected=True):
    snapshot = CandidateFeatureSnapshot(
        tick_audit_id=audit.id, candidate_id=name, candidate_source="manual_seed", source_kind="owned",
        policy_version_id=version.id, feature_schema_version="channelops-candidate-v1", feature_as_of=NOW,
        raw_features_json={"freshness": 0.25}, normalized_features_json=None,
        missing_feature_mask_json={"reward": True}, source_record_refs_json={"seed_id": "stored-seed"},
        candidate_set_hash="c" * 64, feature_hash="d" * 64,
    )
    session.add(snapshot)
    await session.flush()
    decision = DecisionAuditEntry(
        tick_audit_id=audit.id, channel_profile_id=audit.channel_profile_id,
        candidate_id=name, candidate_source="manual_seed", selected=selected,
        decision="accepted" if selected else "rejected", feature_snapshot_id=snapshot.id,
        policy_version_id=version.id, candidate_set_hash="c" * 64, decision_hash="e" * 64,
        score_json={"stored_score": 1.25}, guard_results_json=[{"verdict": "allow" if selected else "deny"}],
        pds_decision_json={"stored": True}, learning_context_json={"as_of": "stored"},
        baseline_score=1.25 if selected else None, final_score=1.25 if selected else None,
        rejection_reason=None if selected else "cadence",
    )
    session.add(decision)
    await session.flush()
    return snapshot, decision


async def test_empty_status_is_off_and_service_matches_api(session, client):
    owner = await channel(session)
    response = await client.get(f"{PREFIX}/channels/{owner.id}/policy-status")
    assert response.status_code == 200
    assert response.json() == {
        "channel_id": str(owner.id), "mode": "off", "latest_policy": None, "current_activation": None,
    }
    from app.services.policy_evidence import get_policy_status

    result = await get_policy_status(session, owner.id, now=NOW)
    assert result.model_dump(mode="json") == response.json()


async def test_versions_are_channel_linked_deduplicated_and_latest_validated(session, client):
    owner, other = await channel(session), await channel(session)
    old = await policy(session)
    latest = await policy(session, number=2)
    draft = await policy(session, number=3, status="draft", created_at=NOW + timedelta(days=1))
    retired = await policy(session, number=4, status="retired", created_at=NOW + timedelta(days=2))
    foreign = await policy(session, number=5, created_at=NOW + timedelta(days=3))
    await tick(session, owner, old)
    await tick(session, owner, latest)
    await tick(session, owner, latest)
    await tick(session, owner, draft)
    await activation(session, owner, retired)
    await tick(session, other, foreign)
    rows = (await client.get(f"{PREFIX}/channels/{owner.id}/policy-versions")).json()
    assert [row["version"] for row in rows] == ["v4", "v3", "v2", "v1"]
    status = (await client.get(f"{PREFIX}/channels/{owner.id}/policy-status")).json()
    assert status["latest_policy"]["id"] == str(latest.id)
    assert status["mode"] == "shadow"
    assert status["current_activation"]["policy_version_id"] == str(retired.id)
    assert (await client.get(f"{PREFIX}/channels/{owner.id}/policy-versions/{foreign.id}")).status_code == 404
    assert (await client.get(f"{PREFIX}/channels/{other.id}/policy-versions/{latest.id}")).status_code == 404


async def test_policy_detail_returns_immutable_stored_facts_without_mutations(session, client):
    owner = await channel(session)
    version = await policy(session)
    audit = await tick(session, owner, version)
    await candidate(session, audit, version)
    await activation(session, owner, version)
    await session.commit()
    owner.name = "Changed current config"
    await session.commit()
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(session.bind.sync_engine, "before_cursor_execute", capture)
    try:
        for path in (f"/channels/{owner.id}/policy-status", f"/channels/{owner.id}/policy-versions",
                     f"/channels/{owner.id}/policy-activations", f"/ticks/{audit.id}/decision-explanation"):
            assert (await client.get(PREFIX + path)).status_code == 200
        response = await client.get(f"{PREFIX}/channels/{owner.id}/policy-versions/{version.id}")
    finally:
        event.remove(session.bind.sync_engine, "before_cursor_execute", capture)
    assert response.status_code == 200
    body = response.json()
    assert body["formula_json"] == {"weights": {"freshness": 0.5}}
    assert body["hard_guard_config_json"] == {"privacy": "unlisted"}
    assert body["portfolio_config_json"] == {"lane": "stored"}
    assert body["exploration_config_json"] == {"enabled": False}
    assert body["config_hash"] == "b" * 64
    assert body["code_commit_sha"] == "a" * 40
    assert body["template_registry_version"] == "legacy-unversioned"
    assert body["prompt_bundle_version"] == "legacy-unversioned"
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)


@pytest.mark.parametrize("status", ["draft", "retired"])
async def test_status_does_not_invent_validated_policy_from_other_stored_versions(session, client, status):
    owner = await channel(session)
    version = await policy(session, status=status)
    await tick(session, owner, version)
    await policy(session, number=2)
    response = await client.get(f"{PREFIX}/channels/{owner.id}/policy-status")
    assert response.status_code == 200
    assert response.json()["latest_policy"] is None
    assert response.json()["mode"] == "off"


async def test_activation_starts_inclusively_and_created_at_precedes_id_tiebreak(session):
    from app.services.policy_evidence import get_policy_status

    owner = await channel(session)
    version = await policy(session)
    winner = await activation(session, owner, version, number=1, effective_from=NOW, mode="canary")
    older = await activation(session, owner, version, number=2, effective_from=NOW, mode="active")
    older.created_at = NOW - timedelta(seconds=1)
    await session.flush()
    result = await get_policy_status(session, owner.id, now=NOW)
    assert result.mode == "canary"
    assert result.current_activation.id == winner.id


async def test_status_uses_effective_channel_scope_with_deterministic_ties(session, client):
    from app.services.policy_evidence import get_policy_status

    owner, other = await channel(session), await channel(session)
    version = await policy(session)
    await activation(session, owner, version, number=1)
    winner = await activation(session, owner, version, number=2, mode="off")
    await activation(session, owner, version, number=3, mode="active", account=uuid4())
    await activation(session, owner, version, number=4, effective_from=NOW + timedelta(seconds=1))
    await activation(session, owner, version, number=5, effective_to=NOW)
    await activation(session, other, version, number=6, mode="active")
    result = await get_policy_status(session, owner.id, now=NOW)
    assert result.mode == "off"
    assert result.current_activation.id == winner.id
    history = (await client.get(f"{PREFIX}/channels/{owner.id}/policy-activations")).json()
    assert [row["id"] for row in history] == [f"a0000000-0000-0000-0000-{n:012x}" for n in (4, 5, 3, 2, 1)]
    assert history[2]["target_account_id"] is not None
    assert history[0]["effective_to"] is None
    assert history[1]["effective_to"] is not None
    assert history[2]["rollout_percentage"] == 12.5
    assert history[2]["deterministic_salt"] == "stored-salt"
    assert history[2]["feature_flag_snapshot_json"] == {"enabled": False}


@pytest.mark.parametrize("kind", ["account", "future", "expired"])
async def test_no_effective_channel_activation_means_off(session, kind):
    from app.services.policy_evidence import get_policy_status

    owner = await channel(session)
    version = await policy(session)
    kwargs = {"account": uuid4()} if kind == "account" else (
        {"effective_from": NOW + timedelta(seconds=1)} if kind == "future" else {"effective_to": NOW}
    )
    await activation(session, owner, version, mode="active", **kwargs)
    result = await get_policy_status(session, owner.id, now=NOW)
    assert result.mode == "off"
    assert result.current_activation is None
    assert result.latest_policy.id == version.id


@pytest.mark.parametrize("resource", ["policy-versions", "policy-activations"])
async def test_lists_paginate_with_existing_default_and_cap(session, client, resource):
    owner = await channel(session)
    for number in range(1, 503):
        version = await policy(session, number=number)
        await activation(session, owner, version, number=number)
    url = f"{PREFIX}/channels/{owner.id}/{resource}"
    assert len((await client.get(url)).json()) == 100
    assert len((await client.get(url + "?limit=9999")).json()) == 500
    page = (await client.get(url + "?limit=2&offset=1")).json()
    assert [row["id"] for row in page] == [
        "a0000000-0000-0000-0000-0000000001f5", "a0000000-0000-0000-0000-0000000001f4",
    ]
    assert len((await client.get(url + "?limit=0&offset=-1")).json()) == 1


async def test_complete_explanation_returns_all_stored_candidates_and_nullable_facts(session, client):
    owner = await channel(session)
    version = await policy(session)
    audit = await tick(session, owner, version)
    await candidate(session, audit, version, name="z-accepted")
    rejected, decision = await candidate(session, audit, version, name="a-rejected", selected=False)
    response = await client.get(f"{PREFIX}/ticks/{audit.id}/decision-explanation")
    assert response.status_code == 200
    body = response.json()
    assert body["replay_status"] == "snapshot_complete"
    assert body["tick_audit_id"] == str(audit.id)
    assert body["channel_profile_id"] == str(owner.id)
    assert body["policy"]["id"] == str(version.id)
    assert body["candidate_set_hash"] == "c" * 64
    assert body["feature_as_of"].startswith("2026-07-26T12:00:00")
    assert [row["candidate_id"] for row in body["snapshots"]] == ["a-rejected", "z-accepted"]
    snapshot = body["snapshots"][0]
    assert snapshot["id"] == str(rejected.id)
    assert snapshot["raw_features_json"] == {"freshness": 0.25}
    assert snapshot["feature_hash"] == "d" * 64
    assert snapshot["missing_feature_mask_json"] == {"reward": True}
    assert snapshot["source_record_refs_json"] == {"seed_id": "stored-seed"}
    for field in ("normalized_features_json", "cadence_snapshot_json", "content_mix_snapshot_json",
                  "material_supply_json", "production_reliability_json", "learning_references_json",
                  "cost_estimate_json", "risk_estimate_json", "topic_lane_id", "target_account_id"):
        assert snapshot[field] is None
    row = body["decisions"][0]
    assert row["id"] == str(decision.id)
    assert row["feature_snapshot_id"] == str(rejected.id)
    assert row["decision"] == "rejected"
    assert row["decision_hash"] == "e" * 64
    assert row["rejection_reason"] == "cadence"
    assert row["score_json"] == {"stored_score": 1.25}
    assert row["guard_results_json"] == [{"verdict": "deny"}]
    assert row["pds_decision_json"] == {"stored": True}
    assert row["learning_context_json"] == {"as_of": "stored"}
    for field in ("baseline_score", "final_score", "rank", "shadow_score", "shadow_rank",
                  "shadow_selected", "experiment_id"):
        assert row[field] is None


@pytest.mark.parametrize("state", ["legacy_unreplayable", "snapshot_pending", "snapshot_complete"])
async def test_empty_and_legacy_explanations_are_explicit_not_reconstructed(session, client, state):
    owner = await channel(session)
    version = await policy(session)
    await activation(session, owner, version)
    audit = await tick(session, owner, version if state == "snapshot_complete" else None, state=state)
    if state == "legacy_unreplayable":
        session.add(DecisionAuditEntry(
            tick_audit_id=audit.id, channel_profile_id=owner.id,
            candidate_id="old", candidate_source="manual_seed", selected=True,
        ))
        await session.flush()
    response = await client.get(f"{PREFIX}/ticks/{audit.id}/decision-explanation")
    assert response.status_code == 200
    body = response.json()
    assert body["replay_status"] == state
    assert body["snapshots"] == []
    if state != "snapshot_complete":
        assert body["policy"] is None
        assert body["policy_version_id"] is None
        assert body["candidate_set_hash"] is None
        assert body["feature_as_of"] is None
    if state == "legacy_unreplayable":
        assert body["decisions"][0]["selected"] is True
        for field in ("decision", "policy_version_id", "feature_snapshot_id", "decision_hash", "baseline_score"):
            assert body["decisions"][0][field] is None
    else:
        assert body["decisions"] == []


async def test_explanation_does_not_truncate_at_list_cap(session, client):
    owner = await channel(session)
    version = await policy(session)
    audit = await tick(session, owner, version)
    for number in range(501):
        await candidate(session, audit, version, name=f"candidate-{number:04}")
    body = (await client.get(f"{PREFIX}/ticks/{audit.id}/decision-explanation")).json()
    assert len(body["snapshots"]) == len(body["decisions"]) == 501


@pytest.mark.parametrize("bad_link", ["foreign_tick", "foreign_channel", "candidate", "decision_policy", "snapshot_policy"])
async def test_malformed_links_fail_closed_without_foreign_evidence(session, client, bad_link):
    owner, other = await channel(session), await channel(session)
    version = await policy(session)
    foreign_version = await policy(session, number=2)
    audit = await tick(session, owner, version)
    snapshot, decision = await candidate(session, audit, version)
    if bad_link in {"foreign_tick", "foreign_channel"}:
        foreign_tick = await tick(session, other if bad_link == "foreign_channel" else owner, version)
        foreign_snapshot, _ = await candidate(session, foreign_tick, version, name="secret-foreign")
        decision.feature_snapshot_id = foreign_snapshot.id
    elif bad_link == "candidate":
        decision.candidate_id = "different-candidate"
    elif bad_link == "decision_policy":
        decision.policy_version_id = foreign_version.id
    else:
        snapshot.policy_version_id = foreign_version.id
    await session.flush()
    response = await client.get(f"{PREFIX}/ticks/{audit.id}/decision-explanation")
    assert response.status_code == 409
    assert response.json() == {"detail": "Inconsistent policy evidence links"}


async def test_explanation_excludes_cross_channel_decisions_even_with_matching_tick(session, client):
    owner, other = await channel(session), await channel(session)
    version = await policy(session)
    audit = await tick(session, owner, version)
    await candidate(session, audit, version)
    session.add(DecisionAuditEntry(
        tick_audit_id=audit.id, channel_profile_id=other.id,
        candidate_id="foreign-secret", candidate_source="manual_seed",
    ))
    await session.flush()
    response = await client.get(f"{PREFIX}/ticks/{audit.id}/decision-explanation")
    assert response.status_code == 200
    assert [row["candidate_id"] for row in response.json()["decisions"]] == ["accepted"]


@pytest.mark.parametrize("path", [
    "/channels/{id}/policy-status", "/channels/{id}/policy-versions",
    "/channels/{id}/policy-activations", "/channels/{id}/policy-versions/{id}",
    "/ticks/{id}/decision-explanation",
])
@pytest.mark.parametrize("identifier,expected", [("not-a-uuid", 422), (str(UUID(int=999)), 404)])
async def test_invalid_and_missing_identifiers(client, path, identifier, expected):
    response = await client.get(PREFIX + path.format(id=identifier))
    assert response.status_code == expected


async def test_version_detail_validates_version_uuid_and_missing_version(session, client):
    owner = await channel(session)
    url = f"{PREFIX}/channels/{owner.id}/policy-versions"
    assert (await client.get(f"{url}/not-a-uuid")).status_code == 422
    assert (await client.get(f"{url}/{uuid4()}")).status_code == 404
    assert (await client.get(url)).json() == []
    assert (await client.get(f"{PREFIX}/channels/{owner.id}/policy-activations")).json() == []
    assert (await session.scalars(select(DecisionPolicyVersion))).all() == []


def test_router_source_and_registration_expose_only_read_only_policy_routes():
    tree = ast.parse(inspect.getsource(api))
    paths = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or not node.args:
            continue
        path = node.args[0]
        if isinstance(path, ast.Constant) and isinstance(path.value, str) and "/policy-" in path.value:
            assert node.func.attr == "get"
            paths.append(path.value)
    assert len(paths) == 4
    routes = [route for route in api.router.routes
              if "/policy-" in route.path or route.path.endswith("/decision-explanation")]
    assert len(routes) == 5
    assert all(route.methods == {"GET"} for route in routes)
