"""Offline contracts only: no live database, Manager, Redis or admission."""
from __future__ import annotations

import copy
import dataclasses
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql

from app.services import owned_seed_inventory_history as history


NOW = datetime(2026, 9, 11, 8, tzinfo=timezone.utc)
UC = "UC" + "a" * 22
TABLES = (
    "owned_seed_inventories", "owned_seed_inventory_items", "youtube_upload_operations",
    "production_tasks", "publishing_accounts", "channel_profiles", "jobs", "node_executions",
    "artifacts", "assets", "manual_seeds", "publication_records", "publication_metric_schedules",
    "feedback_snapshots", "channel_ops_queue_items", "worker_task_dispatches",
    "worker_task_delivery_attestations", "worker_event_emissions", "registered_worker_event_receipts",
    "registered_worker_event_deliveries", "worker_registrations", "worker_admission_grants",
    "legacy_worker_event_resolutions", "runtime_schedules",
    "publication_promotion_operations",
    "worker_redis_marker_cleanup_authorizations", "worker_redis_marker_repair_audits",
)


def uid(n):
    return f"00000000-0000-0000-0000-{n:012d}"


def iso(at):
    return at.isoformat()


def empty_rows():
    return {name: [] for name in TABLES}


def snap(rows, *, observed_at=NOW, redis=()):
    return history.OwnedHistorySnapshot.from_rows(
        rows, platform_channel_id=UC, observed_at=observed_at, redis_observations=redis,
    )


def assess(rows, *, at=NOW, observed_at=NOW, redis=()):
    return history.assess_owned_history(snap(rows, observed_at=observed_at, redis=redis), now=at)


def test_empty_history_is_not_producer_authority():
    result = assess(empty_rows())
    assert result.block_reason is None
    assert result.classifications == ()
    assert not hasattr(result, "producer_authorized")


def test_snapshot_is_deeply_immutable_and_does_not_alias_input():
    rows = empty_rows()
    rows["channel_profiles"] = [{"id": uid(1), "name": "original", "metadata": {"x": [1]}}]
    snapshot = snap(rows)
    before = snapshot.rows.canonical_json
    rows["channel_profiles"][0]["metadata"]["x"].append(2)
    output = snapshot.rows.as_dict()
    output["channel_profiles"][0]["metadata"]["x"].append(3)
    assert snapshot.rows.canonical_json == before
    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        snapshot.observed_at = NOW - timedelta(days=1)
    assert "original" not in repr(snapshot)


@pytest.mark.parametrize("seconds", [-1, 61])
def test_stale_or_future_database_observation_blocks(seconds):
    result = assess(empty_rows(), observed_at=NOW - timedelta(seconds=seconds))
    assert result.block_reason == "owned_history_observation_stale"


@pytest.mark.parametrize("bad", ["missing", "duplicate", "oversized"])
def test_incomplete_or_ambiguous_snapshot_rejected(bad):
    rows = empty_rows()
    if bad == "missing":
        del rows["registered_worker_event_deliveries"]
    elif bad == "duplicate":
        rows["jobs"] = [{"id": uid(1)}, {"id": uid(1)}]
    else:
        rows["jobs"] = [{"id": uid(i)} for i in range(history.MAX_ROWS + 1)]
    with pytest.raises(history.OwnedHistoryError, match="^owned_history_"):
        snap(rows)


@pytest.mark.asyncio
async def test_loader_uses_one_complete_readonly_statement_with_real_model_columns():
    class DB:
        calls = []

        async def scalar(self, statement):
            self.calls.append(statement)
            return {"observed_at": iso(NOW), "rows": empty_rows()}

    db = DB()
    result = await history.load_owned_history_evidence(db, platform_channel_id=UC)
    assert result.observed_at == NOW
    assert result.redis_observations == ()
    assert len(db.calls) == 1
    sql = str(db.calls[0].compile(dialect=postgresql.dialect()))
    assert sql.lstrip().startswith("SELECT ")
    for table in TABLES:
        assert table in sql
    for forbidden in ("FOR UPDATE", "FOR SHARE", "INSERT ", "UPDATE ", "DELETE ", "pg_advisory", "lease_secret_sha256"):
        assert forbidden not in sql
    assert "LIMIT" in sql


@pytest.mark.asyncio
async def test_loader_does_not_expose_database_exception_text():
    class DB:
        async def scalar(self, statement):
            raise RuntimeError("postgresql://secret:password@unreachable")

    with pytest.raises(history.OwnedHistoryError) as error:
        await history.load_owned_history_evidence(DB(), platform_channel_id=UC)
    assert str(error.value) == "owned_history_read_failed"
    assert "password" not in str(error.value)


def test_unbound_and_orphan_operations_block_globally_without_current_membership():
    rows = empty_rows()
    rows["youtube_upload_operations"] = [{"id": uid(1), "production_task_id": uid(2)}]
    assert assess(rows).block_reason == "owned_history_orphan"
    rows["production_tasks"] = [{"id": uid(2), "target_account_id": uid(3), "channel_profile_id": uid(4)}]
    rows["publishing_accounts"] = [{"id": uid(3), "channel_profile_id": uid(4), "platform": "youtube", "platform_account_id": ""}]
    rows["channel_profiles"] = [{"id": uid(4)}]
    assert assess(rows).block_reason == "owned_history_unclassified"


def test_canonical_bytes_do_not_change_with_key_order():
    left = history.FrozenJSON.from_value({"text": "\u4e2d\u6587", "float": 1.0, "nested": {"b": 2, "a": 1}})
    right = history.FrozenJSON.from_value({"nested": {"a": 1, "b": 2}, "float": 1.0, "text": "\u4e2d\u6587"})
    assert left == right
    assert left.canonical_json == '{"float":1.0,"nested":{"a":1,"b":2},"text":"\\u4e2d\\u6587"}'
    assert history.history_sha256(left) == history.history_sha256(right)


@pytest.mark.parametrize("raw", ['{"a":1,"a":2}', '{"x":NaN}', '{"x":"\\ud800"}', '{} {}'])
def test_ambiguous_json_is_rejected(raw):
    with pytest.raises(history.OwnedHistoryError):
        history.FrozenJSON.from_json(raw)


def test_fixture_module_never_loads_a_dsn():
    assert set(TABLES) == set(history.HISTORY_MODELS)


def manifest(version=1):
    entries = []
    for i in range(7):
        provenance = {"rights": "owned", "provenance": "generated", "evidence_reference": "synthetic:test",
                      "evidence_sha256": "a" * 64, "attestation": "Synthetic owned test input"}
        entries.append({"id": uid(1000+i), "ordinal": i+1, "asset_id": uid(1100+i),
                        "manual_seed_id": uid(1200+i), "content_sha256": f"{i+1:064x}", "byte_size": 10,
                        "storage_descriptor": {"id": uid(1100+i), "storage_backend": "local",
                                               "storage_path": f"assets/{i}.mp4", "file_size": 10,
                                               "mime_type": "video/mp4", "media_info_sha256": history.history_sha256({})},
                        "provenance_evidence": provenance, "provenance_sha256": history.history_sha256(provenance),
                        "seed_sha256": "b"*64, "prompt": "Synthetic owned input", "title_seed": ""})
    result = {"version": version, "inventory_id": uid(100), "channel_profile_id": uid(2),
              "topic_lane_id": uid(101), "lane_format_id": uid(102), "target_account_id": uid(3),
              "platform_channel_id": UC, "starts_at": iso(NOW), "expires_at": iso(NOW+timedelta(days=7)),
              "privacy": "unlisted", "max_admissions": 7, "minimum_interval_seconds": 86400,
              "tick_interval_minutes": 1, "configuration_sha256": "c"*64, "entries": entries}
    if version == 2:
        result["legacy_history"] = {"version": 1, "bindings": [], "retired_unassigned_preupload": None}
    return result


@pytest.mark.parametrize("version", [1, 2])
def test_strict_manifest_versions_are_immutable_and_do_not_activate(version):
    data = manifest(version)
    decoded = history.decode_history_manifest(data)
    assert decoded.version == version
    assert decoded.document.as_dict() == data
    assert (decoded.legacy_history is None) == (version == 1)
    data["entries"][0]["prompt"] = "changed"
    assert decoded.document.as_dict()["entries"][0]["prompt"] != "changed"


@pytest.mark.parametrize("change", ["v1_history", "unknown", "privacy", "bool", "duration", "asset", "sha", "subject"])
def test_manifest_decoder_rejects_implicit_upgrade_and_caller_authority(change):
    data = manifest()
    if change == "v1_history":
        data["legacy_history"] = manifest(2)["legacy_history"]
    elif change == "unknown":
        data["version"] = 3
    elif change == "privacy":
        data["privacy"] = "public"
    elif change == "bool":
        data["tick_interval_minutes"] = True
    elif change == "duration":
        data["expires_at"] = iso(NOW+timedelta(days=8))
    elif change == "asset":
        data["entries"][1]["asset_id"] = data["entries"][0]["asset_id"]
    elif change == "sha":
        data["entries"][0]["provenance_sha256"] = "f"*64
    else:
        data["server_subject"] = "caller says safe"
    with pytest.raises(history.OwnedHistoryError, match="^owned_history_manifest_invalid$"):
        history.decode_history_manifest(data)


def test_request_locator_cannot_carry_qualification_authority():
    value = {"operation_id": uid(1), "legacy_account_id": uid(2), "legacy_channel_profile_id": uid(3)}
    assert history.HistoryOperationLocator.parse(value).operation_id == uid(1)
    with pytest.raises(history.OwnedHistoryError):
        history.HistoryOperationLocator.parse({**value, "server_subject": "forged"})


def z(at):
    return at.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def completed_rows(*, start=NOW-timedelta(hours=25), own=False):
    rows = empty_rows()
    rows["channel_profiles"] = [{"id": uid(2)}]
    rows["publishing_accounts"] = [{"id": uid(3), "channel_profile_id": uid(2), "platform": "youtube",
                                    "platform_account_id": UC, "credential_ref": "fixture-reference",
                                    "platform_specific_config_json": {}}]
    rows["production_tasks"] = [{"id": uid(4), "target_account_id": uid(3), "channel_profile_id": uid(2),
                                 "manual_seed_id": uid(5), "job_id": uid(6), "state": "measured",
                                 "retry_count": 0, "failure_reason": None, "blocked_by_guard": None}]
    receipt = {"video_id": "abcdefghijk", "url": "https://www.youtube.com/watch?v=abcdefghijk",
               "title": "owned", "privacy": "unlisted", "tags": [], "quota_estimate": 1600}
    rows["youtube_upload_operations"] = [{"id": uid(7), "production_task_id": uid(4), "job_id": uid(6),
        "node_execution_id": uid(8), "input_artifact_id": uid(9), "content_sha256": "d"*64,
        "status": "succeeded", "privacy": "unlisted", "title": "owned", "manager_task_id": uid(10),
        "platform_video_id": "abcdefghijk", "receipt_json": receipt, "error_message": None,
        "request_attempted_at": iso(start-timedelta(minutes=1)), "completed_at": iso(start)}]
    rows["jobs"] = [{"id": uid(6), "status": "SUCCEEDED", "completed_at": iso(start), "error_message": None}]
    rows["node_executions"] = [{"id": uid(8), "job_id": uid(6), "node_type": "youtube_upload", "status": "SUCCEEDED",
        "input_artifact_ids": [uid(9)], "output_artifact_id": uid(11), "completed_at": iso(start), "error_message": None}]
    rows["artifacts"] = [{"id": uid(11), "job_id": uid(6), "node_execution_id": uid(8), "media_info": {"youtube": receipt}},
                         {"id": uid(9), "job_id": uid(6), "node_execution_id": uid(12), "media_info": {}}]
    rows["publication_records"] = [{"id": uid(13), "production_task_id": uid(4), "account_id": uid(3), "platform": "youtube",
        "platform_content_id": "abcdefghijk", "current_privacy": "unlisted", "desired_privacy": "unlisted", "public_at": None,
        "publish_status": "uploaded", "uploaded_at": iso(start), "scheduled_publish_at": iso(start)}]
    def queue(i, kind, key, payload, at, parent=None, status="succeeded"):
        return {"id": uid(i), "channel_profile_id": uid(2), "kind": kind, "idempotency_key": key,
                "payload_json": payload, "run_after": iso(at), "parent_queue_item_id": parent, "status": status,
                "attempt_count": 0 if status == "queued" else 1, "locked_at": None, "locked_by": None,
                "last_error": None, "dead_letter_at": None}
    pub = uid(13)
    rows["channel_ops_queue_items"] = [
        queue(14, "promote_publication", f"promote_publication:{pub}", {"publication_id": pub, "target_visibility": "unlisted"}, start),
        queue(15, "reconcile_publication", f"reconcile_publication:{pub}:{z(start)}", {"publication_id": pub}, start+timedelta(minutes=30), uid(14)),
    ]
    for i, (stage, due, grace) in enumerate((("1h", 1, 3), ("6h", 6, 12), ("24h", 24, 30), ("72h", 72, 84), ("7d", 168, 192))):
        due_at = start+timedelta(hours=due)
        done = due_at <= NOW
        rows["publication_metric_schedules"].append({"id": uid(20+i), "publication_id": pub, "snapshot_stage": stage,
            "effective_start_at": iso(start), "due_at": iso(due_at), "grace_until": iso(start+timedelta(hours=grace)),
            "status": "succeeded" if done else "pending", "attempt_count": int(done), "last_error_code": None,
            "completed_at": iso(due_at) if done else None, "last_attempt_at": iso(due_at) if done else None})
        rows["channel_ops_queue_items"].append(queue(30+i, "collect_metrics", f"collect_metrics:{pub}:stage:{stage}:attempt:0",
            {"publication_id": pub, "metric_schedule_id": uid(20+i), "snapshot_stage": stage, "metrics_poll_count": 0},
            due_at, uid(14), "succeeded" if done else "queued"))
        if done:
            rows["feedback_snapshots"].append({"id": uid(40+i), "publication_id": pub, "snapshot_stage": stage})
    if own:
        data = manifest()
        data.update(starts_at=iso(NOW-timedelta(days=2)), expires_at=iso(NOW+timedelta(days=5)))
        data["entries"][0].update(id=uid(50), manual_seed_id=uid(5))
        rows["owned_seed_inventories"] = [approved_manifest_row(data)]
        rows["owned_seed_inventories"][0]["approved_at"] = data["starts_at"]
        rows["owned_seed_inventory_items"] = [{"id": uid(50), "inventory_id": uid(100), "platform_channel_id": UC,
            "production_task_id": uid(4), "manual_seed_id": uid(5), "state": "reserved",
            "asset_id": data["entries"][0]["asset_id"], "content_sha256": data["entries"][0]["content_sha256"]}]
    return rows


def test_normal_completed_publication_and_stable_queue_lease_churn():
    rows = completed_rows(own=True)
    result = assess(rows)
    assert result.block_reason is None and result.wait_reason is None
    assert result.account_ids == (uid(3),)
    assert result.completed_item_ids == (uid(50),)
    assert result.classifications[0].classification == "direct"
    digest = result.stable_history_sha256
    queue = rows["channel_ops_queue_items"][2+3]
    queue.update(status="running", attempt_count=1, locked_by="worker", locked_at=iso(NOW))
    later = assess(rows)
    assert later.block_reason is None
    assert later.stable_history_sha256 == digest


@pytest.mark.parametrize("table,key,value", [
    ("youtube_upload_operations", "status", "submitted"),
    ("youtube_upload_operations", "request_attempted_at", None),
    ("youtube_upload_operations", "manager_task_id", "unknown"),
    ("youtube_upload_operations", "privacy", "public"),
    ("jobs", "status", "CANCELLED"), ("node_executions", "status", "RUNNING"),
    ("production_tasks", "failure_reason", "failed"),
    ("publication_records", "current_privacy", "private"),
    ("publication_records", "current_privacy", "public"),
    ("publication_records", "platform_content_id", "other_video"),
    ("publication_records", "publish_status", "unknown"),
    ("channel_ops_queue_items", "last_error", "failed"),
])
def test_normal_history_fails_closed_on_outcome_and_identity_drift(table, key, value):
    rows = completed_rows()
    rows[table][0][key] = value
    assert assess(rows).block_reason is not None


def pending_promotion_rows():
    start = NOW-timedelta(minutes=2)
    rows = completed_rows(start=start, own=True)
    rows["publication_records"][0]["scheduled_publish_at"] = None
    rows["production_tasks"][0]["state"] = "uploaded_private"
    rows["publication_metric_schedules"] = []
    rows["feedback_snapshots"] = []
    promote = rows["channel_ops_queue_items"][0]
    due = start+timedelta(hours=1)
    promote.update(status="queued", attempt_count=0, run_after=iso(due), parent_queue_item_id=uid(60),
                   idempotency_key=f"promote_publication:{uid(13)}:unlisted:{z(due)}")
    promote["payload_json"]["scheduled_at"] = z(due)
    parent = copy.deepcopy(promote)
    parent.update(id=uid(60), kind="publish_task", status="succeeded", attempt_count=1,
                  payload_json={"production_task_id": uid(4)}, parent_queue_item_id=None)
    rows["channel_ops_queue_items"] = [promote, parent]
    return rows


def test_exact_pending_normal_unlisted_promotion_waits():
    rows = pending_promotion_rows()
    result = assess(rows)
    assert result.block_reason is None and result.wait_reason == "owned_inventory_outstanding"
    assert result.completed_item_ids == ()
    rows["channel_ops_queue_items"][0]["payload_json"]["target_visibility"] = "public"
    assert assess(rows).block_reason is not None


def metric_retry(rows, *, recovered=False):
    metric = rows["publication_metric_schedules"][2]
    due = history._time(metric["due_at"])
    metric.update(status="pending", attempt_count=1, completed_at=None, last_error_code="metrics_unavailable",
                  last_attempt_at=iso(due))
    rows["feedback_snapshots"] = [r for r in rows["feedback_snapshots"] if r["snapshot_stage"] != "24h"]
    original = rows["channel_ops_queue_items"][4]
    retry = copy.deepcopy(original)
    retry.update(id=uid(61), idempotency_key=original["idempotency_key"][:-1]+"1", parent_queue_item_id=original["id"],
                 status="queued", attempt_count=0, run_after=iso(due+timedelta(minutes=15)))
    retry["payload_json"]["metrics_poll_count"] = 1
    if recovered:
        metric.update(status="succeeded", attempt_count=2, completed_at=retry["run_after"],
                      last_attempt_at=retry["run_after"], last_error_code=None)
        retry.update(status="succeeded", attempt_count=1)
        rows["feedback_snapshots"].append({"id": uid(62), "publication_id": uid(13), "snapshot_stage": "24h"})
    rows["channel_ops_queue_items"].append(retry)
    return retry


@pytest.mark.parametrize("recovered", [False, True])
def test_authentic_metric_retry_chain_waits_or_recovers(recovered):
    rows = completed_rows()
    baseline = assess(rows).stable_history_sha256
    retry = metric_retry(rows, recovered=recovered)
    result = assess(rows)
    assert result.block_reason is None
    assert result.wait_reason == (None if recovered else "owned_inventory_metrics_pending")
    assert result.stable_history_sha256 == baseline
    if recovered:
        retry.update(status="running", locked_at=iso(NOW), locked_by="worker")
    else:
        rows["channel_ops_queue_items"][4].update(status="running", locked_at=iso(NOW), locked_by="worker")
    assert assess(rows).wait_reason == "owned_inventory_metrics_pending"


@pytest.mark.parametrize("bad", ["parent", "key", "payload", "late", "duplicate", "error"])
def test_metric_retry_chain_rejects_wrong_scope_and_uncertainty(bad):
    rows = completed_rows()
    retry = metric_retry(rows)
    if bad == "parent":
        retry["parent_queue_item_id"] = uid(99)
    elif bad == "key":
        retry["idempotency_key"] += ":other"
    elif bad == "payload":
        retry["payload_json"]["publication_id"] = uid(99)
    elif bad == "late":
        rows["publication_metric_schedules"][2]["grace_until"] = iso(NOW-timedelta(seconds=1))
    elif bad == "duplicate":
        rows["channel_ops_queue_items"].append({**retry, "id": uid(99)})
    else:
        retry["last_error"] = "uncertain"
    assert assess(rows).block_reason is not None


def approved_manifest_row(data, *, row_id=None):
    return {"id": row_id or data["inventory_id"], "manifest_json": data, "manifest_sha256": history.history_sha256(data),
            "platform_channel_id": data["platform_channel_id"], "target_account_id": data["target_account_id"],
            "channel_profile_id": data["channel_profile_id"], "approved_at": iso(NOW),
            "approved_by": "test-server-operator", "approval_reference": "synthetic:approval", "state": "revoked",
            "revoked_at": iso(NOW), "succession_released_at": iso(NOW)}


def mapped_rows():
    rows = completed_rows()
    account = rows["publishing_accounts"][0]
    account["platform_account_id"] = ""
    op = rows["youtube_upload_operations"][0]
    fact = {"operation_id": op["id"], "manager_task_id": op["manager_task_id"], "platform_video_id": op["platform_video_id"],
            "actual_platform_channel_id": UC, "operation_sha256": history.history_sha256(op),
            "receipt_sha256": history.history_sha256(op["receipt_json"]), "observed_at": iso(NOW)}
    qualification = {"observed_at": iso(NOW), "server_subject": "test-server-operator",
        "manager_endpoint_identity": "sha256:"+"e"*64, "manager_task_id": fact["manager_task_id"],
        "platform_video_id": fact["platform_video_id"], "actual_platform_channel_id": UC,
        "sanitized_facts": [fact], "facts_sha256": history.history_sha256([fact]), "approval_reference": "synthetic:approval"}
    binding = {"legacy_account_id": uid(3), "legacy_channel_profile_id": uid(2), "platform": "youtube",
               "canonical_platform_channel_id": UC, "use": "history_only",
               "account_descriptor_sha256": history.account_descriptor_sha256(account),
               "qualified_operation_ids": [op["id"]], "qualification": qualification}
    data = manifest(2)
    data.update(target_account_id=uid(103), channel_profile_id=uid(104))
    data["legacy_history"]["bindings"] = [binding]
    rows["owned_seed_inventories"] = [approved_manifest_row(data)]
    return rows


def test_approved_revoked_predecessor_keeps_actual_history_membership_without_rewriting_account():
    rows = mapped_rows()
    before = copy.deepcopy(rows)
    result = assess(rows)
    assert result.block_reason is None
    assert result.account_ids == (uid(3),)
    assert result.classifications[0].classification == "history_only"
    assert rows == before and rows["publishing_accounts"][0]["platform_account_id"] == ""
    assert result.authority_sha256 != history.history_sha256([])


@pytest.mark.parametrize("bad", ["draft", "wrong_uc", "missing_fact", "duplicate_fact", "operation_hash", "receipt_hash",
                                 "outer_video", "descriptor", "new_operation", "submitted", "future", "unknown_field"])
def test_history_only_proof_requires_every_actual_succeeded_effect(bad):
    rows = mapped_rows()
    inv = rows["owned_seed_inventories"][0]
    binding = inv["manifest_json"]["legacy_history"]["bindings"][0]
    qualification = binding["qualification"]
    fact = qualification["sanitized_facts"][0]
    if bad == "draft":
        inv.update(approved_at=None, approved_by=None, approval_reference=None, state="draft")
    elif bad == "wrong_uc":
        fact["actual_platform_channel_id"] = "UC"+"b"*22
    elif bad == "missing_fact":
        qualification["sanitized_facts"] = []
    elif bad == "duplicate_fact":
        qualification["sanitized_facts"].append(copy.deepcopy(fact))
    elif bad == "operation_hash":
        fact["operation_sha256"] = "0"*64
    elif bad == "receipt_hash":
        fact["receipt_sha256"] = "0"*64
    elif bad == "outer_video":
        qualification["platform_video_id"] = "other_video"
    elif bad == "descriptor":
        rows["publishing_accounts"][0]["credential_ref"] = "changed-reference"
    elif bad == "new_operation":
        rows["youtube_upload_operations"].append({**rows["youtube_upload_operations"][0], "id": uid(99)})
    elif bad == "submitted":
        rows["youtube_upload_operations"][0]["status"] = "submitted"
        fact["operation_sha256"] = history.history_sha256(rows["youtube_upload_operations"][0])
    elif bad == "future":
        fact["observed_at"] = iso(NOW+timedelta(seconds=1))
    else:
        qualification["safe"] = True
    qualification["facts_sha256"] = history.history_sha256(qualification["sanitized_facts"])
    inv["manifest_sha256"] = history.history_sha256(inv["manifest_json"])
    assert assess(rows).block_reason is not None


def test_conflicting_approved_history_survives_revocation_and_blocks_globally():
    rows = mapped_rows()
    successor = copy.deepcopy(rows["owned_seed_inventories"][0])
    successor["id"] = successor["manifest_json"]["inventory_id"] = uid(105)
    other = "UC"+"b"*22
    successor["manifest_json"]["platform_channel_id"] = other
    successor["platform_channel_id"] = other
    binding = successor["manifest_json"]["legacy_history"]["bindings"][0]
    binding["canonical_platform_channel_id"] = other
    q = binding["qualification"]
    q["actual_platform_channel_id"] = q["sanitized_facts"][0]["actual_platform_channel_id"] = other
    q["facts_sha256"] = history.history_sha256(q["sanitized_facts"])
    successor["manifest_sha256"] = history.history_sha256(successor["manifest_json"])
    rows["owned_seed_inventories"].append(successor)
    assert assess(rows).block_reason == "owned_history_authority_conflict"


def test_empty_platform_alias_uses_execution_fallback_but_other_provider_does_not():
    rows = completed_rows()
    rows["publishing_accounts"][0]["platform"] = ""
    assert assess(rows).block_reason is None
    rows["publishing_accounts"][0]["platform"] = "vimeo"
    assert assess(rows).block_reason == "owned_history_unclassified"


def replacement_rows():
    rows = completed_rows()
    pub = rows["publication_records"][0]
    uploaded = history._time(pub["uploaded_at"])-timedelta(minutes=5)
    pub["uploaded_at"] = iso(uploaded)
    manual = rows["channel_ops_queue_items"][0]
    manual["idempotency_key"] = f"promote_publication:{pub['id']}:unlisted:manual"
    manual["payload_json"]["channel_profile_id"] = uid(2)
    automatic = copy.deepcopy(manual)
    due = uploaded+timedelta(hours=1)
    automatic.update(id=uid(70), status="cancelled", attempt_count=0, last_error="replaced_by_immediate_unlisted_canary_promotion",
        dead_letter_at=iso(uploaded+timedelta(minutes=1)), run_after=iso(due), parent_queue_item_id=uid(71),
        idempotency_key=f"promote_publication:{pub['id']}:unlisted:{z(due)}")
    automatic["payload_json"]["scheduled_at"] = z(due)
    parent = copy.deepcopy(manual)
    parent.update(id=uid(71), kind="publish_task", payload_json={"production_task_id": uid(4)})
    rows["channel_ops_queue_items"].extend([automatic, parent])
    return rows


def test_structurally_settled_manual_replacement_retains_all_admin_history():
    rows = replacement_rows()
    result = assess(rows)
    assert result.block_reason is None
    rows["channel_ops_queue_items"][-2]["dead_letter_at"] = iso(NOW-timedelta(hours=25, minutes=3))
    changed = assess(rows)
    assert changed.block_reason is None and changed.stable_history_sha256 != result.stable_history_sha256


@pytest.mark.parametrize("bad", ["manual_video", "manual_privacy", "parent", "outcome", "reason_only", "automatic_attempt", "own"])
def test_replacement_is_not_a_string_only_failure_exemption(bad):
    rows = replacement_rows()
    manual = rows["channel_ops_queue_items"][0]
    auto = rows["channel_ops_queue_items"][-2]
    if bad == "manual_video":
        manual["payload_json"]["publication_id"] = uid(99)
    elif bad == "manual_privacy":
        manual["payload_json"]["target_visibility"] = "public"
    elif bad == "parent":
        rows["channel_ops_queue_items"][1]["parent_queue_item_id"] = auto["id"]
    elif bad == "outcome":
        manual["status"] = "failed"
    elif bad == "reason_only":
        auto["idempotency_key"] = "something_else"
    elif bad == "automatic_attempt":
        auto["attempt_count"] = 1
    else:
        rows["owned_seed_inventory_items"] = completed_rows(own=True)["owned_seed_inventory_items"]
    assert assess(rows).block_reason is not None


C25 = {"operation_id": "c25b9c38-b96a-4a21-80d0-352180cea206", "task_id": "70d27dfb-f0c5-438c-bdfa-5316dc4f209b",
       "job_id": "8061df32-3184-4c99-a5aa-556744a43ba5", "upload_node_id": "4c1b523f-0a35-45fd-990f-095e8156de2e",
       "legacy_account_id": "2c4184d5-a02e-41e3-aeeb-16db8122f6e1", "legacy_channel_profile_id": "4057a1a3-c37c-4bae-85a9-6d9d3dcac869"}


def full_row(table, **values):
    """Synthetic full-column records, never observations of the real c25 graph."""
    result = {}
    for column in history.HISTORY_MODELS[table].__table__.columns:
        if column.name in {"lease_secret_sha256", "token_sha256"}:
            continue
        if column.nullable:
            result[column.name] = None
            continue
        kind = column.type.python_type
        result[column.name] = ({str: "", int: 0, float: 0.0, bool: False, dict: {}, list: [],
                               uuid.UUID: uid(900), datetime: iso(NOW-timedelta(days=4))}.get(kind))
    result.update(values)
    return result


def retired_rows():
    """Four native terminal paths in a synthetic graph, not a live certificate."""
    rows = empty_rows()
    base = NOW-timedelta(days=4)
    cancel = base+timedelta(minutes=10)
    jid, tid, aid, cid, upload = (C25[k] for k in ("job_id", "task_id", "legacy_account_id", "legacy_channel_profile_id", "upload_node_id"))
    rows["publishing_accounts"] = [full_row("publishing_accounts", id=aid, channel_profile_id=cid,
        platform="youtube", platform_account_id="", credential_ref="synthetic", platform_specific_config_json={},
        default_privacy="unlisted", enabled=True)]
    rows["channel_profiles"] = [full_row("channel_profiles", id=cid, halted_at=iso(cancel),
        halt_reason="operator_canary_failure", intake_paused_at=iso(base), intake_pause_reason="operator_canary")]
    rows["manual_seeds"] = [full_row("manual_seeds", id=uid(201), channel_profile_id=cid, target_account_id=aid,
        constraints_json={"input_asset_id": uid(202)}, status="exhausted")]
    rows["assets"] = [full_row("assets", id=uid(202), filename="owned.mp4", original_name="owned.mp4", mime_type="video/mp4",
        file_size=100, storage_backend="local", storage_path="assets/owned.mp4", media_info={"license": "owned", "provenance": "generated"})]
    rows["production_tasks"] = [full_row("production_tasks", id=tid, target_account_id=aid, channel_profile_id=cid,
        manual_seed_id=uid(201), job_id=jid, state="held", failure_reason="operator_canary_failure",
        blocked_by_guard="operator_canary_failure", state_updated_at=iso(cancel), retry_count=0,
        transition_history_json=[{"from": "producing", "to": "held", "actor": "operator_canary_failure", "at": iso(cancel)}])]
    pipeline_nodes = []
    for i, (node_id, name, kind, status) in enumerate(((uid(203), "source_1", "source", "SUCCEEDED"),
            (uid(204), "transcode_1", "transcode", "SUCCEEDED"), (upload, "youtube_upload_1", "youtube_upload", "CANCELLED"),
            (uid(205), "unused_trim", "trim", "CANCELLED"))):
        config = {"asset_id": uid(202)} if kind == "source" else {}
        pipeline_nodes.append({"id": name, "type": kind, "position": {"x": i*100, "y": 0}, "data": {"config": config}})
        rows["node_executions"].append(full_row("node_executions", id=node_id, job_id=jid, node_id=name,
            node_type=kind, node_config=config, status=status, progress=100 if status == "SUCCEEDED" else 0,
            started_at=iso(base+timedelta(minutes=i)) if name != "unused_trim" else None,
            completed_at=iso(cancel) if status == "CANCELLED" else iso(base+timedelta(minutes=i, seconds=30)),
            error_message="operator_canary_failure" if status == "CANCELLED" else None,
            input_artifact_ids=[] if kind == "source" else [uid(210 if kind == "transcode" else 211)],
            output_artifact_id=uid(210+i) if status == "SUCCEEDED" else None))
    edges = [{"id": f"e{i}", "source": src, "target": dst, "sourceHandle": "video", "targetHandle": "video"}
             for i, (src, dst) in enumerate((("source_1", "transcode_1"), ("transcode_1", "youtube_upload_1"), ("transcode_1", "unused_trim")))]
    rows["jobs"] = [full_row("jobs", id=jid, pipeline_snapshot={"nodes": pipeline_nodes, "edges": edges},
        status="CANCELLED", started_at=iso(base), completed_at=iso(cancel), retry_count=0, error_message="operator_canary_failure")]
    for index in range(2):
        asset = rows["assets"][0]
        rows["artifacts"].append(full_row("artifacts", id=uid(210+index), job_id=jid, node_execution_id=uid(203+index),
            kind="intermediate", filename="owned.mp4", mime_type="video/mp4", file_size=100, storage_backend="local",
            storage_path="assets/owned.mp4" if index == 0 else "artifacts/render.mp4",
            media_info={"source_asset_id": asset["id"], "asset_id": asset["id"], "original_name": asset["original_name"], **asset["media_info"]} if index == 0 else {}))
    rows["youtube_upload_operations"] = [full_row("youtube_upload_operations", id=C25["operation_id"], production_task_id=tid,
        job_id=jid, node_execution_id=upload, input_artifact_id=uid(211), content_sha256="d"*64, title="owned",
        privacy="unlisted", status="reserved", receipt_json={})]
    redis = []
    for index, (node_id, kind, event) in enumerate(((uid(204), "ffmpeg_go", "node_completed"), (upload, "youtube_publisher", "node_failed"))):
        regid, grantid, key, dispatchid = uid(220+index), uid(230+index), uid(240+index), uid(250+index)
        attid, emissionid, receiptid, deliveryid = uid(260+index), uid(270+index), uid(280+index), uid(290+index)
        host = "150-publisher" if index else "colima-127"
        instance = uid(300+index)
        worker = f"{kind}-worker@{host}:1:{instance}"
        started = base+timedelta(minutes=index+1)
        stream, group, msg = f"vp:tasks:{kind}", f"{kind}-workers", f"100{index}-0"
        grant = full_row("worker_admission_grants", id=grantid, service_name=f"worker-{kind}", generation=1,
            worker_type=kind, worker_host=host, capabilities_json=[kind], release_commit="a"*40, image_identity="sha256:"+"b"*64,
            database_principal=f"vp_{kind}", redis_stream=stream, redis_group=group, endpoint_bindings_json={},
            state="revoked", activated_at=iso(base-timedelta(hours=1)), issued_at=iso(base-timedelta(hours=2)))
        rows["worker_admission_grants"].append(grant)
        rows["worker_registrations"].append(full_row("worker_registrations", id=regid, grant_id=grantid,
            service_name=grant["service_name"], worker_type=kind, worker_host=host, capabilities_json=[kind],
            worker_instance_id=instance, worker_slot=1, redis_consumer_id=worker, image_identity=grant["image_identity"],
            database_principal=grant["database_principal"], database_fingerprint="c"*64, redis_fingerprint="d"*64,
            storage_fingerprint="e"*64, lease_epoch=2, status="revoked", registered_at=iso(base),
            heartbeat_at=iso(base), lease_expires_at=iso(base+timedelta(minutes=3))))
        node = next(n for n in rows["node_executions"] if n["id"] == node_id)
        node.update(worker_registration_id=regid, worker_lease_epoch=2, started_at=iso(started),
                    worker_id=worker if event == "node_completed" else None)
        payload = {"job_id": jid, "node_execution_id": node_id, "dispatch_key": key, "node_type": node["node_type"],
                   "node_id": node["node_id"], "config": json.dumps(node["node_config"], sort_keys=True),
                   "input_artifacts": json.dumps({"video": node["input_artifact_ids"][0]}),
                   "preferred_hosts": "[]", "affinity_enqueued_at": "123", "affinity_bounces": "0"}
        sha = history.history_sha256(payload)
        dispatch = full_row("worker_task_dispatches", id=dispatchid, origin_receipt_id=None, dispatch_key=key,
            job_id=jid, node_execution_id=node_id, redis_stream=stream, consumer_group=group, payload_sha256=sha, payload_json=payload,
            delivery_state="delivered", delivery_attempted_at=iso(started-timedelta(seconds=2)),
            redis_message_id=msg, resolution_state="acknowledged", acknowledged_at=iso(started+timedelta(seconds=40)),
            delivered_at=iso(started-timedelta(seconds=1)), created_at=iso(base))
        rows["worker_task_dispatches"].append(dispatch)
        claim = {"job_id": jid, "node_execution_id": node_id, "worker_registration_id": regid, "worker_lease_epoch": 2,
                 "worker_id": worker, "worker_started_at": iso(started)}
        att = full_row("worker_task_delivery_attestations", id=attid, redis_stream=stream, consumer_group=group,
            message_id=msg, payload_sha256=sha, dispatch_key=key, **claim, ack_state="acknowledged",
            acknowledged_at=dispatch["acknowledged_at"], ack_event_emission_id=emissionid, attested_at=iso(started))
        rows["worker_task_delivery_attestations"].append(att)
        payload = {"event": event, **{k: str(v) for k, v in claim.items() if k != "worker_started_at"},
                   "started_at": iso(started), "task_stream": stream, "task_group": group, "task_message_id": msg,
                   "task_payload_sha256": sha, "task_dispatch_key": key}
        if event == "node_completed":
            payload["output_artifact_id"] = uid(211)
        else:
            payload["error"] = "synthetic preupload failure"
        esha, emsg = history.history_sha256(payload), f"200{index}-0"
        common = {"redis_stream": "vp:events", "consumer_group": "orchestrator", "message_id": emsg,
                  "payload_sha256": esha, "source_task_attestation_id": attid}
        rows["worker_event_emissions"].append(full_row("worker_event_emissions", id=emissionid, **common,
            payload_json=payload, event_type=event, **claim, emission_state="resolved", prepared_at=iso(started+timedelta(seconds=30)),
            emitted_at=iso(started+timedelta(seconds=31)), resolved_at=iso(started+timedelta(seconds=50))))
        rows["registered_worker_event_receipts"].append(full_row("registered_worker_event_receipts", id=receiptid,
            **common, payload_json=payload, event_type=event, **claim, source_task_stream=stream, source_task_group=group,
            source_task_message_id=msg, application_state="applied", ack_state="acknowledged", source_task_ack_state="acknowledged",
            accepted_at=iso(started+timedelta(seconds=35)), applied_at=iso(started+timedelta(seconds=45)),
            acknowledged_at=iso(started+timedelta(seconds=50)), source_task_acknowledged_at=att["acknowledged_at"]))
        rows["registered_worker_event_deliveries"].append(full_row("registered_worker_event_deliveries", id=deliveryid,
            **common, receipt_id=receiptid, resolution_state="accepted", reason_code=None, ack_state="acknowledged",
            accepted_at=iso(started+timedelta(seconds=35)), acknowledged_at=iso(started+timedelta(seconds=50))))
        redis.extend([{"kind": "task", "redis_stream": stream, "consumer_group": group, "message_id": msg,
            "dispatch_key": key, "payload_sha256": sha, "marker_message_id": msg, "pending_message_ids": [], "observed_at": iso(NOW)},
            {"kind": "event", "redis_stream": "vp:events", "consumer_group": "orchestrator", "message_id": emsg,
             "dispatch_key": None, "payload_sha256": esha, "marker_message_id": None, "pending_message_ids": [], "observed_at": iso(NOW)}])
    original = rows["worker_task_dispatches"][-1]
    retry = copy.deepcopy(original)
    retry.update(id="3546f81c-d1e4-41a3-9133-74353301096c", dispatch_key="4273d592-b175-44b9-8e63-706272de9b3b",
        origin_receipt_id=uid(281), redis_message_id="3000-0", delivered_at=iso(base+timedelta(minutes=4)),
        delivery_attempted_at=iso(base+timedelta(minutes=4)), acknowledged_at=iso(cancel+timedelta(minutes=1)))
    retry["payload_json"]["dispatch_key"] = retry["dispatch_key"]
    retry["payload_sha256"] = history.history_sha256(retry["payload_json"])
    rows["worker_task_dispatches"].append(retry)
    redis.append({"kind": "task", "redis_stream": retry["redis_stream"], "consumer_group": retry["consumer_group"],
        "message_id": retry["redis_message_id"], "dispatch_key": retry["dispatch_key"], "payload_sha256": retry["payload_sha256"],
        "marker_message_id": retry["redis_message_id"], "pending_message_ids": [], "observed_at": iso(NOW)})
    never = copy.deepcopy(original)
    never.update(id=uid(310), dispatch_key=uid(311), node_execution_id=uid(205), delivery_state="pending",
        delivery_attempted_at=None, redis_message_id=None, delivered_at=None, resolution_state="cancelled",
        acknowledged_at=None, cancelled_at=iso(cancel))
    never["payload_json"].update(dispatch_key=uid(311), node_execution_id=uid(205), node_type="trim", node_id="unused_trim")
    never["payload_sha256"] = history.history_sha256(never["payload_json"])
    never.update(redis_stream="vp:tasks:ffmpeg_go", consumer_group="ffmpeg_go-workers")
    rows["worker_task_dispatches"].append(never)
    redis.append({"kind": "task", "redis_stream": never["redis_stream"], "consumer_group": never["consumer_group"],
        "message_id": None, "dispatch_key": never["dispatch_key"], "payload_sha256": never["payload_sha256"],
        "marker_message_id": None, "pending_message_ids": [], "observed_at": iso(NOW)})
    data = manifest(2)
    graph = {name: copy.deepcopy(rows[name]) for name in history.TERMINAL_TABLES}
    for records in graph.values():
        records.sort(key=lambda r: r["id"])
    retained = {"operation": rows["youtube_upload_operations"][0], "task": rows["production_tasks"][0],
                "job": rows["jobs"][0], "upload_node": next(n for n in rows["node_executions"] if n["id"] == upload),
                "account": rows["publishing_accounts"][0], "channel": rows["channel_profiles"][0],
                "manual_seed": rows["manual_seeds"][0],
                "source_assets": [{"asset": rows["assets"][0], "content_sha256": "a"*64}]}
    certificate = {**C25, "classification": "retired_unassigned_preupload", "retained_facts": copy.deepcopy(retained),
                   "terminal_graph": graph, "terminal_graph_sha256": history.history_sha256(graph),
                   "transition_sha256": history.history_sha256(rows["production_tasks"][0]["transition_history_json"]),
                   "observed_at": iso(NOW), "server_subject": "test-server-operator", "approval_reference": "synthetic:approval"}
    data["legacy_history"]["retired_unassigned_preupload"] = certificate
    rows["owned_seed_inventories"] = [approved_manifest_row(data)]
    return rows, redis


def retired_assess(rows, redis):
    return assess(rows, redis=tuple(history.RedisTerminalObservation.parse(v) for v in redis))


def test_exact_retired_tuple_has_four_real_terminal_paths_and_no_uc_or_completion():
    rows, redis = retired_rows()
    before = copy.deepcopy(rows)
    result = retired_assess(rows, redis)
    assert result.block_reason is None
    assert result.classifications[0].classification == "retired_unassigned_preupload"
    assert result.classifications[0].platform_channel_id is None
    assert result.account_ids == result.completed_item_ids == () and result.wait_reason is None
    assert result.retired_source_sha256 == ("a"*64,) and result.retired_render_sha256 == ("d"*64,)
    assert {p.path for p in result.terminal_paths} == {"synchronous_source", "receipt_backed", "never_delivered_cancelled", "delivered_cancelled_ack"}
    retry = next(d for d in rows["worker_task_dispatches"] if d["id"].startswith("3546f81c"))
    assert retry["delivered_at"] and retry["cancelled_at"] is None
    assert not any(a["dispatch_key"] == retry["dispatch_key"] for a in rows["worker_task_delivery_attestations"])
    assert rows == before


@pytest.mark.parametrize("bad", ["attempt", "manager", "video", "receipt", "complete", "fk", "registration", "started",
    "job_running", "node_running", "unhalt", "unpause", "claim", "emission", "delivery", "pending", "marker", "stale",
    "origin", "retry_key", "retry_message", "retry_hash", "retry_no_ack", "retry_authorized", "retry_invented_cancel",
    "source_missing", "payload_missing", "orphan_receipt", "new_job", "new_queue", "no_redis", "source_hash_missing"])
def test_retired_complete_fresh_proof_rejects_every_unsafe_or_missing_fact(bad):
    rows, redis = retired_rows()
    op = rows["youtube_upload_operations"][0]
    upload = next(n for n in rows["node_executions"] if n["id"] == C25["upload_node_id"])
    retry = next(d for d in rows["worker_task_dispatches"] if d["id"].startswith("3546f81c"))
    if bad in {"attempt", "manager", "video", "receipt", "complete", "fk"}:
        key, value = {"attempt": ("request_attempted_at", iso(NOW)), "manager": ("manager_task_id", uid(99)),
            "video": ("platform_video_id", "abcdefghijk"), "receipt": ("receipt_json", {"video_id": "abcdefghijk"}),
            "complete": ("completed_at", iso(NOW)), "fk": ("production_task_id", None)}[bad]
        op[key] = value
    elif bad == "registration":
        upload["worker_registration_id"] = None
    elif bad == "started":
        upload["started_at"] = None
    elif bad == "job_running":
        rows["jobs"][0]["status"] = "RUNNING"
    elif bad == "node_running":
        upload["status"] = "RUNNING"
    elif bad == "unhalt":
        rows["channel_profiles"][0]["halted_at"] = None
    elif bad == "unpause":
        rows["channel_profiles"][0]["intake_paused_at"] = None
    elif bad == "claim":
        rows["worker_task_delivery_attestations"].append({**rows["worker_task_delivery_attestations"][-1], "id": uid(99), "dispatch_key": retry["dispatch_key"]})
    elif bad == "emission":
        rows["worker_event_emissions"].append({**rows["worker_event_emissions"][-1], "id": uid(99), "emission_state": "prepared"})
    elif bad == "delivery":
        rows["registered_worker_event_deliveries"][-1]["ack_state"] = "pending"
    elif bad == "pending":
        redis[-2]["pending_message_ids"] = [retry["redis_message_id"]]
    elif bad == "marker":
        redis[-2]["marker_message_id"] = "9999-0"
    elif bad == "stale":
        redis[-2]["observed_at"] = iso(NOW-timedelta(seconds=61))
    elif bad.startswith("retry_") or bad == "origin":
        key, value = {"origin": ("origin_receipt_id", None), "retry_key": ("dispatch_key", uid(99)),
            "retry_message": ("redis_message_id", "9999-0"), "retry_hash": ("payload_sha256", "f"*64),
            "retry_no_ack": ("acknowledged_at", None), "retry_authorized": ("resolution_state", "cancel_authorized"),
            "retry_invented_cancel": ("cancelled_at", retry["acknowledged_at"])}[bad]
        retry[key] = value
    elif bad == "source_missing":
        rows["assets"] = []
    elif bad == "payload_missing":
        del retry["payload_json"]
    elif bad == "orphan_receipt":
        rows["registered_worker_event_receipts"][-1]["source_task_attestation_id"] = uid(99)
    elif bad == "new_job":
        rows["jobs"].append({**rows["jobs"][0], "id": uid(99), "parent_job_id": C25["job_id"]})
    elif bad == "new_queue":
        rows["channel_ops_queue_items"] = [full_row("channel_ops_queue_items", id=uid(99), channel_profile_id=C25["legacy_channel_profile_id"],
            kind="execute_task", status="queued", payload_json={"production_task_id": C25["task_id"]})]
    elif bad == "source_hash_missing":
        cert = rows["owned_seed_inventories"][0]["manifest_json"]["legacy_history"]["retired_unassigned_preupload"]
        cert["retained_facts"]["source_assets"] = []
        rows["owned_seed_inventories"][0]["manifest_sha256"] = history.history_sha256(rows["owned_seed_inventories"][0]["manifest_json"])
    else:
        redis = []
    assert retired_assess(rows, redis).block_reason is not None


def reseal_synthetic_certificate(rows):
    """Re-hash test inputs so negatives exercise semantics, not only drift guards."""
    inv = rows["owned_seed_inventories"][0]
    cert = inv["manifest_json"]["legacy_history"]["retired_unassigned_preupload"]
    pairs = {"operation": "youtube_upload_operations", "task": "production_tasks", "job": "jobs",
             "account": "publishing_accounts", "channel": "channel_profiles", "manual_seed": "manual_seeds"}
    for key, table in pairs.items():
        cert["retained_facts"][key] = copy.deepcopy(rows[table][0])
    cert["retained_facts"]["upload_node"] = copy.deepcopy(next(n for n in rows["node_executions"] if n["id"] == C25["upload_node_id"]))
    cert["terminal_graph"] = {name: sorted(copy.deepcopy(rows[name]), key=lambda r: r["id"]) for name in history.TERMINAL_TABLES}
    cert["terminal_graph_sha256"] = history.history_sha256(cert["terminal_graph"])
    cert["transition_sha256"] = history.history_sha256(rows["production_tasks"][0]["transition_history_json"])
    inv["manifest_sha256"] = history.history_sha256(inv["manifest_json"])


def receipt_authorized_ack_rows():
    rows, redis = retired_rows()
    att = rows["worker_task_delivery_attestations"][0]
    receipt = rows["registered_worker_event_receipts"][0]
    dispatch = rows["worker_task_dispatches"][0]
    att["ack_event_emission_id"] = None
    ack = iso(history._time(receipt["applied_at"]) + timedelta(seconds=10))
    att["acknowledged_at"] = dispatch["acknowledged_at"] = receipt["source_task_acknowledged_at"] = ack
    reseal_synthetic_certificate(rows)
    return rows, redis


def test_retired_receipt_authorized_ack_accepts_exact_applied_receipt_without_emission_link():
    rows, redis = receipt_authorized_ack_rows()
    before = copy.deepcopy(rows)
    result = retired_assess(rows, redis)
    assert result.block_reason is None
    assert result.classifications[0].classification == "retired_unassigned_preupload"
    assert result.classifications[0].platform_channel_id is None
    assert result.account_ids == result.completed_item_ids == () and result.wait_reason is None
    assert any(p.record_id == uid(250) and p.path == "receipt_backed" for p in result.terminal_paths)
    assert rows == before


@pytest.mark.parametrize("bad", ["missing_receipt", "unapplied_receipt", "foreign_receipt", "wrong_emission", "premature_ack"])
def test_retired_receipt_authorized_ack_rejects_rehashed_invalid_authority(bad):
    rows, redis = receipt_authorized_ack_rows()
    att = rows["worker_task_delivery_attestations"][0]
    receipt = rows["registered_worker_event_receipts"][0]
    if bad == "missing_receipt":
        rows["registered_worker_event_receipts"].remove(receipt)
    elif bad == "unapplied_receipt":
        receipt.update(application_state="accepted", applied_at=None)
    elif bad == "foreign_receipt":
        receipt["source_task_attestation_id"] = rows["worker_task_delivery_attestations"][1]["id"]
    elif bad == "wrong_emission":
        att["ack_event_emission_id"] = rows["worker_event_emissions"][1]["id"]
    else:
        ack = iso(history._time(receipt["applied_at"]) - timedelta(seconds=1))
        att["acknowledged_at"] = rows["worker_task_dispatches"][0]["acknowledged_at"] = receipt["source_task_acknowledged_at"] = ack
    reseal_synthetic_certificate(rows)
    before = copy.deepcopy(rows)
    assert retired_assess(rows, redis).block_reason == "owned_history_retired_receipt"
    assert rows == before


@pytest.mark.parametrize("bad", ["origin_completed", "retry_emission", "missing_ack", "applied", "att_mismatch",
    "payload_event", "wrong_stream", "extra_dispatch", "broken_dependency", "source_path", "source_claim", "grant_image",
    "transition", "invented_cancel", "never_attempted", "missing_column", "cleanup_authority", "orphan_delivery"])
def test_rehashed_retirement_claim_cannot_bypass_native_semantics(bad):
    rows, redis = retired_rows()
    retry = next(d for d in rows["worker_task_dispatches"] if d["id"].startswith("3546f81c"))
    if bad == "origin_completed":
        retry["origin_receipt_id"] = rows["registered_worker_event_receipts"][0]["id"]
    elif bad == "retry_emission":
        emission = copy.deepcopy(rows["worker_event_emissions"][-1])
        emission.update(id=uid(401), source_task_attestation_id=uid(402), emission_state="resolved")
        emission["payload_json"]["task_dispatch_key"] = retry["dispatch_key"]
        rows["worker_event_emissions"].append(emission)
    elif bad == "missing_ack":
        rows["registered_worker_event_deliveries"][0]["acknowledged_at"] = None
    elif bad == "applied":
        rows["registered_worker_event_receipts"][0]["application_state"] = "accepted"
    elif bad == "att_mismatch":
        rows["worker_task_delivery_attestations"][0]["worker_lease_epoch"] = 99
    elif bad == "payload_event":
        rows["registered_worker_event_receipts"][0]["payload_json"]["event"] = "node_failed"
    elif bad == "wrong_stream":
        retry["redis_stream"] = "vp:tasks:vision"
    elif bad == "extra_dispatch":
        duplicate = copy.deepcopy(retry)
        duplicate.update(id=uid(401), dispatch_key=uid(402), redis_message_id="9000-0")
        duplicate["payload_json"]["dispatch_key"] = duplicate["dispatch_key"]
        duplicate["payload_sha256"] = history.history_sha256(duplicate["payload_json"])
        rows["worker_task_dispatches"].append(duplicate)
        redis.append({**redis[-2], "dispatch_key": duplicate["dispatch_key"], "message_id": "9000-0", "marker_message_id": "9000-0", "payload_sha256": duplicate["payload_sha256"]})
    elif bad == "broken_dependency":
        rows["jobs"][0]["pipeline_snapshot"]["edges"][1]["source"] = "missing_node"
    elif bad == "source_path":
        rows["artifacts"][0]["storage_path"] = "artifacts/other.mp4"
    elif bad == "source_claim":
        rows["node_executions"][0]["worker_registration_id"] = rows["worker_registrations"][0]["id"]
    elif bad == "grant_image":
        rows["worker_admission_grants"][0]["image_identity"] = "sha256:"+"f"*64
    elif bad == "transition":
        rows["production_tasks"][0]["transition_history_json"][-1]["from"] = "uploaded_private"
    elif bad == "invented_cancel":
        retry["cancelled_at"] = retry["acknowledged_at"]
    elif bad == "never_attempted":
        never = rows["worker_task_dispatches"][-1]
        never["delivery_attempted_at"] = never["cancelled_at"]
    elif bad == "missing_column":
        del retry["payload_json"]
    elif bad == "cleanup_authority":
        rows["worker_redis_marker_cleanup_authorizations"].append(full_row("worker_redis_marker_cleanup_authorizations",
            id=uid(401), marker_kind="task_dispatch", source_id=retry["id"], marker_key="synthetic-marker",
            redis_stream=retry["redis_stream"], expected_message_id=retry["redis_message_id"], payload_sha256=retry["payload_sha256"],
            authorization_state="pending"))
    else:
        rows["registered_worker_event_deliveries"].append({**rows["registered_worker_event_deliveries"][0], "id": uid(401), "receipt_id": uid(402)})
    reseal_synthetic_certificate(rows)
    assert retired_assess(rows, redis).block_reason is not None


def test_missing_certificate_and_v1_cannot_retire_c25():
    rows, redis = retired_rows()
    rows["owned_seed_inventories"] = []
    assert retired_assess(rows, redis).block_reason == "owned_history_unclassified"
    rows["owned_seed_inventories"] = [approved_manifest_row(manifest())]
    assert retired_assess(rows, redis).block_reason == "owned_history_unclassified"


def test_history_only_account_cannot_grow_a_new_task_even_if_already_held():
    rows = mapped_rows()
    rows["production_tasks"].append({**rows["production_tasks"][0], "id": uid(401), "state": "held", "job_id": None})
    assert assess(rows).block_reason == "owned_history_membership_changed"


def test_inventory_item_without_approved_manifest_cannot_claim_normal_outstanding_exemption():
    rows = pending_promotion_rows()
    rows["owned_seed_inventories"] = []
    assert assess(rows).block_reason == "owned_history_item_authority"


def test_history_timestamp_observations_and_row_order_do_not_change_stable_effects():
    rows = mapped_rows()
    before = assess(rows)
    for records in rows.values():
        records.reverse()
    later = assess(rows, at=NOW+timedelta(seconds=1), observed_at=NOW+timedelta(seconds=1))
    assert before.block_reason is later.block_reason is None
    assert before.stable_history_sha256 == later.stable_history_sha256
    assert before.authority_sha256 == later.authority_sha256


def test_public_create_schema_still_rejects_any_v2_authority_payload():
    from app.schemas.channel_agent import OwnedSeedInventoryCreate
    from pydantic import ValidationError
    data = manifest(2)
    with pytest.raises(ValidationError):
        OwnedSeedInventoryCreate.model_validate(data)


@pytest.mark.parametrize("name", ["direct", "history_only", "retired_unassigned", "pending_promotion",
                                  "metrics_pending_retry", "metrics_recovered", "promotion_replacement", "v1_unclassified"])
def test_cross_language_golden_snapshot_decisions_and_hashes(name):
    path = Path(__file__).parents[1] / "fixtures" / "owned_seed_inventory_history" / f"{name}.json"
    value = history.FrozenJSON.from_json(path.read_text()).as_dict()
    assert value["synthetic"] is True and value["contract_version"] == 1
    snapshot = history.OwnedHistorySnapshot.from_rows(value["rows"], platform_channel_id=value["platform_channel_id"],
        observed_at=history._time(value["observed_at"]),
        redis_observations=tuple(history.RedisTerminalObservation.parse(r) for r in value["redis_observations"]))
    result = history.assess_owned_history(snapshot, now=history._time(value["now"]))
    assert history.FrozenJSON.from_value(dataclasses.asdict(result)).as_dict() == value["expected"]
    assert history.history_sha256(snapshot.rows) == value["snapshot_sha256"]
    vector = value["canonical_vector"]
    assert history.FrozenJSON.from_value(vector["value"]).canonical_json == vector["canonical_ascii"]
    assert history.history_sha256(vector["value"]) == vector["sha256"]


@pytest.mark.parametrize("value", [{1: "value"}, {"nested": ({1: "value"},)}])
def test_python_non_json_keys_are_not_coerced_into_authority(value):
    with pytest.raises(history.OwnedHistoryError, match="owned_history_invalid_json"):
        history.FrozenJSON.from_value(value)


def test_invalid_observation_timestamp_has_only_static_error_text():
    with pytest.raises(history.OwnedHistoryError, match="^owned_history_invalid$"):
        snap(empty_rows(), observed_at="private-sentinel-not-a-timestamp")
