from __future__ import annotations

import copy
import importlib
from datetime import timedelta

import pytest

from app.services import owned_seed_inventory_history as history
from tests.services.test_owned_seed_inventory_history import (
    NOW, UC, approved_manifest_row, completed_rows, empty_rows, iso, manifest, snap, uid,
)


def feedback_module():
    # A missing implementation is an explicit RED assertion, not a collection error.
    import app.services as services
    assert importlib.util.find_spec("app.services.owned_inventory_feedback"), "Task 5 feedback helper is absent"
    return importlib.import_module(f"{services.__name__}.owned_inventory_feedback")


def settled_inventory(*, state="exhausted", days_ago=6):
    rows = empty_rows()
    data = manifest()
    starts = NOW - timedelta(days=days_ago, hours=2)
    data.update(starts_at=iso(starts), expires_at=iso(starts + timedelta(days=7)))
    inventory = approved_manifest_row(data)
    inventory.update(state=state, approved_at=data["starts_at"], revoked_at=None, succession_released_at=None,
                     starts_at=data["starts_at"], expires_at=data["expires_at"], privacy="unlisted",
                     max_admissions=7, minimum_interval_seconds=86400, hold_reason=None)
    rows["owned_seed_inventories"] = [inventory]
    for index, entry in enumerate(data["entries"]):
        start = NOW - timedelta(days=days_ago-index, hours=1)
        original = completed_rows(start=start)
        replacements = {uid(n): uid(n + 10000 * (index + 1)) for n in range(4, 45)}
        replacements["abcdefghijk"] = f"owned{index:06d}"

        def rebind(value):
            if isinstance(value, dict):
                return {key: rebind(item) for key, item in value.items()}
            if isinstance(value, list):
                return [rebind(item) for item in value]
            if isinstance(value, str):
                for before, after in replacements.items():
                    value = value.replace(before, after)
            return value

        graph = rebind(original)
        graph["production_tasks"][0]["manual_seed_id"] = entry["manual_seed_id"]
        graph["youtube_upload_operations"][0]["content_sha256"] = f"{index+100:064x}"
        for table, records in graph.items():
            if table in {"channel_profiles", "publishing_accounts"}:
                if not index:
                    rows[table] = records
            else:
                rows[table].extend(records)
        rows["owned_seed_inventory_items"].append({
            "id": entry["id"], "inventory_id": inventory["id"], "platform_channel_id": UC,
            "asset_id": entry["asset_id"], "manual_seed_id": entry["manual_seed_id"],
            "content_sha256": entry["content_sha256"], "ordinal": index+1,
            "production_task_id": graph["production_tasks"][0]["id"],
            "state": "completed" if index < 6 else "reserved",
            "consumed_at": iso(start-timedelta(minutes=2)),
            "completed_at": iso(start+timedelta(minutes=30)) if index < 6 else None,
        })
    rows["channel_profiles"][0].update(enabled=True, dry_run=False, halted_at=None,
        owned_seed_inventory_id=inventory["id"], intake_paused_at=iso(NOW), intake_pause_reason="owned_inventory_exhausted")
    return rows


def assess(rows, **kwargs):
    return feedback_module().assess_owned_inventory_feedback(snap(rows), uid(100), now=NOW, **kwargs)


def test_seventh_settles_without_an_eligible_tick_or_future_metrics():
    rows = settled_inventory()
    before = copy.deepcopy(rows)
    result = assess(rows)
    assert result.hold_reason is None
    assert result.completed_item_ids == (uid(1006),)
    assert result.metrics["inventory_intake_status"] == "closed"
    assert result.metrics["inventory_settlement_status"] == "complete"
    assert result.metrics["inventory_feedback_status"] == "pending"
    assert result.metrics["inventory_future_metric_count"] > 0
    assert rows == before


def test_full_feedback_requires_all_thirty_five_normal_stage_receipts():
    rows = settled_inventory(days_ago=14)
    result = assess(rows)
    assert result.hold_reason == "owned_inventory_expired"
    assert result.metrics["inventory_settlement_status"] == "complete"
    assert result.metrics["inventory_feedback_status"] == "complete"
    assert result.metrics["inventory_succeeded_metric_count"] == 35
    assert result.metrics["inventory_due_metric_count"] == result.metrics["inventory_future_metric_count"] == 0
    rows["feedback_snapshots"].pop()
    assert assess(rows).metrics["inventory_feedback_status"] == "blocked"


@pytest.mark.parametrize("fault", ["uncertain", "failed", "receipt", "public", "job", "node", "reconcile", "item"])
def test_failed_or_uncertain_item_never_releases_replacement(fault):
    rows = settled_inventory()
    if fault in {"uncertain", "failed"}:
        rows["youtube_upload_operations"][-1]["status"] = fault
    elif fault == "receipt":
        rows["youtube_upload_operations"][-1]["receipt_json"]["video_id"] = "wrong-video"
    elif fault == "public":
        rows["publication_records"][-1]["current_privacy"] = "public"
    elif fault == "job":
        rows["jobs"][-1]["status"] = "FAILED"
    elif fault == "node":
        rows["node_executions"][-1]["status"] = "FAILED"
    elif fault == "reconcile":
        [q for q in rows["channel_ops_queue_items"] if q["kind"] == "reconcile_publication"][-1]["status"] = "failed"
    else:
        rows["owned_seed_inventory_items"][-1]["state"] = "held"
    result = assess(rows)
    assert result.hold_reason is not None
    assert not result.completed_item_ids
    assert result.metrics["inventory_settlement_status"] != "complete"


@pytest.mark.parametrize("fault", ["missing_stage", "failed_stage", "unrelated_feedback"])
def test_every_due_metric_stage_is_checked(fault):
    rows = settled_inventory()
    if fault == "missing_stage":
        rows["publication_metric_schedules"].pop(0)
    elif fault == "failed_stage":
        rows["publication_metric_schedules"][0]["status"] = "failed"
    else:
        rows["feedback_snapshots"][0]["publication_id"] = uid(999999)
    assert assess(rows).hold_reason is not None


def test_exhaustion_and_intake_pause_are_not_emergency_quarantine():
    rows = settled_inventory()
    assert assess(rows).metrics["inventory_feedback_status"] == "pending"
    rows["channel_profiles"][0]["halted_at"] = iso(NOW)
    result = assess(rows)
    assert result.hold_reason == "channel_halted"
    assert result.metrics["inventory_feedback_status"] == "blocked"
    assert not result.completed_item_ids


@pytest.mark.parametrize("state", ["held", "expired", "revoked"])
def test_terminal_intake_does_not_claim_feedback_finished_or_reopen(state):
    rows = settled_inventory(state=state)
    result = assess(rows)
    assert result.metrics["inventory_intake_status"] == "closed"
    assert result.metrics["inventory_feedback_status"] == "pending"
    assert rows["owned_seed_inventories"][0]["state"] == state


def test_missing_health_observation_holds_without_forged_healthy_result():
    result = assess(settled_inventory(), external_conditions=("service_unhealthy",))
    assert result.hold_reason == "service_unhealthy"
    assert result.metrics["inventory_intake_status"] == "closed"


def test_reconciliation_finalizer_is_persisted_proof_only():
    rows = settled_inventory()
    helper = feedback_module().completed_owned_inventory_items
    publication_id = rows["publication_records"][-1]["id"]
    assert helper(snap(rows), uid(100), now=NOW, publication_id=publication_id) == (uid(1006),)
    queue = [q for q in rows["channel_ops_queue_items"] if q["kind"] == "reconcile_publication"][-1]
    queue.update(status="running", locked_by="normal-runner", locked_at=iso(NOW))
    assert helper(snap(rows), uid(100), now=NOW, publication_id=publication_id) == ()
    queue.update(status="succeeded", locked_by=None, locked_at=None)
    rows["owned_seed_inventory_items"][-1].update(state="completed", completed_at=iso(NOW))
    assert helper(snap(rows), uid(100), now=NOW, publication_id=publication_id) == ()


@pytest.mark.parametrize("fault", ["private", "scope", "seed", "future_consumed", "ordinal_gap", "multiple_reserved", "halted"])
def test_reconciliation_finalizer_rejects_binding_and_authority_faults(fault):
    rows = settled_inventory()
    if fault == "private":
        op = rows["youtube_upload_operations"][-1]
        op["privacy"] = op["receipt_json"]["privacy"] = "private"
        [a for a in rows["artifacts"] if a.get("node_execution_id") == op["node_execution_id"]][0]["media_info"]["youtube"]["privacy"] = "private"
    elif fault == "scope":
        rows["production_tasks"][-1]["target_account_id"] = uid(9090)
    elif fault == "seed":
        rows["production_tasks"][-1]["manual_seed_id"] = uid(9090)
    elif fault == "future_consumed":
        rows["owned_seed_inventory_items"][-1]["consumed_at"] = iso(NOW + timedelta(hours=1))
    elif fault == "ordinal_gap":
        rows["owned_seed_inventory_items"][0].update(state="unused", production_task_id=None, consumed_at=None, completed_at=None)
    elif fault == "multiple_reserved":
        rows["owned_seed_inventory_items"][0].update(state="reserved", completed_at=None)
    else:
        rows["channel_profiles"][0]["halted_at"] = iso(NOW)
    with pytest.raises(history.OwnedHistoryError):
        feedback_module().completed_owned_inventory_items(snap(rows), uid(100), now=NOW)
