"""Retained terminal facts qualify history, never reconstructed queue rows."""

import copy
import json
from datetime import datetime
from pathlib import Path

import pytest

from app.services import owned_seed_inventory_history as history
from app.services import owned_seed_inventory as inventory
from app.schemas.channel_agent import OwnedHistoryLocators

FIXTURE = Path(__file__).parents[1] / "fixtures/owned_seed_inventory_retention_gap.json"


def fixture():
    return json.loads(FIXTURE.read_text())


def snapshot(f):
    return history.OwnedHistorySnapshot.from_rows(
        f["rows"],
        platform_channel_id=f["platform_channel_id"],
        observed_at=datetime.fromisoformat(f["observed_at"]),
    )


def assess(f):
    return history.assess_owned_history(
        snapshot(f), now=datetime.fromisoformat(f["now"])
    )


def test_certificate_qualifies_terminal_legacy_without_new_item_credit():
    f = fixture()
    result = assess(f)
    assert result.block_reason is None
    assert result.wait_reason is None and result.completed_item_ids == ()
    assert result.classifications[0].classification == "history_only"


@pytest.mark.parametrize("active", ["none", "current_tick", "dirty_tick", "other_tick", "legacy_metric"])
def test_retained_history_queue_admission(active):
    from app.channel_agent.owned_inventory import queues_safe

    f = fixture()
    rows = f["rows"]
    channel = rows["owned_seed_inventories"][0]["channel_profile_id"]
    if active != "none":
        queue = copy.deepcopy(rows["channel_ops_queue_items"][0])
        queue.update(
            id="00000000-0000-0000-0000-000000000999",
            channel_profile_id=channel,
            kind="agent_tick", status="running", attempt_count=1,
            last_error=None, dead_letter_at=None, parent_queue_item_id=None,
            locked_at=f["now"], locked_by="current-runner",
            payload_json={"channel_id": channel},
        )
        if active == "dirty_tick":
            queue["attempt_count"] = 2
        elif active == "other_tick":
            queue["channel_profile_id"] = "00000000-0000-0000-0000-000000000998"
            queue["payload_json"]["channel_id"] = queue["channel_profile_id"]
        elif active == "legacy_metric":
            queue.update(kind="collect_metrics", channel_profile_id=rows["production_tasks"][0]["channel_profile_id"],
                         payload_json={"publication_id": rows["publication_records"][0]["id"]})
        rows["channel_ops_queue_items"].append(queue)
    result = assess(f)
    assert queues_safe(rows, result, channel, datetime.fromisoformat(f["now"])) is (
        active in {"none", "current_tick"}
    )


@pytest.mark.parametrize("mutation", ["missing", "unapproved", "changed_fact"])
def test_queue_admission_requires_current_valid_retained_authority(mutation):
    from app.channel_agent.owned_inventory import queues_safe

    f = fixture()
    rows = f["rows"]
    inventory_row = rows["owned_seed_inventories"][0]
    if mutation == "missing":
        inventory_row["manifest_json"]["legacy_history"].pop("retained_terminal_uploads")
        inventory_row["manifest_sha256"] = history.history_sha256(inventory_row["manifest_json"])
    elif mutation == "unapproved":
        inventory_row["approved_at"] = None
    else:
        rows["feedback_snapshots"][0]["views"] = 987654
    result = assess(f)
    assert result.block_reason is not None
    assert not queues_safe(rows, result, inventory_row["channel_profile_id"], datetime.fromisoformat(f["now"]))


def test_native_locators_capture_actual_retained_facts_without_certificate_input():
    f = fixture()
    row = f["rows"]["owned_seed_inventories"].pop()
    cert = row["manifest_json"]["legacy_history"]["retained_terminal_uploads"][0]
    locators = OwnedHistoryLocators.model_validate(
        {
            "operations": [
                {
                    "operation_id": cert["operation_id"],
                    "legacy_account_id": cert["legacy_account_id"],
                    "legacy_channel_profile_id": cert["legacy_channel_profile_id"],
                }
            ]
        }
    )
    groups = inventory._history_sources(snapshot(f), locators, row["target_account_id"])
    assert (
        groups[cert["legacy_account_id"]]["retained_terminal_uploads"][0]
        == cert["retained_facts"]
    )


def test_without_certificate_missing_queue_history_still_blocks():
    f = fixture()
    row = f["rows"]["owned_seed_inventories"][0]
    section = row["manifest_json"]["legacy_history"]
    section.pop("retained_terminal_uploads")
    section["version"] = 1
    row["manifest_sha256"] = history.history_sha256(row["manifest_json"])
    assert assess(f).block_reason is not None


def test_other_terminal_cancellation_on_shared_legacy_channel_is_not_this_history():
    f = fixture()
    other = copy.deepcopy(f["rows"]["channel_ops_queue_items"][0])
    other.update(id="00000000-0000-0000-0000-000000000999", parent_queue_item_id=None)
    other["payload_json"]["publication_id"] = "00000000-0000-0000-0000-000000000998"
    f["rows"]["channel_ops_queue_items"].append(other)
    assert assess(f).block_reason is None
    other["status"] = "queued"
    assert assess(f).block_reason is not None


def test_queue_retention_age_uses_old_upload_not_later_metric_completion():
    from app.services.owned_seed_inventory_retention import retained_terminal_facts

    f = fixture()
    # Queues for all five metric stages were created with promotion. A 7d
    # completion can be younger than thirty days when its queue was eligible.
    now = datetime.fromisoformat("2026-10-12T08:00:00+00:00")
    assert retained_terminal_facts(f["rows"], f["rows"]["production_tasks"][0], now)


def reseal(f):
    # Synthetic adversarial certificates exercise semantic gates, not only hashes.
    rows = f["rows"]
    inv = rows["owned_seed_inventories"][0]
    c = inv["manifest_json"]["legacy_history"]["retained_terminal_uploads"][0]
    facts = history._task_history(rows, rows["production_tasks"][0])
    facts["promotion_operations"] = copy.deepcopy(
        rows["publication_promotion_operations"]
    )
    for value in facts.values():
        if isinstance(value, list):
            value.sort(key=lambda row: row["id"])
    c["retained_facts"] = copy.deepcopy(facts)
    c["retained_facts_sha256"] = history.history_sha256(facts)
    q = inv["manifest_json"]["legacy_history"]["bindings"][0]["qualification"]
    q["sanitized_facts"][0]["operation_sha256"] = history.history_sha256(
        rows["youtube_upload_operations"][0]
    )
    q["facts_sha256"] = history.history_sha256(q["sanitized_facts"])
    inv["manifest_sha256"] = history.history_sha256(inv["manifest_json"])


CASES = json.loads(
    FIXTURE.with_name("owned_seed_inventory_retention_gap_cases.json").read_text()
)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_semantically_invalid_retained_outcome_blocks_even_with_matching_hash(case):
    f = fixture()
    for change in case["changes"]:
        target = f
        for key in change["path"][:-1]:
            target = target[key]
        target[change["path"][-1]] = change["value"]
    reseal(f)
    assert assess(f).block_reason is not None


@pytest.mark.parametrize(
    "case",
    [case for case in CASES if case["name"].startswith("pending_delivery_")],
    ids=lambda case: case["name"],
)
def test_related_acknowledged_deliveries_are_terminal(case):
    f = fixture()
    for change in case["changes"]:
        target = f
        for key in change["path"][:-1]:
            target = target[key]
        target[change["path"][-1]] = copy.deepcopy(change["value"])
    for delivery in f["rows"]["registered_worker_event_deliveries"]:
        delivery["ack_state"] = "acknowledged"
    assert assess(f).block_reason is None


@pytest.mark.parametrize(
    "control", ["disabled", "dry_run", "halted_at", "intake_paused_at"]
)
def test_each_explicit_stopped_legacy_control_is_accepted(control):
    f = fixture()
    channel = f["rows"]["channel_profiles"][0]
    channel["enabled"] = True
    if control == "disabled":
        channel["enabled"] = False
    elif control == "dry_run":
        channel["dry_run"] = True
    else:
        channel[control] = f["now"]
    assert assess(f).block_reason is None


@pytest.mark.parametrize(
    "table,field",
    [
        ("publication_promotion_operations", "evidence_json"),
        ("feedback_snapshots", "views"),
        ("channel_ops_queue_items", "payload_json"),
    ],
)
def test_surviving_fact_mutation_is_not_replaced_by_stored_certificate(table, field):
    f = fixture()
    f["rows"][table][0][field] = {"changed": True}
    assert assess(f).block_reason is not None


def test_certificates_never_authorize_unapproved_or_new_account_membership():
    f = fixture()
    f["rows"]["owned_seed_inventories"][0]["approved_at"] = None
    assert assess(f).block_reason is not None
    f = fixture()
    extra = copy.deepcopy(f["rows"]["production_tasks"][0])
    extra["id"] = "00000000-0000-0000-0000-000000000999"
    f["rows"]["production_tasks"].append(extra)
    assert assess(f).block_reason == "owned_history_membership_changed"
