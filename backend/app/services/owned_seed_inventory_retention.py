"""Native-only qualification of old terminal uploads with unavailable queue rows.

This records surviving facts, never reconstructed execution history. New owned
items always use the original queue-chain contract. No caller evidence is read.
"""

from datetime import timedelta

from app.services import owned_seed_inventory_history as h

CERTIFICATE_FIELDS = "version classification operation_id task_id publication_id legacy_account_id legacy_channel_profile_id observed_at server_subject approval_reference retained_facts retained_facts_sha256"
FACT_FIELDS = "task operations job nodes artifacts publications queues metrics feedback promotion_operations"
REASON = "owned_history_retained_terminal_invalid"


def require(condition):
    h._require(condition, REASON)


def parse_certificate(value):
    h._exact(value, CERTIFICATE_FIELDS)
    require(
        type(value["version"]) is int
        and value["version"] == 1
        and value["classification"] == "retained_terminal_upload_queue_gap"
    )
    for name in (
        "operation_id",
        "task_id",
        "publication_id",
        "legacy_account_id",
        "legacy_channel_profile_id",
    ):
        h._id(value[name])
    h._utc_fact(value["observed_at"])
    h._text(value["server_subject"])
    h._text(value["approval_reference"])
    facts = h._exact(value["retained_facts"], FACT_FIELDS)
    for name in FACT_FIELDS.split():
        if name in {"task", "job"}:
            require(isinstance(facts[name], dict))
            h._id(facts[name]["id"])
        else:
            require(isinstance(facts[name], list) and len(facts[name]) <= 128)
            ids = [h._id(row["id"]) for row in facts[name]]
            require(ids == sorted(set(ids)))
    require(h.history_sha256(facts) == h._hash(value["retained_facts_sha256"]))
    require(
        len(h.FrozenJSON.from_value(value).canonical_json.encode("ascii"))
        <= 1024 * 1024
    )
    return h.FrozenJSON.from_value(value)


def retained_terminal_facts(rows, task, now):
    """Validate surviving terminal outcomes without manufacturing absent queues."""
    channel = h._one(
        [r for r in rows["channel_profiles"] if r["id"] == task["channel_profile_id"]],
        REASON,
    )
    account = h._one(
        [
            r
            for r in rows["publishing_accounts"]
            if r["id"] == task["target_account_id"]
        ],
        REASON,
    )
    require(account["channel_profile_id"] == channel["id"])
    require(type(channel["enabled"]) is bool and type(channel["dry_run"]) is bool)
    for key in ("halted_at", "intake_paused_at"):
        require(channel[key] is None or h._time(channel[key]) <= now)
    require(
        not channel["enabled"]
        or channel["dry_run"]
        or channel["halted_at"] is not None
        or channel["intake_paused_at"] is not None
    )
    facts = h._task_history(rows, task)
    op = h._one(facts["operations"], REASON)
    job = facts["job"]
    pub = h._one(facts["publications"], REASON)
    facts["promotion_operations"] = [
        r
        for r in rows["publication_promotion_operations"]
        if r.get("production_task_id") == task["id"]
        or r.get("publication_id") == pub["id"]
    ]
    promotion = h._one(facts["promotion_operations"], REASON)
    require(
        task["state"] == "measured"
        and type(task.get("retry_count")) is int
        and task.get("retry_count") == 0
        and task.get("failure_reason") is None
        and task.get("blocked_by_guard") is None
        and task.get("failure_category") in (None, "")
    )
    require(
        not any(
            i.get("production_task_id") == task["id"]
            for i in rows["owned_seed_inventory_items"]
        )
    )
    require(
        all(
            "owned_inventory" not in (task.get(k) or {})
            for k in ("agent_approval_evidence_json", "channel_config_snapshot_json")
        )
    )
    require(
        op["status"] == "succeeded"
        and op.get("error_message") is None
        and op["production_task_id"] == task["id"]
        and op["job_id"] == task["job_id"]
        and bool(h._SHA.fullmatch(op["content_sha256"]))
    )
    h._id(op["manager_task_id"])
    attempted, completed = (
        h._time(op["request_attempted_at"]),
        h._time(op["completed_at"]),
    )
    require(attempted <= completed <= now - timedelta(days=30))
    receipt = h._exact(
        op["receipt_json"], "video_id url title privacy tags quota_estimate"
    )
    video = op["platform_video_id"]
    require(
        isinstance(video, str)
        and len(video) == 11
        and all(c.isascii() and (c.isalnum() or c in "_-") for c in video)
        and op["privacy"] in {"private", "unlisted"}
        and receipt["privacy"] == op["privacy"]
        and receipt["title"] == op["title"]
        and receipt["video_id"] == video
        and receipt["url"] == f"https://www.youtube.com/watch?v={video}"
    )
    require(
        job["id"] == op["job_id"]
        and job["status"] == "SUCCEEDED"
        and job.get("error_message") is None
        and completed <= h._time(job["completed_at"]) <= now
    )
    uploads = []
    require(1 <= len(facts["nodes"]) <= 128 and len(facts["artifacts"]) <= 128)
    for node in facts["nodes"]:
        require(
            node["status"] == "SUCCEEDED"
            and node["job_id"] == job["id"]
            and node.get("error_message") is None
        )
        if node["node_type"] == "youtube_upload":
            uploads.append(node)
            require(
                node["id"] == op["node_execution_id"]
                and node["input_artifact_ids"] == [op["input_artifact_id"]]
                and completed
                <= h._time(node["completed_at"])
                <= h._time(job["completed_at"])
            )
            h._one(
                [
                    a
                    for a in facts["artifacts"]
                    if a["id"] == node.get("output_artifact_id")
                    and a["node_execution_id"] == node["id"]
                    and a["job_id"] == job["id"]
                    and a.get("media_info", {}).get("youtube") == receipt
                ],
                REASON,
            )
    require(len(uploads) == 1)
    require(
        pub["production_task_id"] == task["id"]
        and pub["account_id"] == task["target_account_id"]
        and pub["platform"] == "youtube"
        and pub["platform_content_id"] == video
        and pub["desired_privacy"] == pub["current_privacy"] == "unlisted"
        and pub.get("public_at") is None
        and pub["publish_status"] in {"uploaded", "scheduled"}
    )
    start, uploaded = h._time(pub["scheduled_publish_at"]), h._time(pub["uploaded_at"])
    require(completed <= uploaded <= start <= now)
    require(
        promotion["production_task_id"] == task["id"]
        and promotion["publication_id"] == pub["id"]
        and promotion["platform_video_id"] == video
        and promotion["status"] == "finalized"
        and promotion.get("error_message") is None
        and promotion["target_privacy"] == promotion["observed_privacy"] == "unlisted"
        and promotion["observed_publish_status"] in {"uploaded", "scheduled"}
        and h._time(promotion["scheduled_at"]) == start
        and promotion["attempt_key"] == "channelops-promotion:" + h._id(promotion["id"])
    )
    require(
        completed
        <= h._time(promotion["request_attempted_at"])
        <= h._time(promotion["confirmed_at"])
        <= h._time(promotion["completed_at"])
        <= now
    )
    require(
        isinstance(promotion["decision_json"], dict)
        and promotion["decision_json"].get("verdict") == "allow"
        and bool(promotion["decision_json"].get("decision_id"))
        and bool(promotion["decision_json"].get("rules_version"))
        and isinstance(promotion["evidence_json"], dict)
        and bool(promotion["evidence_json"])
    )
    manual_id = h._id(promotion["queue_item_id"])
    auto = h._one(facts["queues"], REASON)
    payload = auto["payload_json"]
    due = uploaded + timedelta(hours=1)
    require(
        auto["kind"] == "promote_publication"
        and auto["status"] == "cancelled"
        and auto["last_error"] == "replaced_by_immediate_unlisted_canary_promotion"
        and type(auto["attempt_count"]) is int
        and auto["attempt_count"] == 0
        and auto.get("locked_at") is None
        and auto.get("locked_by") is None
        and auto["channel_profile_id"] == task["channel_profile_id"]
        and auto["id"] != manual_id
        and h._time(auto["run_after"]) == due
        and auto["idempotency_key"]
        == f"promote_publication:{pub['id']}:unlisted:{h._z(due)}"
        and payload.get("publication_id") == pub["id"]
        and payload.get("target_visibility") == "unlisted"
        and payload.get("scheduled_at") == h._z(due)
        and uploaded
        <= h._time(auto["dead_letter_at"])
        <= h._time(promotion["request_attempted_at"])
    )
    publish_parent = h._id(auto["parent_queue_item_id"])
    require(publish_parent not in {manual_id, auto["id"]})
    require(
        not any(
            q["id"] in {manual_id, publish_parent}
            for q in rows["channel_ops_queue_items"]
        )
    )
    require(
        all(
            q["status"] in {"succeeded", "cancelled"}
            for q in rows["channel_ops_queue_items"]
            if q.get("channel_profile_id") == task["channel_profile_id"]
        )
    )
    require(
        all(
            q["id"] == auto["id"]
            for q in rows["channel_ops_queue_items"]
            if q.get("payload_json", {}).get("job_id") == job["id"]
        )
    )
    require(len(facts["metrics"]) == 5 and len(facts["feedback"]) == 6)
    for stage, hours, grace_hours in h._METRIC_STAGES:
        metric = h._one(
            [m for m in facts["metrics"] if m["snapshot_stage"] == stage], REASON
        )
        due, grace = (
            start + timedelta(hours=hours),
            start + timedelta(hours=grace_hours),
        )
        done = h._time(metric["completed_at"])
        require(
            metric["publication_id"] == pub["id"]
            and h._time(metric["effective_start_at"]) == start
            and h._time(metric["due_at"]) == due
            and h._time(metric["grace_until"]) == grace
            and metric["status"] == "succeeded"
            and type(metric["attempt_count"]) is int
            and metric["attempt_count"] == 1
            and metric.get("last_error_code") is None
            and done == h._time(metric["last_attempt_at"])
            and due <= done <= min(now, grace)
        )
        feedback = h._one(
            [f for f in facts["feedback"] if f["snapshot_stage"] == stage], REASON
        )
        require(
            feedback["publication_id"] == pub["id"]
            and due <= h._time(feedback["collected_at"]) <= done
        )
    immediate = h._one(
        [f for f in facts["feedback"] if f["snapshot_stage"] == "immediate"], REASON
    )
    require(
        immediate["publication_id"] == pub["id"]
        and completed <= h._time(immediate["collected_at"]) <= now
    )
    require(not any(j.get("parent_job_id") == job["id"] for j in rows["jobs"]))
    graph = h._terminal_graph(
        rows,
        job_id=job["id"],
        upload_node_id=op["node_execution_id"],
        task_id=task["id"],
        legacy_channel_profile_id=task["channel_profile_id"],
    )
    checks = {
        "worker_task_dispatches": {"resolution_state": {"acknowledged", "cancelled"}},
        "worker_task_delivery_attestations": {"ack_state": {"acknowledged"}},
        "worker_event_emissions": {"emission_state": {"resolved"}},
        "registered_worker_event_receipts": {
            "application_state": {"applied"},
            "ack_state": {"acknowledged"},
            "source_task_ack_state": {"acknowledged"},
        },
        "registered_worker_event_deliveries": {"ack_state": {"acknowledged"}},
    }
    for table, fields in checks.items():
        for row in graph[table]:
            require(all(row.get(key) in allowed for key, allowed in fields.items()))
    for name, value in facts.items():
        if isinstance(value, list):
            value.sort(key=lambda r: r["id"])
    return facts


def approved_certificates(rows, bindings, now):
    certificates = {}
    for inventory in rows["owned_seed_inventories"]:
        if inventory.get("approved_at") is None:
            continue
        section = inventory["manifest_json"].get("legacy_history", {})
        for value in section.get("retained_terminal_uploads", []):
            parse_certificate(value)
            operation_id = value["operation_id"]
            require(
                operation_id not in certificates or certificates[operation_id] == value
            )
            binding = bindings.get(value["legacy_account_id"])
            require(
                binding is not None
                and value["legacy_channel_profile_id"]
                == binding.legacy_channel_profile_id
                and operation_id in binding.qualified_operation_ids
            )
            qualification = binding.qualification
            require(
                h._utc_fact(value["observed_at"])
                == qualification.observed_at
                <= h._time(inventory["approved_at"])
                <= now
                and value["server_subject"] == qualification.server_subject
                and value["approval_reference"] == qualification.approval_reference
            )
            task = h._one(
                [t for t in rows["production_tasks"] if t["id"] == value["task_id"]],
                REASON,
            )
            require(
                task["target_account_id"] == value["legacy_account_id"]
                and task["channel_profile_id"] == value["legacy_channel_profile_id"]
            )
            facts = retained_terminal_facts(
                rows, task, h._utc_fact(value["observed_at"])
            )
            require(
                facts["operations"][0]["id"] == operation_id
                and facts["publications"][0]["id"] == value["publication_id"]
                and h.history_sha256(facts) == value["retained_facts_sha256"]
                and facts == value["retained_facts"]
            )
            certificates[operation_id] = value
    return certificates


def issue_certificate(facts, observed_at, subject, reference):
    task, operation, publication = (
        facts["task"],
        facts["operations"][0],
        facts["publications"][0],
    )
    value = dict(
        version=1,
        classification="retained_terminal_upload_queue_gap",
        operation_id=operation["id"],
        task_id=task["id"],
        publication_id=publication["id"],
        legacy_account_id=task["target_account_id"],
        legacy_channel_profile_id=task["channel_profile_id"],
        observed_at=observed_at.isoformat(),
        server_subject=subject,
        approval_reference=reference,
        retained_facts=facts,
        retained_facts_sha256=h.history_sha256(facts),
    )
    parse_certificate(value)
    return value


def stable_certificate(value):
    return {
        k: v
        for k, v in value.items()
        if k not in {"observed_at", "server_subject", "approval_reference"}
    }
