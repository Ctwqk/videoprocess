from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from worker.youtube_ack_drill_evidence import validate_ack_drill_evidence


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")).hexdigest()


def uid(number):
    return f"00000000-0000-4000-8000-{number:012d}"


def at(second):
    return (datetime(2026, 9, 10, 8, tzinfo=timezone.utc) + timedelta(seconds=second)).isoformat()


def log(second, request, *, client="172.19.0.1:48146", status="200 OK"):
    return json.dumps({"time": at(second), "stream": "stdout", "log": f'INFO:     {client} - "{request} HTTP/1.1" {status}\n'}) + "\n"


def evidence():
    title = "Owned unlisted ACK drill"
    identity = {
        "drill_id": uid(1), "production_task_id": uid(2), "job_id": uid(3),
        "node_execution_id": uid(4), "input_artifact_id": uid(5),
        "content_sha256": "1" * 64, "title_sha256": digest(title),
        "privacy": "unlisted", "worker_id": "youtube_publisher-worker@150:1:fixture",
        "started_at": at(-1), "worker_registration_id": uid(6), "worker_lease_epoch": 1,
        "owned_attestation_sha256": "3" * 64, "manager_origin": "http://manager.invalid:8899",
        "expires_at": at(60), "release_commit": "4" * 40, "channel_id": uid(7),
        "account_id": uid(8), "platform_channel_id": "UC" + "a" * 22,
        "service_name": "vp_worker_youtube_publisher", "redis_stream": "vp:tasks:youtube_publisher",
        "consumer_group": "workers", "message_id": "1789027200000-0",
        "payload_sha256": "5" * 64, "dispatch_key": uid(9), "attestation_id": uid(10),
        "receiver_container_id": "6" * 64, "receiver_image_id": "sha256:" + "7" * 64,
        "audit_window_id": uid(11),
    }
    start = {
        "container_id": identity["receiver_container_id"], "image_id": identity["receiver_image_id"],
        "started_at": at(-3600), "restart_count": 0, "log_device": 8, "log_inode": 900,
        "offset": 1024, "at": at(0), "prefix_sha256": "8" * 64,
        "platform_channel_id": identity["platform_channel_id"], "credential_sha256": "9" * 64,
        "active_upload_tasks": [],
    }
    identity["audit_cursor_sha256"] = digest(start)
    operation_id, manager_id, video_id = uid(12), uid(13), "aBcdE_fG-12"
    receipt = {"video_id": video_id, "url": f"https://www.youtube.com/watch?v={video_id}",
               "title": title, "privacy": "unlisted", "tags": ["owned-drill"], "quota_estimate": 1600}
    names = ["start", "upload_post_attempt", "submitted_committed", "completed_get_1",
             "processed_unlisted_get_1", "fresh_submitted_empty_receipt", "token_consumed",
             "pre_receipt_abort", "fresh_submitted_resume", "completed_get_2",
             "processed_unlisted_get_2", "mark_succeeded_commit"]
    times = [1, 2, 4, 6, 7, 8, 9, 10, 11, 13, 14, 15]
    events = []
    for sequence, (event, second) in enumerate(zip(names, times), start=1):
        row = {**identity, "sequence": sequence, "event": event, "utc": at(second),
               "monotonic": float(100 + second), "operation_id": operation_id,
               "manager_task_id": manager_id if sequence >= 3 else None,
               "video_id": video_id if sequence >= 4 else None}
        if event.startswith("completed_get"):
            row["manager_status"] = "completed"
        if event.startswith("processed_unlisted"):
            row["upload_status"] = "processed"
        if event.startswith("fresh_submitted"):
            row.update(status="submitted", request_attempted=True, receipt_empty=True,
                       platform_video_id=None, completed_at=None)
        if event == "mark_succeeded_commit":
            row["receipt_sha256"] = digest(receipt)
        events.append(row)
    consumed = {**identity, "operation_id": operation_id, "manager_task_id": manager_id,
                "video_id": video_id, "nonce": "a" * 64}
    claim = {key: identity[key] for key in ("job_id", "node_execution_id", "worker_registration_id", "worker_id", "worker_lease_epoch")}
    claim["worker_started_at"] = identity["started_at"]
    operation = {key: identity[key] for key in ("production_task_id", "job_id", "node_execution_id", "input_artifact_id", "content_sha256", "privacy")}
    operation.update(id=operation_id, status="succeeded", manager_task_id=manager_id,
                     platform_video_id=video_id, completed_at=at(14.9), receipt_json=receipt,
                     title=title, request_attempted_at=at(1.5), error_message=None,
                     created_at=at(0.5), updated_at=at(14.9))
    output = {"id": uid(14), "job_id": uid(3), "node_execution_id": uid(4), "created_at": at(16),
              "kind": "FINAL", "filename": "youtube_upload.mp4", "mime_type": "video/mp4",
              "file_size": 1024, "storage_backend": "local", "storage_path": "outputs/youtube_upload.mp4",
              "media_info": {"youtube": copy.deepcopy(receipt)}}
    payload = {"event": "node_completed", "output_artifact_id": output["id"],
               **{key: str(identity[key]) for key in ("job_id", "node_execution_id", "worker_id",
                                                     "started_at", "worker_registration_id", "worker_lease_epoch")},
               "task_stream": identity["redis_stream"], "task_group": identity["consumer_group"],
               "task_message_id": identity["message_id"], "task_payload_sha256": identity["payload_sha256"],
               "task_dispatch_key": identity["dispatch_key"]}
    emission = {**claim, "id": uid(15), "source_task_attestation_id": uid(10), "event_type": "node_completed",
                "emission_state": "resolved", "prepared_at": at(17), "emitted_at": at(17.1),
                "resolved_at": at(18), "payload_json": payload, "payload_sha256": digest(payload),
                "redis_stream": "vp:events", "consumer_group": "orchestrator", "message_id": "1789027217100-0"}
    delivery = {**claim, **{key: identity[key] for key in ("redis_stream", "consumer_group", "message_id", "payload_sha256", "dispatch_key")},
                "id": uid(10), "ack_state": "acknowledged", "acknowledged_at": at(18),
                "ack_event_emission_id": uid(15), "attested_at": at(-0.5)}
    publication = {"id": uid(16), "production_task_id": uid(2), "account_id": uid(8),
                   "platform": "youtube", "platform_content_id": video_id,
                   "desired_privacy": "unlisted", "current_privacy": "unlisted",
                   "publish_status": "uploaded", "public_at": None, "uploaded_at": at(19),
                   "permalink": receipt["url"], "title": title, "description": "", "tags_json": receipt["tags"],
                   "thumbnail_storage_path": None, "scheduled_publish_at": None,
                   "compliance_disposition": "allowed", "quota_units_estimated": 1600,
                   "last_metrics_polled_at": None, "warnings_json": [], "created_at": at(19), "updated_at": at(19)}
    durable = {"observed_at": at(20), "operations": [operation], "outputs": [output],
               "emissions": [emission], "deliveries": [delivery], "publications": [publication]}
    raw = (log(3, "POST /api/upload") + log(5, f"GET /api/status/{manager_id}")
           + log(6.5, f"GET /api/videos/{video_id}/status") + log(12, f"GET /api/status/{manager_id}")
           + log(13.5, f"GET /api/videos/{video_id}/status"))
    receiver = {"schema_version": 1, "window_id": uid(11), "start": start,
                "end": {**start, "at": at(25), "offset": start["offset"] + len(raw.encode("utf-8"))},
                "records_utf8": raw}
    return {"schema_version": 1, "events": events, "consumed": [consumed], "durable": durable}, receiver


def test_complete_independent_window_and_fresh_resume_pass_without_mutation():
    journal, receiver = evidence()
    before = copy.deepcopy((journal, receiver))
    result = validate_ack_drill_evidence(journal, receiver)
    assert result == {"status": "passed", "reasons": [], "counts": {"upload_posts": 1, "completed_gets": 2, "consumed_tokens": 1}}
    assert (journal, receiver) == before


def postflow_evidence():
    journal, receiver = evidence()
    durable = journal["durable"]
    durable["observed_at"] = at(90)
    publication = durable["publications"][0]
    publication.update(publish_status="scheduled", scheduled_publish_at=at(30), updated_at=at(35))
    return journal, receiver


@pytest.mark.parametrize("status", ["uploaded", "scheduled"])
def test_postflow_snapshot_after_core_close_preserves_actual_unlisted_publication(status):
    journal, receiver = postflow_evidence()
    journal["durable"]["publications"][0]["publish_status"] = status
    before = copy.deepcopy((journal, receiver))
    result = validate_ack_drill_evidence(journal, receiver)
    assert result == {"status": "passed", "reasons": [], "counts": {
        "upload_posts": 1, "completed_gets": 2, "consumed_tokens": 1,
    }}
    assert (journal, receiver) == before


@pytest.mark.parametrize("uploaded,observed", [(30, 30), (30, 90), (19, 25)])
def test_publication_and_snapshot_may_follow_core_ack_window(uploaded, observed):
    journal, receiver = postflow_evidence()
    journal["durable"]["publications"][0].update(
        uploaded_at=at(uploaded), scheduled_publish_at=at(observed), updated_at=at(observed),
    )
    journal["durable"]["observed_at"] = at(observed)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "passed"


@pytest.mark.parametrize("boundary", [25, 26], ids=["at-core-close", "after-core-close"])
@pytest.mark.parametrize("late", ["output", "prepared", "emitted", "resolved", "ack"])
def test_postflow_observation_cannot_hide_worker_completion_outside_core_window(late, boundary):
    journal, receiver = postflow_evidence()
    durable = journal["durable"]
    times = {"output": 16, "prepared": 17, "emitted": 17.1, "resolved": 18, "ack": 18}
    worker_order = ["output", "prepared", "emitted", "resolved"]
    if late == "ack":
        times["ack"] = boundary
    else:
        for index, field in enumerate(worker_order[worker_order.index(late):]):
            times[field] = boundary + index
        times["ack"] = max(times["ack"], times["emitted"])
    durable["outputs"][0]["created_at"] = at(times["output"])
    for field in ("prepared", "emitted", "resolved"):
        durable["emissions"][0][field + "_at"] = at(times[field])
    durable["deliveries"][0]["acknowledged_at"] = at(times["ack"])
    durable["publications"][0]["uploaded_at"] = at(40)
    result = validate_ack_drill_evidence(journal, receiver)
    assert result["status"] == "failed"
    assert result["reasons"] == ["output event ACK or publication ordering is invalid"]


@pytest.mark.parametrize("observed", [15.9, 16.9, 17.05, 17.9, 18.5, 29.9])
def test_postflow_observation_must_cover_all_worker_and_upload_timestamps(observed):
    journal, receiver = postflow_evidence()
    journal["durable"]["publications"][0]["uploaded_at"] = at(30)
    journal["durable"]["observed_at"] = at(observed)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


def test_postflow_observation_must_cover_ack_later_than_resolved_event():
    journal, receiver = postflow_evidence()
    journal["durable"]["deliveries"][0]["acknowledged_at"] = at(24)
    journal["durable"]["observed_at"] = at(23)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("table,field,value", [
    ("outputs", "created_at", at(14)), ("emissions", "prepared_at", at(15.9)),
    ("emissions", "emitted_at", at(16.9)), ("emissions", "resolved_at", at(17.05)),
    ("deliveries", "acknowledged_at", at(17.05)), ("publications", "uploaded_at", at(14.8)),
])
def test_postflow_snapshot_keeps_existing_worker_and_receipt_order(table, field, value):
    journal, receiver = postflow_evidence()
    journal["durable"][table][0][field] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("field,value", [
    ("platform_content_id", "different12"), ("platform", "other"),
    ("current_privacy", "public"), ("current_privacy", "private"), ("current_privacy", None),
    ("desired_privacy", "public"), ("desired_privacy", "private"),
    ("public_at", at(30)), ("publish_status", "unknown"), ("publish_status", "failed"),
    ("publish_status", "pending"), ("publish_status", "published"),
    ("production_task_id", uid(99)), ("account_id", uid(99)),
])
def test_postflow_snapshot_does_not_relax_publication_identity_or_unlisted_scope(field, value):
    journal, receiver = postflow_evidence()
    journal["durable"]["publications"][0][field] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("missing", ["receiver", "durable", "runtime_identity", "token"])
def test_missing_independent_or_durable_evidence_never_passes(missing):
    journal, receiver = evidence()
    if missing == "receiver":
        receiver = None
    elif missing == "durable":
        journal.pop("durable")
    elif missing == "runtime_identity":
        journal["events"][0].pop("release_commit")
    else:
        journal.pop("consumed")
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


@pytest.mark.parametrize("path,status", [("/api/upload", "500 Internal Server Error"), ("/api/upload/local", "200 OK"), ("/api/%75pload/", "422 Unprocessable Entity")])
def test_duplicate_post_including_failed_or_alternate_route_fails(path, status):
    journal, receiver = evidence()
    extra = log(22, f"POST {path}").replace("200 OK", status)
    receiver["records_utf8"] += extra
    receiver["end"]["offset"] += len(extra.encode())
    result = validate_ack_drill_evidence(journal, receiver)
    assert result["status"] == "failed"
    assert result["counts"]["upload_posts"] == 2


@pytest.mark.parametrize("field,value", [("container_id", "b" * 64), ("restart_count", 1), ("log_inode", 901), ("log_device", 9), ("prefix_sha256", "c" * 64), ("offset", 99999), ("credential_sha256", "d" * 64), ("platform_channel_id", "UC" + "z" * 22)])
def test_rotated_gapped_restarted_or_account_changed_capture_is_inconclusive(field, value):
    journal, receiver = evidence()
    receiver["end"][field] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


@pytest.mark.parametrize("event_index,key,value", [(3, "video_id", "different12"), (8, "operation_id", uid(99)), (8, "manager_task_id", uid(99)), (8, "dispatch_key", uid(99)), (8, "message_id", "123-4"), (5, "receipt_empty", False), (5, "completed_at", at(4)), (8, "status", "succeeded"), (9, "manager_status", "running"), (10, "privacy", "public"), (10, "upload_status", "uploaded")])
def test_conflicting_boundary_resume_or_platform_evidence_fails(event_index, key, value):
    journal, receiver = evidence()
    journal["events"][event_index][key] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


def test_replay_substitution_missing_new_get_is_not_a_pass():
    journal, receiver = evidence()
    journal["events"][9]["event"] = "durable_receipt_replay"
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


def test_repeated_token_is_failure_even_with_one_post():
    journal, receiver = evidence()
    journal["consumed"].append(copy.deepcopy(journal["consumed"][0]))
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("request_line", [f"GET /api/status/{uid(99)}", "POST /api/upload/resumable", "POST /api/upload?privacy=public"])
def test_unknown_concurrent_or_extra_upload_invalidates_exclusive_window(request_line):
    journal, receiver = evidence()
    extra = log(22, request_line)
    receiver["records_utf8"] += extra
    receiver["end"]["offset"] += len(extra.encode())
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


def test_receiver_gets_must_bracket_both_actual_completion_attempts():
    journal, receiver = evidence()
    raw = log(3, "POST /api/upload") + log(5, f"GET /api/status/{uid(13)}") + log(5.5, f"GET /api/status/{uid(13)}")
    receiver["records_utf8"] = raw
    receiver["end"]["offset"] = receiver["start"]["offset"] + len(raw.encode())
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("table,key,value", [("operations", "platform_video_id", "different12"), ("operations", "completed_at", at(5)), ("outputs", "created_at", at(5)), ("emissions", "event_type", "node_failed"), ("emissions", "prepared_at", at(5)), ("deliveries", "acknowledged_at", at(5)), ("deliveries", "ack_state", "pending"), ("publications", "current_privacy", "public"), ("publications", "platform_content_id", "different12")])
def test_early_output_ack_or_conflicting_durable_linkage_fails(table, key, value):
    journal, receiver = evidence()
    journal["durable"][table][0][key] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("table", ["operations", "outputs", "emissions", "deliveries", "publications"])
def test_duplicate_durable_linkage_fails(table):
    journal, receiver = evidence()
    journal["durable"][table].append(copy.deepcopy(journal["durable"][table][0]))
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("raw", ["not json\n", '{"log":"POST /api/upload","time":"bad"}\n', '{"log":"a","log":"b"}\n'])
def test_malformed_receiver_lines_are_not_silently_discarded(raw):
    journal, receiver = evidence()
    receiver["records_utf8"] += raw
    receiver["end"]["offset"] += len(raw.encode())
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


def test_result_never_echoes_raw_log_or_secretful_input():
    journal, receiver = evidence()
    receiver["records_utf8"] = "DO_NOT_ECHO_SECRET"
    result = validate_ack_drill_evidence(journal, receiver)
    assert result["status"] == "inconclusive"
    assert "DO_NOT_ECHO_SECRET" not in json.dumps(result)


def receiver_records(receiver):
    return [json.loads(line) for line in receiver["records_utf8"].splitlines()]


def replace_records(receiver, records):
    raw = "".join(json.dumps(row) + "\n" for row in sorted(records, key=lambda row: row["time"]))
    receiver["records_utf8"] = raw
    receiver["end"]["offset"] = receiver["start"]["offset"] + len(raw.encode("utf-8"))


@pytest.mark.parametrize("phase", [1, 2, "both"])
@pytest.mark.parametrize("defect", ["missing", "wrong_video", "failed", "stale", "late", "not_get"])
def test_exact_successful_platform_get_is_required_in_each_phase(phase, defect):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    selected = {1: [2], 2: [4], "both": [2, 4]}[phase]
    for index in selected:
        record = records[index]
        if defect == "wrong_video":
            record["log"] = record["log"].replace("aBcdE_fG-12", "different12")
        elif defect == "failed":
            record["log"] = record["log"].replace("200 OK", "500 Internal Server Error")
        elif defect == "stale":
            record["time"] = at(5.9 if index == 2 else 12.9)
        elif defect == "late":
            record["time"] = at(7.1 if index == 2 else 14.1)
        elif defect == "not_get":
            record["log"] = record["log"].replace("GET ", "HEAD ")
    if defect == "missing":
        records = [row for index, row in enumerate(records) if index not in selected]
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("index", [1, 2, 3, 4], ids=["manager1", "platform1", "manager2", "platform2"])
@pytest.mark.parametrize("client", ["192.0.2.99:9999", "172.19.0.1:48147"])
def test_each_phase_request_needs_original_client_correlation(index, client):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records[index]["log"] = records[index]["log"].replace("172.19.0.1:48146", client)
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


def test_stale_original_client_get_cannot_link_other_client_get1():
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records[1]["log"] = records[1]["log"].replace("172.19.0.1:48146", "192.0.2.99:9999")
    records.append(json.loads(log(3.5, f"GET /api/status/{uid(13)}")))
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


@pytest.mark.parametrize("value", [None, {}, {"youtube": None}, {"youtube": {}}])
def test_output_requires_normal_youtube_receipt(value):
    journal, receiver = evidence()
    journal["durable"]["outputs"][0]["media_info"] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


@pytest.mark.parametrize("key,value", [("video_id", "different12"), ("privacy", "public"),
                                       ("url", "https://example.invalid/other"), ("title", "other title"),
                                       ("tags", []), ("quota_estimate", 0)])
def test_every_output_receipt_field_must_match_durable_receipt(key, value):
    journal, receiver = evidence()
    journal["durable"]["outputs"][0]["media_info"]["youtube"][key] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("privacy", ["public", "private", None])
def test_matching_rehashed_receipts_cannot_override_intended_unlisted_privacy(privacy):
    journal, receiver = evidence()
    receipt = journal["durable"]["operations"][0]["receipt_json"]
    receipt["privacy"] = privacy
    journal["durable"]["outputs"][0]["media_info"]["youtube"] = copy.deepcopy(receipt)
    journal["events"][-1]["receipt_sha256"] = digest(receipt)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("key", ["video_id", "url", "title", "privacy", "tags", "quota_estimate"])
def test_rehashed_incomplete_normal_receipt_cannot_pass(key):
    journal, receiver = evidence()
    receipt = journal["durable"]["operations"][0]["receipt_json"]
    receipt.pop(key)
    journal["durable"]["outputs"][0]["media_info"]["youtube"] = copy.deepcopy(receipt)
    journal["events"][-1]["receipt_sha256"] = digest(receipt)
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


@pytest.mark.parametrize("key,value", [
    ("event", "node_failed"), ("job_id", uid(99)), ("node_execution_id", uid(99)),
    ("output_artifact_id", uid(99)), ("worker_id", "other-worker"), ("started_at", at(-2)),
    ("worker_registration_id", uid(99)), ("worker_lease_epoch", "2"), ("worker_lease_epoch", 1),
    ("task_stream", "vp:tasks:other"), ("task_group", "other-group"), ("task_message_id", "123-4"),
    ("task_payload_sha256", "f" * 64), ("task_dispatch_key", uid(99)), ("error", "failed upload"),
])
def test_rehashed_event_payload_must_match_normal_success_claim_and_delivery(key, value):
    journal, receiver = evidence()
    emission = journal["durable"]["emissions"][0]
    emission["payload_json"][key] = value
    emission["payload_sha256"] = digest(emission["payload_json"])
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "failed"


@pytest.mark.parametrize("key", ["event", "job_id", "node_execution_id", "output_artifact_id", "worker_id",
                                 "started_at", "worker_registration_id", "worker_lease_epoch", "task_stream",
                                 "task_group", "task_message_id", "task_payload_sha256", "task_dispatch_key"])
def test_normal_registered_success_payload_fields_are_required(key):
    journal, receiver = evidence()
    emission = journal["durable"]["emissions"][0]
    emission["payload_json"].pop(key)
    emission["payload_sha256"] = digest(emission["payload_json"])
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


@pytest.mark.parametrize("value", [None, "f" * 64, "5" * 64])
def test_emission_requires_its_event_digest_not_the_task_digest(value):
    journal, receiver = evidence()
    journal["durable"]["emissions"][0]["payload_sha256"] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


@pytest.mark.parametrize("key,value", [("redis_stream", None), ("redis_stream", "vp:tasks:youtube_publisher"),
                                       ("consumer_group", None), ("consumer_group", "workers"),
                                       ("message_id", None), ("message_id", ""), ("message_id", "not-a-redis-id"),
                                       ("message_id", "0-0")])
def test_emitted_event_requires_normal_redis_identity(key, value):
    journal, receiver = evidence()
    journal["durable"]["emissions"][0][key] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


@pytest.mark.parametrize("message", [
    "Shutting down", "Waiting for application shutdown.", "Application shutdown complete.",
    "Finished server process [100]", "Started server process [200]", "Waiting for application startup.",
    "Application startup complete.", "Started reloader process [200] using WatchFiles",
    "WatchFiles detected changes in 'main.py'. Reloading...", "StatReload detected changes. Reloading...",
    "Stopping reloader process [200]", "Booting worker with pid: 200", "Worker exiting (pid: 100)",
])
def test_receiver_lifecycle_or_reload_breaks_continuity_despite_unchanged_container(message):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records.append({"time": at(10.5), "stream": "stderr", "log": f"INFO:     {message}\n"})
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


@pytest.mark.parametrize("value", [None, "wrong-title", "", {"title": "Owned unlisted ACK drill"}])
def test_final_operation_title_must_match_armed_canonical_hash(value):
    journal, receiver = evidence()
    journal["durable"]["operations"][0]["title"] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


@pytest.mark.parametrize("value", [None, "invalid", "2026-09-10T08:00:01.5", at(0.9), at(2.1), at(4.1), at(16)])
def test_final_operation_attempt_marker_must_precede_post_and_follow_fresh_start(value):
    journal, receiver = evidence()
    journal["durable"]["operations"][0]["request_attempted_at"] = value
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


def test_normal_optional_receipt_values_and_equivalent_claim_time_pass():
    journal, receiver = evidence()
    receipt = journal["durable"]["operations"][0]["receipt_json"]
    receipt.update(url="", tags=[], quota_estimate=None)
    journal["durable"]["outputs"][0]["media_info"]["youtube"] = copy.deepcopy(receipt)
    journal["events"][-1]["receipt_sha256"] = digest(receipt)
    emission = journal["durable"]["emissions"][0]
    emission["payload_json"]["started_at"] = "2026-09-10T00:59:59-07:00"
    emission["payload_sha256"] = digest(emission["payload_json"])
    emission["message_id"] = journal["durable"]["deliveries"][0]["message_id"]
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "passed"


@pytest.mark.parametrize("second,path", [
    (0.5, f"/api/status/{uid(13)}"),
    (5.5, f"/api/status/{uid(13)}"), (6.7, "/api/videos/aBcdE_fG-12/status"),
    (12.5, f"/api/status/{uid(13)}"), (13.7, "/api/videos/aBcdE_fG-12/status"),
    (22, f"/api/status/{uid(13)}"), (22, "/api/videos/aBcdE_fG-12/status"),
])
@pytest.mark.parametrize("client", ["192.0.2.99:9999", "172.19.0.1:48147"])
def test_valid_phase_gets_cannot_mask_extra_uncorrelated_reads_anywhere_in_window(second, path, client):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records.append(json.loads(log(second, f"GET {path}", client=client)))
    replace_records(receiver, records)
    before = copy.deepcopy((journal, receiver))
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"
    assert (journal, receiver) == before


@pytest.mark.parametrize("path", [f"/api/status/{uid(13)}", "/api/videos/aBcdE_fG-12/status"])
@pytest.mark.parametrize("method,status", [("GET", "500 Internal Server Error"), ("HEAD", "200 OK")])
def test_exclusive_window_checks_all_relevant_reads_not_only_successful_gets(path, method, status):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records.append(json.loads(log(22, f"{method} {path}", client="192.0.2.99:9999", status=status)))
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "inconclusive"


@pytest.mark.parametrize("second", [0.5, 6.7, 13.7, 22])
@pytest.mark.parametrize("client", ["172.19.0.1:48146", "192.0.2.99:9999"])
def test_valid_platform_gets_cannot_mask_extra_wrong_video_reads(second, client):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records.append(json.loads(log(second, "GET /api/videos/different12/status", client=client)))
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] != "passed"


def test_correlated_repeated_polling_preserves_positive_fresh_get_evidence():
    journal, receiver = evidence()
    records = receiver_records(receiver)
    for second, path in [
        (4.5, f"/api/status/{uid(13)}"), (5.5, f"/api/status/{uid(13)}"),
        (6.2, "/api/videos/aBcdE_fG-12/status"), (6.7, "/api/videos/aBcdE_fG-12/status"),
        (11.5, f"/api/status/{uid(13)}"), (12.5, f"/api/status/{uid(13)}"),
        (13.2, "/api/videos/aBcdE_fG-12/status"), (13.7, "/api/videos/aBcdE_fG-12/status"),
        (22, f"/api/status/{uid(13)}"), (23, "/api/videos/aBcdE_fG-12/status"),
    ]:
        records.append(json.loads(log(second, f"GET {path}")))
    replace_records(receiver, records)
    before = copy.deepcopy((journal, receiver))
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "passed"
    assert (journal, receiver) == before


@pytest.mark.parametrize("path", ["/api/tasks", "/api/account", "/health"])
def test_unrelated_read_only_routes_do_not_need_upload_client_correlation(path):
    journal, receiver = evidence()
    records = receiver_records(receiver)
    records.append(json.loads(log(22, f"GET {path}", client="192.0.2.99:9999")))
    replace_records(receiver, records)
    assert validate_ack_drill_evidence(journal, receiver)["status"] == "passed"
