"""Check operator-collected evidence, without network, mutations or replay.

The caller supplies the complete journal/token and fresh durable row snapshots.
Receiver evidence is the unfiltered Docker json-file byte range between two
operator snapshots of one unchanged log file, container and credential identity.
Rotation, an incomplete byte range or an unrecognized access format is not proof.
This checks consistency and coverage, not authenticity against a forged capture.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlsplit


_EVENTS = (
    "start", "upload_post_attempt", "submitted_committed", "completed_get_1",
    "processed_unlisted_get_1", "fresh_submitted_empty_receipt", "token_consumed",
    "pre_receipt_abort", "fresh_submitted_resume", "completed_get_2",
    "processed_unlisted_get_2", "mark_succeeded_commit",
)
_UUID_FIELDS = (
    "drill_id", "production_task_id", "job_id", "node_execution_id", "input_artifact_id",
    "worker_registration_id", "channel_id", "account_id", "dispatch_key", "attestation_id",
    "audit_window_id",
)
_HASH_FIELDS = (
    "content_sha256", "title_sha256", "owned_attestation_sha256", "payload_sha256",
    "receiver_container_id", "audit_cursor_sha256",
)
_IDENTITY = _UUID_FIELDS + _HASH_FIELDS + (
    "privacy", "worker_id", "started_at", "worker_lease_epoch", "manager_origin", "expires_at",
    "release_commit", "platform_channel_id", "service_name", "redis_stream", "consumer_group",
    "message_id", "receiver_image_id",
)
_ACCESS = re.compile(r'^INFO:\s+(\S+) - "([A-Z]+) (\S+) HTTP/\d(?:\.\d)?" (\d{3})(?: .*)?$')
_LIFECYCLE = re.compile(
    r"\b(?:shutting down|application (?:startup|shutdown)|(?:started|finished) server process"
    r"|reload(?:ing|er)?|booting worker|worker exiting)\b", re.IGNORECASE,
)


class _EvidenceError(ValueError):
    def __init__(self, reason: str, *, status: str = "failed") -> None:
        self.reason, self.status = reason, status


def _require(condition: bool, reason: str, *, missing: bool = False) -> None:
    if not condition:
        raise _EvidenceError(reason, status="inconclusive" if missing else "failed")


def _mapping(value: Any, reason: str) -> dict:
    _require(type(value) is dict, reason, missing=True)
    return value


def _time(value: Any) -> float:
    _require(isinstance(value, str), "missing timestamp", missing=True)
    try:
        parsed = datetime.fromisoformat(value)
        _require(parsed.utcoffset() is not None, "timestamp lacks time zone", missing=True)
        return parsed.timestamp()
    except (ValueError, OverflowError):
        raise _EvidenceError("invalid timestamp", status="inconclusive") from None


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")).hexdigest()


def _matches(row: dict, expected: dict, keys: tuple[str, ...]) -> None:
    _require(all(key in row for key in keys), "missing identity evidence", missing=True)
    _require(all(row[key] == expected[key] for key in keys), "identity or linkage changed")


def _canonical_uuid(value: Any) -> bool:
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0
    except ValueError:
        return False


def _journal(journal: Any, counts: dict) -> tuple[dict, list[dict]]:
    journal = _mapping(journal, "missing journal")
    _require(type(journal.get("schema_version")) is int and journal["schema_version"] == 1,
             "unsupported journal version", missing=True)
    events = journal.get("events")
    _require(type(events) is list and bool(events), "missing journal events", missing=True)
    _require(len(events) == len(_EVENTS), "incomplete or repeated journal sequence")
    first = _mapping(events[0], "invalid journal event")
    _require(all(key in first for key in _IDENTITY), "missing runtime identity", missing=True)
    _require(all(_canonical_uuid(first[key]) for key in _UUID_FIELDS), "invalid exact identity")
    _require(all(isinstance(first[key], str) and re.fullmatch(r"[0-9a-f]{64}", first[key]) for key in _HASH_FIELDS),
             "invalid evidence digest")
    _require(first["privacy"] == "unlisted" and type(first["worker_lease_epoch"]) is int
             and first["worker_lease_epoch"] > 0, "invalid privacy or registered claim")
    _require(isinstance(first["release_commit"], str) and re.fullmatch(r"[0-9a-f]{40}", first["release_commit"])
             and isinstance(first["receiver_image_id"], str) and re.fullmatch(r"sha256:[0-9a-f]{64}", first["receiver_image_id"]),
             "invalid release identity")
    _require(all(isinstance(first[key], str) and bool(first[key].strip()) for key in
                 ("worker_id", "platform_channel_id", "service_name", "redis_stream", "consumer_group", "message_id")),
             "invalid worker or account identity")
    expiry, started = _time(first["expires_at"]), _time(first["started_at"])
    operation = first.get("operation_id")
    _require(_canonical_uuid(operation), "invalid operation identity")
    manager = _mapping(events[2], "invalid submitted event").get("manager_task_id")
    video = _mapping(events[3], "invalid completion event").get("video_id")
    _require(_canonical_uuid(manager) and isinstance(video, str) and re.fullmatch(r"[A-Za-z0-9_-]{11}", video),
             "invalid Manager or platform identity")
    previous_mono, previous_utc = -math.inf, started
    for index, (row, name) in enumerate(zip(events, _EVENTS), start=1):
        row = _mapping(row, "invalid journal event")
        _matches(row, first, _IDENTITY)
        _require(type(row.get("sequence")) is int and row["sequence"] == index and row.get("event") == name,
                 "journal sequence does not prove fresh recovery")
        stamp, mono = _time(row.get("utc")), row.get("monotonic")
        _require(type(mono) in {int, float} and math.isfinite(mono) and mono > previous_mono
                 and previous_utc <= stamp < expiry, "journal time order or expiry violated")
        previous_mono, previous_utc = mono, stamp
        _require(row.get("operation_id") == operation and row.get("manager_task_id") == (manager if index >= 3 else None)
                 and row.get("video_id") == (video if index >= 4 else None), "operation Manager or video identity changed")
        if name.startswith("completed_get"):
            _require(row.get("manager_status") == "completed", "completion GET is not completed")
        if name.startswith("processed_unlisted"):
            _require(row.get("upload_status") == "processed", "platform processing not verified")
        if name.startswith("fresh_submitted"):
            _require(row.get("status") == "submitted" and row.get("request_attempted") is True
                     and row.get("receipt_empty") is True and "platform_video_id" in row
                     and row["platform_video_id"] is None and "completed_at" in row and row["completed_at"] is None,
                     "fresh submitted empty receipt boundary not established")
    tokens = journal.get("consumed")
    _require(type(tokens) is list and bool(tokens), "missing consumed token", missing=True)
    counts["consumed_tokens"] = len(tokens)
    _require(len(tokens) == 1, "multiple consumed tokens")
    token = _mapping(tokens[0], "invalid consumed token")
    _matches(token, first, _IDENTITY)
    _require(token.get("operation_id") == operation and token.get("manager_task_id") == manager
             and token.get("video_id") == video and isinstance(token.get("nonce"), str)
             and re.fullmatch(r"[0-9a-f]{64}", token["nonce"]), "consumed token does not bind the interrupted operation")
    counts["completed_gets"] = 2
    return first, events


def _unique_pairs(pairs: list) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _receiver(receiver: Any, first: dict, events: list[dict], counts: dict) -> None:
    receiver = _mapping(receiver, "missing independent receiver audit")
    _require(type(receiver.get("schema_version")) is int and receiver["schema_version"] == 1,
             "unsupported receiver audit version", missing=True)
    start, end = (_mapping(receiver.get(key), "missing receiver cursor") for key in ("start", "end"))
    required = ("container_id", "image_id", "started_at", "restart_count", "log_device", "log_inode",
                "offset", "at", "prefix_sha256", "platform_channel_id", "credential_sha256", "active_upload_tasks")
    _require(all(key in row for row in (start, end) for key in required), "incomplete receiver cursor", missing=True)
    _require(receiver.get("window_id") == first["audit_window_id"] and _hash(start) == first["audit_cursor_sha256"],
             "receiver window is not the armed window")
    _require(start["container_id"] == first["receiver_container_id"] and start["image_id"] == first["receiver_image_id"]
             and start["platform_channel_id"] == first["platform_channel_id"], "receiver account or container mismatch")
    unchanged = tuple(key for key in required if key not in {"offset", "at", "active_upload_tasks"})
    _require(all(start[key] == end[key] for key in unchanged), "receiver restarted rotated or changed identity", missing=True)
    _require(all(type(row[key]) is int and row[key] >= 0 for row in (start, end)
                 for key in ("restart_count", "log_device", "log_inode", "offset")), "invalid receiver file cursor", missing=True)
    _require(all(isinstance(start[key], str) and re.fullmatch(r"[0-9a-f]{64}", start[key])
                 for key in ("prefix_sha256", "credential_sha256")), "invalid receiver cursor digest", missing=True)
    _require(start["active_upload_tasks"] == [] and end["active_upload_tasks"] == [],
             "receiver upload window is not quiet", missing=True)
    opened, closed = _time(start["at"]), _time(end["at"])
    _require(opened < _time(events[0]["utc"]) and _time(events[-1]["utc"]) < closed
             and closed - opened <= 3600, "receiver window does not cover recovery", missing=True)
    raw = receiver.get("records_utf8")
    _require(isinstance(raw, str) and 0 < len(raw) <= 16 * 1024 * 1024,
             "missing or oversized receiver byte range", missing=True)
    _require(raw.endswith("\n") and len(raw.encode("utf-8")) == end["offset"] - start["offset"],
             "receiver byte range has a gap or partial record", missing=True)
    accesses: list[tuple[float, str, str, str, int]] = []
    for line in raw.splitlines():
        try:
            record = json.loads(line, object_pairs_hook=_unique_pairs)
        except (ValueError, RecursionError):
            raise _EvidenceError("invalid receiver JSON record", status="inconclusive") from None
        record = _mapping(record, "invalid receiver JSON record")
        stamp = _time(record.get("time"))
        text = record.get("log")
        _require(opened <= stamp <= closed and record.get("stream") in {"stdout", "stderr"}
                 and isinstance(text, str) and text.endswith("\n"), "invalid receiver record coverage", missing=True)
        match = _ACCESS.fullmatch(text.strip())
        if match is None:
            _require(_LIFECYCLE.search(text) is None, "receiver process lifecycle breaks continuity", missing=True)
            _require(not any(marker in text for marker in ("HTTP/", "/api/upload", "ERROR:", "Traceback")),
                     "unrecognized or incomplete receiver access record", missing=True)
            continue
        client, method, target, status = match.groups()
        path = unquote(urlsplit(target).path).rstrip("/")
        accesses.append((stamp, client, method, path, int(status)))
    uploads = [row for row in accesses if row[2] == "POST" and row[3].startswith("/api/upload")]
    counts["upload_posts"] = len(uploads)
    _require(len(uploads) == 1, "receiver observed other than one upload POST")
    post = uploads[0]
    _require(post[3:] == ("/api/upload", 200) and _time(events[1]["utc"]) <= post[0] <= _time(events[2]["utc"]),
             "upload response is not the original successful submission")
    manager_path = f'/api/status/{events[2]["manager_task_id"]}'
    _require(not any(row[3].startswith("/api/status/") and row[3] != manager_path for row in accesses),
             "uncorrelated Manager activity in exclusive window", missing=True)
    _require(not any(row[2] not in {"GET", "HEAD", "OPTIONS"} and row != post for row in accesses),
             "unexpected receiver mutation in exclusive window", missing=True)
    video_path = f'/api/videos/{events[3]["video_id"]}/status'
    _require(not any(row[3].startswith("/api/videos/") and row[3].endswith("/status")
                     and row[3] != video_path for row in accesses),
             "unexpected platform video in exclusive window")
    # No supplied evidence links other clients, even alongside valid phase requests.
    _require(all(row[1] == post[1] for row in accesses if row[3] in {manager_path, video_path}),
             "status activity has no original upload client correlation", missing=True)
    for path, lower, upper in ((manager_path, 2, 3), (manager_path, 8, 9),
                               (video_path, 3, 4), (video_path, 9, 10)):
        gets = [row for row in accesses if row[2:] == ("GET", path, 200)
                and _time(events[lower]["utc"]) <= row[0] <= _time(events[upper]["utc"])]
        _require(bool(gets), "receiver does not prove both fresh Manager and platform GETs")


def _durable(journal: dict, first: dict, events: list[dict], receiver: dict) -> None:
    durable = _mapping(journal.get("durable"), "missing independent durable completion")
    rows: dict = {}
    for table in ("operations", "outputs", "emissions", "deliveries", "publications"):
        collection = durable.get(table)
        _require(type(collection) is list and bool(collection), "missing durable linkage", missing=True)
        _require(len(collection) == 1, "duplicate durable linkage")
        rows[table] = _mapping(collection[0], "invalid durable row")
        _require(_canonical_uuid(rows[table].get("id")), "invalid durable row identity")
    operation, output, emission, delivery, publication = (rows[key] for key in
                                                        ("operations", "outputs", "emissions", "deliveries", "publications"))
    final = events[-1]
    _matches(operation, first, ("production_task_id", "job_id", "node_execution_id", "input_artifact_id", "content_sha256", "privacy"))
    _require(operation.get("id") == final["operation_id"] and operation.get("manager_task_id") == final["manager_task_id"]
             and operation.get("platform_video_id") == final["video_id"] and operation.get("status") == "succeeded",
             "durable operation does not match recovered video")
    _require(isinstance(operation.get("title"), str), "missing durable operation title", missing=True)
    _require(_hash(operation["title"]) == first["title_sha256"], "durable operation title changed")
    attempted_at = _time(operation.get("request_attempted_at"))
    _require(_time(events[0]["utc"]) <= attempted_at <= _time(events[1]["utc"]),
             "durable request attempt does not match original submission")
    receipt = _mapping(operation.get("receipt_json"), "missing durable receipt")
    _require(all(key in receipt for key in ("video_id", "url", "title", "privacy", "tags", "quota_estimate")),
             "incomplete normal durable receipt", missing=True)
    _require(receipt["privacy"] == "unlisted", "durable receipt privacy is not unlisted")
    _require(receipt["video_id"] == final["video_id"] and _hash(receipt) == final.get("receipt_sha256"),
             "durable receipt digest mismatch")
    receipt_at = _time(operation.get("completed_at"))
    _require(_time(events[10]["utc"]) <= receipt_at <= _time(final["utc"]), "receipt committed before fresh recovery")
    _matches(output, first, ("job_id", "node_execution_id"))
    media_info = _mapping(output.get("media_info"), "missing output media info")
    output_receipt = _mapping(media_info.get("youtube"), "missing output YouTube receipt")
    _require(_hash(output_receipt) == _hash(receipt), "output receipt does not match durable receipt")
    output_at = _time(output.get("created_at"))
    claim_keys = ("job_id", "node_execution_id", "worker_registration_id", "worker_id", "worker_lease_epoch")
    for row in (emission, delivery):
        _matches(row, first, claim_keys)
        _require(_time(row.get("worker_started_at")) == _time(first["started_at"]), "worker claim started_at changed")
    _require(emission.get("source_task_attestation_id") == first["attestation_id"] and emission.get("event_type") == "node_completed"
             and emission.get("emission_state") == "resolved" and _mapping(emission.get("payload_json"), "missing success payload").get("output_artifact_id") == output["id"],
             "durable success event does not match output and delivery")
    _matches(delivery, first, ("redis_stream", "consumer_group", "message_id", "payload_sha256", "dispatch_key"))
    _require(delivery["id"] == first["attestation_id"] and delivery.get("ack_state") == "acknowledged"
             and delivery.get("ack_event_emission_id") == emission["id"], "task ACK is not linked to durable success")
    payload = _mapping(emission.get("payload_json"), "missing success payload")
    expected_payload = {
        "event": "node_completed", "output_artifact_id": output["id"],
        **{key: str(first[key]) for key in claim_keys},
        "task_stream": delivery["redis_stream"], "task_group": delivery["consumer_group"],
        "task_message_id": delivery["message_id"], "task_payload_sha256": delivery["payload_sha256"],
        "task_dispatch_key": delivery["dispatch_key"],
    }
    _matches(payload, expected_payload, tuple(expected_payload))
    _require(set(payload) == set(expected_payload) | {"started_at"}
             and all(isinstance(value, str) for value in payload.values()), "invalid normal success payload")
    _require(_time(payload.get("started_at")) == _time(first["started_at"]), "success payload claim started_at changed")
    _require(_hash(payload) == emission.get("payload_sha256"), "emission event payload digest mismatch")
    _require(all(isinstance(emission.get(key), str) and bool(emission[key].strip())
                 for key in ("redis_stream", "consumer_group", "message_id")),
             "missing emitted Redis identity", missing=True)
    _require(emission["redis_stream"] == "vp:events" and emission["consumer_group"] == "orchestrator"
             and re.fullmatch(r"[0-9]{1,20}-[0-9]{1,20}", emission["message_id"])
             and emission["message_id"] != "0-0", "invalid emitted Redis identity")
    _matches(publication, first, ("production_task_id", "account_id"))
    _require(publication.get("platform") == "youtube" and publication.get("platform_content_id") == final["video_id"]
             and publication.get("desired_privacy") == "unlisted" and publication.get("current_privacy") == "unlisted"
             and publication.get("publish_status") in {"uploaded", "scheduled"}
             and "public_at" in publication and publication["public_at"] is None,
             "publication is not the one unlisted recovered video")
    prepared, emitted, resolved, ack = (_time(row.get(key)) for row, key in
                                      ((emission, "prepared_at"), (emission, "emitted_at"), (emission, "resolved_at"), (delivery, "acknowledged_at")))
    observed, uploaded, closed = _time(durable.get("observed_at")), _time(publication.get("uploaded_at")), _time(receiver["end"]["at"])
    # Core worker recovery precedes closure; normal publication may be observed later.
    _require(_time(final["utc"]) <= output_at <= prepared <= emitted <= resolved < closed
             and emitted <= ack < closed and max(resolved, ack, uploaded) <= observed
             and receipt_at <= uploaded,
             "output event ACK or publication ordering is invalid")


def validate_ack_drill_evidence(journal: Any, receiver_audit: Any) -> dict[str, Any]:
    """Return passed/failed/inconclusive; raw records are never echoed.

Durable collections must be complete queries scoped to the target operation,
node, task, attested delivery and video linkage, not a preselected successful row.
Only the collector can establish that query scope and provenance. Passing here
does not prove arbitrary process-crash recovery or close other production gates.
Worker output, event completion and ACK must precede core receiver closure;
the durable snapshot may follow normal unlisted publication/promotion.
"""
    counts = {"upload_posts": 0, "completed_gets": 0, "consumed_tokens": 0}
    try:
        first, events = _journal(journal, counts)
        _receiver(receiver_audit, first, events, counts)
        _durable(journal, first, events, receiver_audit)
    except _EvidenceError as exc:
        return {"status": exc.status, "reasons": [exc.reason], "counts": counts}
    except (TypeError, ValueError, OverflowError, UnicodeError, RecursionError):
        return {"status": "inconclusive", "reasons": ["malformed evidence"], "counts": counts}
    return {"status": "passed", "reasons": [], "counts": counts}
