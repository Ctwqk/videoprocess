"""Read-only inventory history evidence. This module grants no production authority."""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, cast

from sqlalchemy import JSON, func, literal, select

from app.models.artifact import Artifact
from app.models.asset import Asset
from app.models.channel_agent import (
    ChannelOpsQueueItem, ChannelProfile, FeedbackSnapshot, ManualSeed,
    ProductionTask, PublicationMetricSchedule, PublicationRecord, PublishingAccount,
)
from app.models.job import Job, NodeExecution
from app.models.legacy_worker_event_resolution import LegacyWorkerEventResolution
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.publication_promotion_operation import PublicationPromotionOperation
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery, RegisteredWorkerEventReceipt, WorkerEventEmission,
    WorkerTaskDeliveryAttestation, WorkerTaskDispatch, WorkerRedisMarkerCleanupAuthorization, WorkerRedisMarkerRepairAudit,
)
from app.models.schedule import RuntimeSchedule
from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.schemas.channel_agent import OwnedSeedInventoryCreate
from app.schemas.pipeline import PipelineDefinition
from app.node_registry.registry import NodeTypeRegistry
from app.orchestrator.dag import topological_sort
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventError, canonical_redis_payload_sha256, parse_registered_worker_event,
)

MAX_ROWS = 4096
MAX_BYTES = 16 * 1024 * 1024
MAX_OBSERVATION_AGE_SECONDS = 60
_UC = re.compile(r"UC[A-Za-z0-9_-]{22}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_REDIS_ID = re.compile(r"[0-9]+-[0-9]+\Z")
RETIRED_TUPLE = (
    "c25b9c38-b96a-4a21-80d0-352180cea206", "70d27dfb-f0c5-438c-bdfa-5316dc4f209b",
    "8061df32-3184-4c99-a5aa-556744a43ba5", "4c1b523f-0a35-45fd-990f-095e8156de2e",
    "2c4184d5-a02e-41e3-aeeb-16db8122f6e1", "4057a1a3-c37c-4bae-85a9-6d9d3dcac869",
)
TERMINAL_TABLES = (
    "node_executions", "artifacts", "worker_task_dispatches", "worker_task_delivery_attestations",
    "worker_event_emissions", "registered_worker_event_receipts", "registered_worker_event_deliveries",
    "worker_registrations", "worker_admission_grants", "legacy_worker_event_resolutions", "channel_ops_queue_items",
    "worker_redis_marker_cleanup_authorizations", "worker_redis_marker_repair_audits",
)


class OwnedHistoryError(ValueError):
    """Only static reason codes may cross the evidence boundary."""


def _require(condition: bool, reason: str = "owned_history_invalid") -> None:
    if not condition:
        raise OwnedHistoryError(reason)


def _time(value: Any) -> datetime:
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        _require(isinstance(value, datetime))
        # Existing job/worker timestamp columns are naive UTC; authority facts are not.
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise OwnedHistoryError("owned_history_invalid") from None


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise OwnedHistoryError("owned_history_invalid_json")


def _pairs(pairs: list[tuple[str, Any]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        _require(key not in result, "owned_history_invalid_json")
        result[key] = value
    return result


def _check_json(value: Any) -> None:
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, dict):
        for key, item in value.items():
            _require(isinstance(key, str), "owned_history_invalid_json")
            _check_json(key)
            _check_json(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _check_json(item)


@dataclass(frozen=True, repr=False)
class FrozenJSON:
    canonical_json: str

    @classmethod
    def from_value(cls, value: Any) -> FrozenJSON:
        try:
            _check_json(value)
            encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=True, allow_nan=False, default=_json_default)
            _require(len(encoded) <= MAX_BYTES, "owned_history_too_large")
            _check_json(json.loads(encoded))
            return cls(encoded)
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise OwnedHistoryError("owned_history_invalid_json") from None

    @classmethod
    def from_json(cls, raw: str) -> FrozenJSON:
        try:
            _require(len(raw) <= MAX_BYTES, "owned_history_too_large")
            value = json.loads(raw, object_pairs_hook=_pairs,
                               parse_constant=lambda _: _require(False, "owned_history_invalid_json"))
            return cls.from_value(value)
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise OwnedHistoryError("owned_history_invalid_json") from None

    def as_dict(self) -> Any:
        return json.loads(self.canonical_json)


def history_sha256(value: Any) -> str:
    frozen = value if isinstance(value, FrozenJSON) else FrozenJSON.from_value(value)
    return hashlib.sha256(frozen.canonical_json.encode("ascii")).hexdigest()


def _exact(value: Any, names: str) -> dict:
    _require(isinstance(value, dict) and set(value) == set(names.split()))
    return value


def _id(value: Any) -> str:
    try:
        _require(isinstance(value, str) and str(uuid.UUID(value)) == value)
        return value
    except (ValueError, TypeError, AttributeError):
        raise OwnedHistoryError("owned_history_invalid") from None


def _hash(value: Any) -> str:
    _require(isinstance(value, str) and bool(_SHA.fullmatch(value)))
    return value


def _utc_fact(value: Any) -> datetime:
    _require(isinstance(value, str) and _time(value).isoformat() == value)
    return _time(value)


@dataclass(frozen=True)
class HistoryOperationLocator:
    """Untrusted lookup coordinates, deliberately without authority fields."""
    operation_id: str
    legacy_account_id: str
    legacy_channel_profile_id: str

    @classmethod
    def parse(cls, value: dict) -> HistoryOperationLocator:
        _exact(value, "operation_id legacy_account_id legacy_channel_profile_id")
        return cls(*(_id(value[key]) for key in ("operation_id", "legacy_account_id", "legacy_channel_profile_id")))


@dataclass(frozen=True, repr=False)
class RedisTerminalObservation:
    kind: str
    redis_stream: str
    consumer_group: str
    message_id: str | None
    dispatch_key: str | None
    payload_sha256: str
    marker_message_id: str | None
    pending_message_ids: tuple[str, ...]
    observed_at: datetime

    @classmethod
    def parse(cls, value: dict) -> RedisTerminalObservation:
        _exact(value, "kind redis_stream consumer_group message_id dispatch_key payload_sha256 marker_message_id pending_message_ids observed_at")
        _require(value["kind"] in {"task", "event"})
        for field in ("message_id", "marker_message_id"):
            _require(value[field] is None or isinstance(value[field], str) and bool(_REDIS_ID.fullmatch(value[field])))
        _require(isinstance(value["pending_message_ids"], list) and len(value["pending_message_ids"]) <= MAX_ROWS)
        _require(all(isinstance(v, str) and _REDIS_ID.fullmatch(v) for v in value["pending_message_ids"]))
        if value["kind"] == "task":
            _id(value["dispatch_key"])
        else:
            _require(value["dispatch_key"] is None and value["marker_message_id"] is None and value["message_id"] is not None)
        return cls(value["kind"], _text(value["redis_stream"]), _text(value["consumer_group"]), value["message_id"],
                   value["dispatch_key"], _hash(value["payload_sha256"]), value["marker_message_id"],
                   tuple(value["pending_message_ids"]), _utc_fact(value["observed_at"]))


@dataclass(frozen=True, repr=False)
class RetiredSourceFact:
    asset: FrozenJSON
    content_sha256: str


@dataclass(frozen=True, repr=False)
class RetainedPreuploadFacts:
    operation: FrozenJSON
    task: FrozenJSON
    job: FrozenJSON
    upload_node: FrozenJSON
    account: FrozenJSON
    channel: FrozenJSON
    manual_seed: FrozenJSON
    source_assets: tuple[RetiredSourceFact, ...]

    @classmethod
    def parse(cls, value: dict) -> RetainedPreuploadFacts:
        _exact(value, "operation task job upload_node account channel manual_seed source_assets")
        pairs = (("operation", "youtube_upload_operations"), ("task", "production_tasks"), ("job", "jobs"),
                 ("upload_node", "node_executions"), ("account", "publishing_accounts"),
                 ("channel", "channel_profiles"), ("manual_seed", "manual_seeds"))
        retained = [FrozenJSON.from_value(_complete_row(value[key], table)) for key, table in pairs]
        _require(isinstance(value["source_assets"], list) and 1 <= len(value["source_assets"]) <= 7)
        assets = []
        for source in value["source_assets"]:
            _exact(source, "asset content_sha256")
            assets.append(RetiredSourceFact(FrozenJSON.from_value(_complete_row(source["asset"], "assets")), _hash(source["content_sha256"])))
        ids = tuple(a.asset.as_dict()["id"] for a in assets)
        _require(ids == tuple(sorted(set(ids))))
        return cls(retained[0], retained[1], retained[2], retained[3], retained[4], retained[5], retained[6], tuple(assets))


@dataclass(frozen=True, repr=False)
class TerminalGraph:
    node_executions: tuple[FrozenJSON, ...]
    artifacts: tuple[FrozenJSON, ...]
    worker_task_dispatches: tuple[FrozenJSON, ...]
    worker_task_delivery_attestations: tuple[FrozenJSON, ...]
    worker_event_emissions: tuple[FrozenJSON, ...]
    registered_worker_event_receipts: tuple[FrozenJSON, ...]
    registered_worker_event_deliveries: tuple[FrozenJSON, ...]
    worker_registrations: tuple[FrozenJSON, ...]
    worker_admission_grants: tuple[FrozenJSON, ...]
    legacy_worker_event_resolutions: tuple[FrozenJSON, ...]
    channel_ops_queue_items: tuple[FrozenJSON, ...]
    worker_redis_marker_cleanup_authorizations: tuple[FrozenJSON, ...]
    worker_redis_marker_repair_audits: tuple[FrozenJSON, ...]

    @classmethod
    def parse(cls, value: dict) -> TerminalGraph:
        _exact(value, " ".join(TERMINAL_TABLES))
        groups = []
        for name in TERMINAL_TABLES:
            records = value[name]
            _require(isinstance(records, list) and len(records) <= MAX_ROWS)
            _require([r["id"] for r in records] == sorted({r["id"] for r in records}))
            groups.append(tuple(FrozenJSON.from_value(_complete_row(r, name)) for r in records))
        return cls(*groups)

    def as_dict(self) -> dict:
        return {name: [r.as_dict() for r in getattr(self, name)] for name in TERMINAL_TABLES}


@dataclass(frozen=True, repr=False)
class RetiredPreuploadCertificate:
    operation_id: str
    task_id: str
    job_id: str
    upload_node_id: str
    legacy_account_id: str
    legacy_channel_profile_id: str
    retained_facts: RetainedPreuploadFacts
    terminal_graph: TerminalGraph
    terminal_graph_sha256: str
    transition_sha256: str
    observed_at: datetime
    server_subject: str
    approval_reference: str
    document: FrozenJSON

    @classmethod
    def parse(cls, value: dict) -> RetiredPreuploadCertificate:
        keys = "operation_id task_id job_id upload_node_id legacy_account_id legacy_channel_profile_id"
        _exact(value, keys + " classification retained_facts terminal_graph terminal_graph_sha256 transition_sha256 observed_at server_subject approval_reference")
        identities = tuple(_id(value[k]) for k in keys.split())
        _require(identities == RETIRED_TUPLE and value["classification"] == "retired_unassigned_preupload")
        retained = RetainedPreuploadFacts.parse(value["retained_facts"])
        graph = TerminalGraph.parse(value["terminal_graph"])
        _require(history_sha256(graph.as_dict()) == _hash(value["terminal_graph_sha256"]) and
                 history_sha256(retained.task.as_dict()["transition_history_json"]) == _hash(value["transition_sha256"]))
        return cls(identities[0], identities[1], identities[2], identities[3], identities[4], identities[5],
                   retained, graph, value["terminal_graph_sha256"], value["transition_sha256"],
                   _utc_fact(value["observed_at"]), _text(value["server_subject"]), _text(value["approval_reference"]), FrozenJSON.from_value(value))


@dataclass(frozen=True, repr=False)
class QualifiedUploadFact:
    operation_id: str
    manager_task_id: str
    platform_video_id: str
    actual_platform_channel_id: str
    operation_sha256: str
    receipt_sha256: str
    observed_at: datetime

    @classmethod
    def parse(cls, value: dict) -> QualifiedUploadFact:
        _exact(value, "operation_id manager_task_id platform_video_id actual_platform_channel_id operation_sha256 receipt_sha256 observed_at")
        _require(bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", value["platform_video_id"])) and
                 bool(_UC.fullmatch(value["actual_platform_channel_id"])))
        return cls(_id(value["operation_id"]), _id(value["manager_task_id"]), value["platform_video_id"],
                   value["actual_platform_channel_id"], _hash(value["operation_sha256"]),
                   _hash(value["receipt_sha256"]), _utc_fact(value["observed_at"]))


def _text(value: Any) -> str:
    _require(isinstance(value, str) and bool(value.strip()) and len(value) <= 512)
    return value


@dataclass(frozen=True, repr=False)
class UploadQualification:
    observed_at: datetime
    server_subject: str
    manager_endpoint_identity: str
    facts: tuple[QualifiedUploadFact, ...]
    facts_sha256: str
    approval_reference: str
    document: FrozenJSON

    @classmethod
    def parse(cls, value: dict) -> UploadQualification:
        _exact(value, "observed_at server_subject manager_endpoint_identity manager_task_id platform_video_id "
               "actual_platform_channel_id sanitized_facts facts_sha256 approval_reference")
        _require(isinstance(value["sanitized_facts"], list) and 1 <= len(value["sanitized_facts"]) <= 128)
        facts = tuple(QualifiedUploadFact.parse(f) for f in value["sanitized_facts"])
        _require(tuple(f.operation_id for f in facts) == tuple(sorted({f.operation_id for f in facts})))
        first = facts[0]
        _require(value["manager_task_id"] == first.manager_task_id and value["platform_video_id"] == first.platform_video_id and
                 all(f.actual_platform_channel_id == value["actual_platform_channel_id"] for f in facts) and
                 history_sha256(value["sanitized_facts"]) == _hash(value["facts_sha256"]))
        observed = _utc_fact(value["observed_at"])
        _require(all(f.observed_at <= observed for f in facts))
        endpoint = value["manager_endpoint_identity"]
        _require(isinstance(endpoint, str) and endpoint.startswith("sha256:") and bool(_SHA.fullmatch(endpoint[7:])))
        return cls(observed, _text(value["server_subject"]), endpoint, facts, value["facts_sha256"],
                   _text(value["approval_reference"]), FrozenJSON.from_value(value))


@dataclass(frozen=True, repr=False)
class HistoryOnlyBinding:
    legacy_account_id: str
    legacy_channel_profile_id: str
    canonical_platform_channel_id: str
    account_descriptor_sha256: str
    qualified_operation_ids: tuple[str, ...]
    qualification: UploadQualification
    document: FrozenJSON

    @classmethod
    def parse(cls, value: dict) -> HistoryOnlyBinding:
        _exact(value, "legacy_account_id legacy_channel_profile_id platform canonical_platform_channel_id use "
               "account_descriptor_sha256 qualified_operation_ids qualification")
        _require(value["platform"] == "youtube" and value["use"] == "history_only" and
                 bool(_UC.fullmatch(value["canonical_platform_channel_id"])))
        _require(isinstance(value["qualified_operation_ids"], list))
        ids = tuple(_id(v) for v in value["qualified_operation_ids"])
        qualification = UploadQualification.parse(value["qualification"])
        _require(ids == tuple(f.operation_id for f in qualification.facts) and
                 all(f.actual_platform_channel_id == value["canonical_platform_channel_id"] for f in qualification.facts))
        return cls(_id(value["legacy_account_id"]), _id(value["legacy_channel_profile_id"]), value["canonical_platform_channel_id"],
                   _hash(value["account_descriptor_sha256"]), ids, qualification, FrozenJSON.from_value(value))


@dataclass(frozen=True, repr=False)
class LegacyHistoryFacts:
    bindings: tuple[HistoryOnlyBinding, ...] = ()
    retired_unassigned_preupload: RetiredPreuploadCertificate | None = None


@dataclass(frozen=True, repr=False)
class HistoryManifest:
    version: int
    document: FrozenJSON
    legacy_history: LegacyHistoryFacts | None


def decode_history_manifest(value: Mapping | FrozenJSON) -> HistoryManifest:
    """Decode retained authority, not an API request or a manifest upgrade."""
    try:
        document = value if isinstance(value, FrozenJSON) else FrozenJSON.from_value(value)
        data = document.as_dict()
        version = data["version"]
        _require(type(version) is int and version in {1, 2})
        _exact(data, "version inventory_id channel_profile_id topic_lane_id lane_format_id target_account_id "
               "platform_channel_id starts_at expires_at privacy max_admissions minimum_interval_seconds "
               "tick_interval_minutes configuration_sha256 entries" + (" legacy_history" if version == 2 else ""))
        _id(data["inventory_id"])
        _id(data["channel_profile_id"])
        _hash(data["configuration_sha256"])
        _require(type(data["tick_interval_minutes"]) is int and data["tick_interval_minutes"] == 1)
        _utc_fact(data["starts_at"])
        _utc_fact(data["expires_at"])
        entries: list[dict] = []
        for entry in data["entries"]:
            _exact(entry, "id ordinal asset_id manual_seed_id content_sha256 byte_size storage_descriptor "
                   "provenance_evidence provenance_sha256 seed_sha256 prompt title_seed")
            _id(entry["id"])
            _id(entry["manual_seed_id"])
            _require(type(entry["ordinal"]) is int and entry["ordinal"] == len(entries) + 1)
            _require(type(entry["byte_size"]) is int and 0 < entry["byte_size"] <= 67108864)
            descriptor = _exact(entry["storage_descriptor"], "id storage_backend storage_path file_size mime_type media_info_sha256")
            _require(descriptor["id"] == entry["asset_id"] and descriptor["file_size"] == entry["byte_size"])
            _hash(descriptor["media_info_sha256"])
            _require(history_sha256(entry["provenance_evidence"]) == _hash(entry["provenance_sha256"]))
            _hash(entry["seed_sha256"])
            entries.append({"asset_id": entry["asset_id"], "expected_content_sha256": entry["content_sha256"],
                            "provenance_evidence": entry["provenance_evidence"], "prompt": entry["prompt"],
                            "title_seed": entry["title_seed"]})
        _require(len({e["id"] for e in data["entries"]}) == 7 and
                 len({e["manual_seed_id"] for e in data["entries"]}) == 7)
        OwnedSeedInventoryCreate.model_validate({
            **{key: data[key] for key in ("topic_lane_id", "lane_format_id", "target_account_id", "platform_channel_id",
                                          "starts_at", "expires_at", "privacy", "max_admissions", "minimum_interval_seconds")},
            "client_request_id": data["inventory_id"], "entries": entries,
        })
        legacy = None
        if version == 2:
            section = _exact(data["legacy_history"], "version bindings retired_unassigned_preupload")
            _require(type(section["version"]) is int and section["version"] == 1)
            _require(isinstance(section["bindings"], list) and len(section["bindings"]) <= 32)
            bindings = tuple(HistoryOnlyBinding.parse(b) for b in section["bindings"])
            _require(tuple(b.legacy_account_id for b in bindings) == tuple(sorted({b.legacy_account_id for b in bindings})))
            _require(all(b.canonical_platform_channel_id == data["platform_channel_id"] and
                         b.legacy_account_id != data["target_account_id"] for b in bindings))
            raw_certificate = section["retired_unassigned_preupload"]
            certificate = RetiredPreuploadCertificate.parse(raw_certificate) if raw_certificate is not None else None
            if certificate is not None:
                _require(data["channel_profile_id"] != certificate.legacy_channel_profile_id and
                         data["target_account_id"] != certificate.legacy_account_id and
                         all(b.legacy_account_id != certificate.legacy_account_id for b in bindings))
            legacy = LegacyHistoryFacts(bindings, certificate)
        return HistoryManifest(version, document, legacy)
    except (ValueError, TypeError, KeyError, AttributeError):
        raise OwnedHistoryError("owned_history_manifest_invalid") from None


# Explicit existing models, not a runtime catalogue. Do not join away orphans.
HISTORY_MODELS = {model.__tablename__: model for model in (
    OwnedSeedInventory, OwnedSeedInventoryItem, YouTubeUploadOperation, ProductionTask,
    PublishingAccount, ChannelProfile, Job, NodeExecution, Artifact, Asset, ManualSeed,
    PublicationRecord, PublicationMetricSchedule, FeedbackSnapshot, ChannelOpsQueueItem,
    WorkerTaskDispatch, WorkerTaskDeliveryAttestation, WorkerEventEmission,
    RegisteredWorkerEventReceipt, RegisteredWorkerEventDelivery, WorkerRegistration,
    WorkerAdmissionGrant, LegacyWorkerEventResolution, RuntimeSchedule, PublicationPromotionOperation,
    WorkerRedisMarkerCleanupAuthorization, WorkerRedisMarkerRepairAudit,
)}


def _complete_row(row: dict, table: str) -> dict:
    columns = [c for c in HISTORY_MODELS[table].__table__.columns if c.name not in {"lease_secret_sha256", "token_sha256"}]
    _exact(row, " ".join(c.name for c in columns))
    _require(all(row[c.name] is not None for c in columns if not c.nullable), "owned_history_incomplete")
    _id(row["id"])
    return row


@dataclass(frozen=True, repr=False)
class OwnedHistorySnapshot:
    platform_channel_id: str
    observed_at: datetime
    rows: FrozenJSON
    redis_observations: tuple[RedisTerminalObservation, ...] = ()

    @classmethod
    def from_rows(cls, rows: Mapping, *, platform_channel_id: str, observed_at: datetime,
                  redis_observations: tuple[RedisTerminalObservation, ...] = ()) -> OwnedHistorySnapshot:
        _require(isinstance(platform_channel_id, str) and bool(_UC.fullmatch(platform_channel_id)))
        _require(set(rows) == set(HISTORY_MODELS), "owned_history_incomplete")
        data = FrozenJSON.from_value(dict(rows)).as_dict()
        for name, records in data.items():
            _require(isinstance(records, list) and len(records) <= MAX_ROWS, "owned_history_incomplete")
            key = "service_name" if name == "runtime_schedules" else "id"
            _require(all(isinstance(row, dict) and isinstance(row.get(key), str) for row in records))
            _require(len({row[key] for row in records}) == len(records), "owned_history_duplicate")
            records.sort(key=lambda row: row[key])
        _require(all(isinstance(r, RedisTerminalObservation) for r in redis_observations))
        return cls(platform_channel_id, _time(observed_at), FrozenJSON.from_value(data), tuple(redis_observations))


@dataclass(frozen=True)
class OperationClassification:
    operation_id: str
    classification: str
    platform_channel_id: str | None
    account_id: str


@dataclass(frozen=True)
class TerminalPath:
    record_id: str
    path: str


@dataclass(frozen=True)
class OwnedHistoryAssessment:
    block_reason: str | None
    classifications: tuple[OperationClassification, ...] = ()
    account_ids: tuple[str, ...] = ()
    retired_source_sha256: tuple[str, ...] = ()
    retired_render_sha256: tuple[str, ...] = ()
    authority_sha256: str = ""
    stable_history_sha256: str = ""
    wait_reason: str | None = None
    completed_item_ids: tuple[str, ...] = ()
    terminal_paths: tuple[TerminalPath, ...] = ()


async def load_owned_history_evidence(db, *, platform_channel_id: str) -> OwnedHistorySnapshot:
    """One MVCC statement, no locks or writes; the caller owns the DB transaction.

    Limits reject, never truncate. Redis observations must be supplied separately by
    a future server qualifier; this DB-only loader cannot certify retirement alone.
    """
    try:
        fields: list[Any] = []
        for name, model in HISTORY_MODELS.items():
            table = model.__table__
            columns = [c for c in table.columns if c.name not in {"lease_secret_sha256", "token_sha256"}]
            bounded = select(*columns).order_by(*table.primary_key).limit(MAX_ROWS + 1).subquery()
            rows = select(func.coalesce(func.json_agg(func.row_to_json(bounded.table_valued())),
                                        literal("[]").cast(JSON))).scalar_subquery()
            fields.extend((literal(name), rows))
        statement = select(func.json_build_object(
            "observed_at", func.clock_timestamp(), "rows", func.json_build_object(*fields)))
        result = await db.scalar(statement)
        return OwnedHistorySnapshot.from_rows(result["rows"], platform_channel_id=platform_channel_id,
                                              observed_at=result["observed_at"])
    except Exception:
        raise OwnedHistoryError("owned_history_read_failed") from None


def _one(records: list[dict], reason: str) -> dict:
    _require(len(records) == 1, reason)
    return records[0]


def _fields(row: dict, names: str) -> dict:
    return {name: row[name] for name in names.split()}


def account_descriptor_sha256(account: Mapping) -> str:
    return history_sha256(_fields(dict(account), "id channel_profile_id platform platform_account_id credential_ref platform_specific_config_json"))


def _int(value: Any) -> int:
    _require(type(value) is int)
    return value


def _z(at: datetime) -> str:
    return at.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _queue_clean(q: dict) -> bool:
    return q.get("last_error") is None and q.get("dead_letter_at") is None and _int(q.get("attempt_count")) in {0, 1}


def _queue_state(q: dict, allowed: set[str]) -> bool:
    if not _queue_clean(q) or q.get("status") not in allowed:
        return False
    if q["status"] == "queued":
        return q["attempt_count"] == 0 and q.get("locked_at") is None and q.get("locked_by") is None
    if q["status"] == "running":
        return q["attempt_count"] == 1 and bool(q.get("locked_by")) and bool(_time(q.get("locked_at")))
    return q["attempt_count"] == 1 and q.get("locked_at") is None and q.get("locked_by") is None


def _task_history(rows: dict, task: dict) -> dict:
    pubs = [r for r in rows["publication_records"] if r.get("production_task_id") == task["id"]]
    pub_ids = {r["id"] for r in pubs}
    metrics = [r for r in rows["publication_metric_schedules"] if r.get("publication_id") in pub_ids]
    metric_ids = {r["id"] for r in metrics}
    queues = []
    for q in rows["channel_ops_queue_items"]:
        payload = q.get("payload_json", {})
        if (payload.get("production_task_id") == task["id"] or payload.get("publication_id") in pub_ids or
                payload.get("metric_schedule_id") in metric_ids):
            queues.append(q)
    # Include related parents/children even if their payload has drifted.
    while True:
        ids = {q["id"] for q in queues}
        parents = {q.get("parent_queue_item_id") for q in queues} - {None}
        expanded = [q for q in rows["channel_ops_queue_items"] if
                    q["id"] in ids | parents or q.get("parent_queue_item_id") in ids]
        if len(expanded) == len(queues):
            break
        queues = expanded
    jobs = [r for r in rows["jobs"] if r["id"] == task.get("job_id")]
    nodes = [r for r in rows["node_executions"] if r.get("job_id") == task.get("job_id")]
    outputs = {n.get("output_artifact_id") for n in nodes} - {None}
    return {"task": task, "operations": [r for r in rows["youtube_upload_operations"] if r.get("production_task_id") == task["id"]],
            "job": jobs[0] if jobs else {}, "nodes": nodes,
            "artifacts": [r for r in rows["artifacts"] if r.get("job_id") == task.get("job_id") and r["id"] in outputs],
            "publications": pubs, "queues": queues, "metrics": metrics,
            "feedback": [r for r in rows["feedback_snapshots"] if r.get("publication_id") in pub_ids]}


def _reconciled(h: dict, pub: dict, start: datetime) -> bool:
    reason = "owned_inventory_reconciliation"
    q = _one([q for q in h["queues"] if q["kind"] == "reconcile_publication"], reason)
    _require(_time(q["run_after"]) == start + timedelta(minutes=30) and
             q["idempotency_key"] == f"reconcile_publication:{pub['id']}:{_z(start)}" and
             q["payload_json"].get("publication_id") == pub["id"] and q["channel_profile_id"] == h["task"]["channel_profile_id"] and
             _queue_state(q, {"queued", "running", "succeeded"}), reason)
    parent = _one([p for p in h["queues"] if p["id"] == q.get("parent_queue_item_id")], reason)
    _require(parent["kind"] == "promote_publication" and parent["channel_profile_id"] == q["channel_profile_id"] and
             parent["payload_json"].get("publication_id") == pub["id"] and
             parent["payload_json"].get("target_visibility") == "unlisted", reason)
    if _queue_state(parent, {"running"}) and q["status"] == "queued":
        return False
    _require(_queue_state(parent, {"succeeded"}), reason)
    return q["status"] == "succeeded"


def _pending_promotion(h: dict, pub: dict, completed: datetime, now: datetime) -> bool:
    try:
        uploaded = _time(pub["uploaded_at"])
        _require(completed <= uploaded <= now and pub["publish_status"] == "uploaded" and
                 h["task"]["state"] == "uploaded_private" and not h["metrics"] and not h["feedback"])
        _require(not any(q["kind"] in {"reconcile_publication", "collect_metrics"} for q in h["queues"]))
        q = _one([q for q in h["queues"] if q["kind"] == "promote_publication"], "owned_history_invalid")
        due = uploaded + timedelta(hours=1)
        _require(_time(q["run_after"]) == due and
                 q["idempotency_key"] == f"promote_publication:{pub['id']}:unlisted:{_z(due)}" and
                 q["channel_profile_id"] == h["task"]["channel_profile_id"] and
                 q["payload_json"].get("publication_id") == pub["id"] and
                 q["payload_json"].get("target_visibility") == "unlisted" and
                 q["payload_json"].get("scheduled_at") == _z(due) and _queue_state(q, {"queued", "running"}))
        parent = _one([p for p in h["queues"] if p["id"] == q.get("parent_queue_item_id")], "owned_history_invalid")
        return (parent["kind"] == "publish_task" and parent["channel_profile_id"] == q["channel_profile_id"] and
                parent["payload_json"].get("production_task_id") == h["task"]["id"] and
                _queue_state(parent, {"running", "succeeded"}))
    except (ValueError, TypeError, KeyError):
        return False


_METRIC_STAGES = (("1h", 1, 3), ("6h", 6, 12), ("24h", 24, 30), ("72h", 72, 84), ("7d", 168, 192))


def _metrics_ready(h: dict, pub: dict, start: datetime, now: datetime) -> bool:
    """Return a normal wait, or reject an unauthentic/expired native retry chain."""
    reason = "owned_inventory_metrics"
    _require(len(h["metrics"]) == len(_METRIC_STAGES), reason)
    metric_ids = {m["id"] for m in h["metrics"]}
    _require(all(q["payload_json"].get("metric_schedule_id") in metric_ids for q in h["queues"] if q["kind"] == "collect_metrics"), reason)
    wait = False
    for stage, due_hours, grace_hours in _METRIC_STAGES:
        m = _one([m for m in h["metrics"] if m["snapshot_stage"] == stage], reason)
        due, grace = start + timedelta(hours=due_hours), start + timedelta(hours=grace_hours)
        _require(m["publication_id"] == pub["id"] and _time(m["effective_start_at"]) == start and
                 _time(m["due_at"]) == due and _time(m["grace_until"]) == grace, reason)
        succeeded = m["status"] == "succeeded"
        attempts = _int(m["attempt_count"])
        last_index = attempts - int(succeeded)
        _require(m["status"] in {"pending", "succeeded"} and 0 <= last_index <= 1024, reason)
        _require(m.get("last_error_code") == (None if succeeded or attempts == 0 else "metrics_unavailable"), reason)
        if attempts:
            _require(due <= _time(m["last_attempt_at"]) <= min(now, grace), reason)
        else:
            _require(m.get("last_attempt_at") is None, reason)
        chain = [q for q in h["queues"] if q["kind"] == "collect_metrics" and q["payload_json"].get("metric_schedule_id") == m["id"]]
        _require(len(chain) == last_index + 1, reason)
        chain.sort(key=lambda q: _int(q["payload_json"].get("metrics_poll_count")))
        for index, q in enumerate(chain):
            p = q["payload_json"]
            _id(q["id"])
            _require(_int(p.get("metrics_poll_count")) == index and p.get("publication_id") == pub["id"] and
                     p.get("snapshot_stage") == stage and q["channel_profile_id"] == h["task"]["channel_profile_id"] and
                     q["idempotency_key"] == f"collect_metrics:{pub['id']}:stage:{stage}:attempt:{index}", reason)
            run = _time(q["run_after"])
            _require(run <= grace, reason)
            if index:
                _require(run > _time(chain[index-1]["run_after"]) and q.get("parent_queue_item_id") == chain[index-1]["id"], reason)
            else:
                _require(run == due, reason)
                parent = _one([p for p in h["queues"] if p["id"] == q.get("parent_queue_item_id")], reason)
                _require(parent["kind"] == "promote_publication" and _queue_state(parent, {"succeeded"}) and
                         parent["channel_profile_id"] == q["channel_profile_id"] and
                         parent["payload_json"].get("publication_id") == pub["id"] and
                         parent["payload_json"].get("target_visibility") == "unlisted", reason)
            _require(_queue_state(q, {"queued", "running", "succeeded"}), reason)
            if q["status"] == "succeeded":
                _require(index < last_index or succeeded, reason)
            else:
                if index < last_index:
                    _require(index == last_index - 1 and q["status"] == "running" and not succeeded, reason)
                wait |= succeeded or index < last_index
        feedback = [f for f in h["feedback"] if f["snapshot_stage"] == stage]
        if succeeded:
            done = _time(m["completed_at"])
            _require(done == _time(m["last_attempt_at"]) and due <= done <= min(now, grace) and
                     done >= _time(chain[-1]["run_after"]) and len(feedback) == 1, reason)
        else:
            _require(m.get("completed_at") is None and now < grace and not feedback, reason)
            if attempts:
                _require(_time(chain[-1]["run_after"]) > _time(m["last_attempt_at"]), reason)
            wait |= now >= due
    _require(all(f.get("snapshot_stage") in {s[0] for s in _METRIC_STAGES} for f in h["feedback"]), reason)
    return wait


def _settled_replacement(h: dict) -> dict | None:
    try:
        pub = _one(h["publications"], "owned_history_invalid")
        uploaded, start = _time(pub["uploaded_at"]), _time(pub["scheduled_publish_at"])
        _require(uploaded <= start and pub["desired_privacy"] == pub["current_privacy"] == "unlisted" and pub.get("public_at") is None)
        promotes = [q for q in h["queues"] if q["kind"] == "promote_publication"]
        auto = _one([q for q in promotes if q["status"] == "cancelled"], "owned_history_invalid")
        manual = _one([q for q in promotes if q["status"] != "cancelled"], "owned_history_invalid")
        _id(auto["id"])
        _id(manual["id"])
        due, channel = uploaded + timedelta(hours=1), h["task"]["channel_profile_id"]
        _require(auto["id"] != manual["id"] and _time(auto["run_after"]) == due and
                 uploaded <= _time(auto["dead_letter_at"]) <= _time(manual["run_after"]) <= start and
                 auto["last_error"] == "replaced_by_immediate_unlisted_canary_promotion" and
                 auto["attempt_count"] == 0 and auto.get("locked_at") is None and auto.get("locked_by") is None and
                 auto["channel_profile_id"] == channel and auto["idempotency_key"] == f"promote_publication:{pub['id']}:unlisted:{_z(due)}" and
                 auto["payload_json"].get("publication_id") == pub["id"] and
                 auto["payload_json"].get("target_visibility") == "unlisted" and auto["payload_json"].get("scheduled_at") == _z(due))
        _require(_queue_state(manual, {"succeeded"}) and manual.get("parent_queue_item_id") is None and
                 manual["channel_profile_id"] == channel and manual["idempotency_key"] == f"promote_publication:{pub['id']}:unlisted:manual" and
                 manual["payload_json"].get("publication_id") == pub["id"] and manual["payload_json"].get("target_visibility") == "unlisted" and
                 manual["payload_json"].get("channel_profile_id") == channel and manual["payload_json"].get("scheduled_at") is None)
        parent = _one([q for q in h["queues"] if q["id"] == auto.get("parent_queue_item_id")], "owned_history_invalid")
        _require(parent["kind"] == "publish_task" and parent["channel_profile_id"] == channel and
                 parent["payload_json"].get("production_task_id") == h["task"]["id"] and _queue_state(parent, {"succeeded"}))
        _require(_reconciled(h, pub, start))
        reconcile = _one([q for q in h["queues"] if q["kind"] == "reconcile_publication"], "owned_history_invalid")
        _require(reconcile["parent_queue_item_id"] == manual["id"])
        return {"automatic": auto, "manual": manual, "reconcile": reconcile, "publish_parent": parent}
    except (ValueError, TypeError, KeyError):
        return None


def _normal_history(h: dict, item: dict | None, now: datetime) -> tuple[str | None, bool, dict | None]:
    """Reviewed Go Task-2 semantics, shared by future Python admission/guards."""
    task = h["task"]
    own = item is not None
    reserved = item is not None and item["state"] == "reserved"
    replacement = None if own else _settled_replacement(h)
    _require(task.get("retry_count") == 0 and task.get("failure_reason") is None and
             task.get("blocked_by_guard") is None and task["state"] in
             {"selected", "planning", "producing", "scheduled", "uploaded_private", "measured"}, "owned_inventory_task_failed")
    for q in h["queues"]:
        if replacement is not None and q["id"] == replacement["automatic"]["id"]:
            continue
        _require(_queue_clean(q) and q["status"] in {"queued", "running", "succeeded"} and
                 q["channel_profile_id"] == task["channel_profile_id"], "owned_inventory_queue_failed")
    _require(not h["job"] or h["job"]["status"] in {"SUCCEEDED", "RUNNING", "PENDING", "WAITING_WINDOW", "VALIDATING", "PLANNING"},
             "owned_inventory_job_outcome")
    if not h["operations"] and reserved:
        return "owned_inventory_outstanding", False, None
    op = _one(h["operations"], "owned_inventory_operation_count")
    _require(op["production_task_id"] == task["id"] and op["job_id"] == task["job_id"] and
             bool(_SHA.fullmatch(op["content_sha256"])) and op.get("error_message") is None, "owned_inventory_operation_identity")
    if op["status"] != "succeeded":
        _require(reserved and op["status"] in {"reserved", "attempted", "submitted"}, "owned_inventory_operation_unresolved")
        return "owned_inventory_outstanding", False, None
    attempted, completed = _time(op["request_attempted_at"]), _time(op["completed_at"])
    receipt = _exact(op["receipt_json"], "video_id url title privacy tags quota_estimate")
    _id(op["manager_task_id"])
    video = op["platform_video_id"]
    _require(attempted <= completed <= now and bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", video)) and
             op["privacy"] in {"private", "unlisted"} and receipt["privacy"] == op["privacy"] and
             receipt["title"] == op["title"] and receipt["video_id"] == video and
             receipt["url"] == f"https://www.youtube.com/watch?v={video}", "owned_inventory_receipt")
    job = h["job"]
    if job.get("status") == "RUNNING" and reserved:
        return "owned_inventory_outstanding", False, None
    _require(job.get("status") == "SUCCEEDED" and job["id"] == op["job_id"] and job.get("error_message") is None and
             completed <= _time(job["completed_at"]) <= now, "owned_inventory_job_receipt")
    uploads = []
    for node in h["nodes"]:
        _require(node["status"] == "SUCCEEDED" and node["job_id"] == job["id"] and node.get("error_message") is None,
                 "owned_inventory_node_outcome")
        if node["node_type"] == "youtube_upload":
            uploads.append(node)
            _require(node["id"] == op["node_execution_id"] and node["input_artifact_ids"] == [op["input_artifact_id"]] and
                     completed <= _time(node["completed_at"]) <= _time(job["completed_at"]), "owned_inventory_node_receipt")
            _one([a for a in h["artifacts"] if a["id"] == node.get("output_artifact_id") and
                  a["node_execution_id"] == node["id"] and a["job_id"] == job["id"] and
                  a.get("media_info", {}).get("youtube") == receipt], "owned_inventory_output_receipt")
    _require(len(uploads) == 1, "owned_inventory_upload_node_count")
    if not h["publications"] and reserved:
        return "owned_inventory_outstanding", False, None
    pub = _one(h["publications"], "owned_inventory_publication_count")
    _require(pub["production_task_id"] == task["id"] and pub["account_id"] == task["target_account_id"] and
             pub["platform"] == "youtube" and pub["platform_content_id"] == video and pub["desired_privacy"] == "unlisted" and
             pub.get("public_at") is None, "owned_inventory_publication_identity")
    if pub["current_privacy"] == "private" and reserved:
        return "owned_inventory_outstanding", False, None
    _require(pub["current_privacy"] == "unlisted" and pub["publish_status"] in {"uploaded", "scheduled"}, "owned_inventory_publication_privacy")
    if pub.get("scheduled_publish_at") is None and reserved and _pending_promotion(h, pub, completed, now):
        return "owned_inventory_outstanding", False, None
    start = _time(pub["scheduled_publish_at"])
    _require(completed <= start <= now, "owned_inventory_publication_time")
    if not _reconciled(h, pub, start):
        return "owned_inventory_outstanding", False, None
    wait = "owned_inventory_metrics_pending" if _metrics_ready(h, pub, start, now) else None
    if now - attempted < timedelta(days=1) or now - completed < timedelta(days=1):
        wait = "owned_inventory_cooldown"
    stable = {"task": _fields(task, "id channel_profile_id target_account_id manual_seed_id job_id"),
              "operations": h["operations"], "job": job, "nodes": h["nodes"], "artifacts": h["artifacts"],
              "publication": _fields(pub, "id production_task_id account_id platform platform_content_id desired_privacy current_privacy public_at uploaded_at scheduled_publish_at"),
              "settled_promotion_replacement": replacement}
    return wait, bool(reserved), stable


def _approved_authority(rows: dict, now: datetime) -> tuple[dict[str, HistoryOnlyBinding], RetiredPreuploadCertificate | None, list[dict]]:
    bindings: dict[str, HistoryOnlyBinding] = {}
    certificate = None
    authority = []
    for row in rows["owned_seed_inventories"]:
        # Revocation/exhaustion does not erase approved historical facts.
        if row.get("approved_at") is None:
            continue
        _require(row.get("state") in {"approved", "held", "exhausted", "expired", "revoked"} and
                 _time(row["approved_at"]) <= now and bool(row.get("approved_by")) and bool(row.get("approval_reference")),
                 "owned_history_authority_invalid")
        decoded = decode_history_manifest(row["manifest_json"])
        data = decoded.document.as_dict()
        _require(history_sha256(decoded.document) == row["manifest_sha256"] and data["inventory_id"] == row["id"] and
                 all(data[k] == row[k] for k in ("platform_channel_id", "target_account_id", "channel_profile_id")),
                 "owned_history_authority_invalid")
        if decoded.legacy_history is None:
            continue
        retired = decoded.legacy_history.retired_unassigned_preupload
        if retired is not None:
            _require(retired.observed_at <= _time(row["approved_at"]), "owned_history_authority_invalid")
            _require(certificate is None or certificate.document == retired.document, "owned_history_authority_conflict")
            certificate = retired
        for b in decoded.legacy_history.bindings:
            _require(b.qualification.observed_at <= _time(row["approved_at"]), "owned_history_authority_invalid")
            existing = bindings.get(b.legacy_account_id)
            _require(existing is None or existing.document == b.document, "owned_history_authority_conflict")
            bindings[b.legacy_account_id] = b
        authority.append({"inventory_id": row["id"], "manifest_sha256": row["manifest_sha256"],
                          "approved_at": row["approved_at"], "approved_by": row["approved_by"],
                          "approval_reference": row["approval_reference"], "legacy_history": data["legacy_history"]})
    accounts = {a["id"]: a for a in rows["publishing_accounts"]}
    tasks = {t["id"]: t for t in rows["production_tasks"]}
    for b in bindings.values():
        account = accounts.get(b.legacy_account_id, {})
        _require(account and account.get("channel_profile_id") == b.legacy_channel_profile_id and
                 (account.get("platform") or "youtube") == "youtube" and
                 account.get("platform_account_id") in {"", b.canonical_platform_channel_id} and
                 account_descriptor_sha256(account) == b.account_descriptor_sha256, "owned_history_binding_changed")
        operations = [o for o in rows["youtube_upload_operations"] if
                      tasks.get(o.get("production_task_id"), {}).get("target_account_id") == b.legacy_account_id]
        _require(tuple(o["id"] for o in operations) == b.qualified_operation_ids, "owned_history_membership_changed")
        _require({t["id"] for t in tasks.values() if t["target_account_id"] == b.legacy_account_id} ==
                 {o["production_task_id"] for o in operations}, "owned_history_membership_changed")
        for op, fact in zip(operations, b.qualification.facts):
            _require(op["status"] == "succeeded" and op["manager_task_id"] == fact.manager_task_id and
                     op["platform_video_id"] == fact.platform_video_id and
                     _time(op["completed_at"]) <= fact.observed_at <= now and history_sha256(op) == fact.operation_sha256 and
                     history_sha256(op["receipt_json"]) == fact.receipt_sha256, "owned_history_qualification_changed")
    _require(certificate is None or certificate.legacy_account_id not in bindings, "owned_history_authority_conflict")
    return bindings, certificate, authority


def _terminal_graph(rows: dict, *, job_id: str, upload_node_id: str, task_id: str, legacy_channel_profile_id: str) -> dict:
    """Close over identities in both directions, including inconsistent/orphan links."""
    graph: dict[str, list[dict]] = {name: [] for name in TERMINAL_TABLES}
    nodes = [n for n in rows["node_executions"] if n["job_id"] == job_id or n["id"] == upload_node_id]
    node_ids = {n["id"] for n in nodes}
    graph["node_executions"] = nodes
    artifact_ids = {a for n in nodes for a in (n.get("input_artifact_ids") or [])} | {n.get("output_artifact_id") for n in nodes}
    graph["artifacts"] = [a for a in rows["artifacts"] if a["job_id"] == job_id or a["id"] in artifact_ids]
    linked_attestations: set[str] = set()
    linked_receipts: set[str] = set()
    linked_dispatches: set[str] = set()
    while True:
        old = (frozenset(linked_attestations), frozenset(linked_receipts), frozenset(linked_dispatches))
        for table in ("worker_task_dispatches", "worker_task_delivery_attestations", "worker_event_emissions", "registered_worker_event_receipts"):
            graph[table] = [r for r in rows[table] if r.get("job_id") == job_id or r.get("node_execution_id") in node_ids or
                r.get("source_task_attestation_id") in linked_attestations or r.get("dispatch_key") in linked_dispatches or
                r.get("origin_receipt_id") in linked_receipts or r["id"] in linked_receipts or
                r.get("payload_json", {}).get("task_dispatch_key") in linked_dispatches]
        linked_dispatches.update(r["dispatch_key"] for r in graph["worker_task_dispatches"])
        linked_attestations.update(r["id"] for r in graph["worker_task_delivery_attestations"])
        linked_attestations.update(r["source_task_attestation_id"] for r in graph["worker_event_emissions"] + graph["registered_worker_event_receipts"])
        linked_receipts.update(r["id"] for r in graph["registered_worker_event_receipts"])
        linked_receipts.update(r["origin_receipt_id"] for r in graph["worker_task_dispatches"] if r.get("origin_receipt_id"))
        if old == (frozenset(linked_attestations), frozenset(linked_receipts), frozenset(linked_dispatches)):
            break
    event_identities = {(r["redis_stream"], r["consumer_group"], r.get("message_id")) for r in
                        graph["worker_event_emissions"] + graph["registered_worker_event_receipts"]}
    graph["registered_worker_event_deliveries"] = [r for r in rows["registered_worker_event_deliveries"] if
        r["source_task_attestation_id"] in linked_attestations or r.get("receipt_id") in linked_receipts or
        (r["redis_stream"], r["consumer_group"], r["message_id"]) in event_identities]
    reg_ids = {r.get("worker_registration_id") for r in nodes + graph["worker_task_delivery_attestations"] +
               graph["worker_event_emissions"] + graph["registered_worker_event_receipts"]} - {None}
    graph["worker_registrations"] = [r for r in rows["worker_registrations"] if r["id"] in reg_ids]
    _require({r["id"] for r in graph["worker_registrations"]} == reg_ids, "owned_history_retired_orphan")
    grant_ids = {r["grant_id"] for r in graph["worker_registrations"]}
    graph["worker_admission_grants"] = [r for r in rows["worker_admission_grants"] if r["id"] in grant_ids]
    _require({r["id"] for r in graph["worker_admission_grants"]} == grant_ids, "owned_history_retired_orphan")
    graph["legacy_worker_event_resolutions"] = [r for r in rows["legacy_worker_event_resolutions"] if
        r["job_id"] == job_id or r["node_execution_id"] in node_ids or
        (r["redis_stream"], r["consumer_group"], r["message_id"]) in event_identities]
    graph["channel_ops_queue_items"] = [r for r in rows["channel_ops_queue_items"] if
        r.get("channel_profile_id") == legacy_channel_profile_id or
        r.get("payload_json", {}).get("production_task_id") == task_id or r.get("payload_json", {}).get("job_id") == job_id]
    source_ids = {r["id"] for r in graph["worker_task_dispatches"] + graph["worker_event_emissions"]}
    for name in ("worker_redis_marker_cleanup_authorizations", "worker_redis_marker_repair_audits"):
        graph[name] = [r for r in rows[name] if r["source_id"] in source_ids]
    for records in graph.values():
        records.sort(key=lambda r: r["id"])
    return graph


def _terminal_projection(graph: dict) -> dict:
    projected = FrozenJSON.from_value(graph).as_dict()
    # The original observations remain retained. Heartbeats/revocation are not new
    # execution claims; fresh terminal node + complete dispatch/receipt checks fence those.
    for row in projected["worker_registrations"]:
        for field in ("heartbeat_at", "lease_expires_at", "status", "revoked_at", "revoke_reason", "superseded_by"):
            row.pop(field, None)
    for row in projected["worker_admission_grants"]:
        for field in ("state", "revoked_at", "revoke_reason", "updated_at"):
            row.pop(field, None)
    return projected


def _redis_terminal(observations: tuple[RedisTerminalObservation, ...], *, kind: str, stream: str, group: str,
                    message: str | None, key: str | None, sha: str, now: datetime) -> None:
    matches = [r for r in observations if r.kind == kind and r.redis_stream == stream and r.consumer_group == group and
               r.message_id == message and r.dispatch_key == key and r.payload_sha256 == sha]
    _require(len(matches) == 1, "owned_history_retired_redis_missing")
    r = matches[0]
    _require(not r.pending_message_ids and r.marker_message_id == (message if kind == "task" else None) and
             0 <= (now - r.observed_at).total_seconds() <= MAX_OBSERVATION_AGE_SECONDS, "owned_history_retired_redis_changed")


def _claim_equal(left: dict, right: dict) -> bool:
    fields = "job_id node_execution_id worker_registration_id worker_lease_epoch worker_id"
    return (_fields(left, fields) == _fields(right, fields) and
            _time(left["worker_started_at"]) == _time(right["worker_started_at"]))


def _receipt_path(dispatch: dict, att: dict, graph: dict, node: dict, cert: RetiredPreuploadCertificate,
                  observations: tuple[RedisTerminalObservation, ...], now: datetime) -> None:
    reason = "owned_history_retired_receipt"
    _require(att["job_id"] == cert.job_id and att["node_execution_id"] == node["id"] and
             att["redis_stream"] == dispatch["redis_stream"] and att["consumer_group"] == dispatch["consumer_group"] and
             att["message_id"] == dispatch["redis_message_id"] and att["payload_sha256"] == dispatch["payload_sha256"] and
             att["dispatch_key"] == dispatch["dispatch_key"] and att["ack_state"] == "acknowledged" and
             _time(att["acknowledged_at"]) == _time(dispatch["acknowledged_at"]), reason)
    emission = _one([e for e in graph["worker_event_emissions"] if e["source_task_attestation_id"] == att["id"]], reason)
    receipt = _one([r for r in graph["registered_worker_event_receipts"] if r["source_task_attestation_id"] == att["id"]], reason)
    deliveries = [d for d in graph["registered_worker_event_deliveries"] if d["source_task_attestation_id"] == att["id"]]
    _require(bool(deliveries) and _claim_equal(att, emission) and _claim_equal(att, receipt) and
             emission["emission_state"] == "resolved" and receipt["application_state"] == "applied" and
             receipt["ack_state"] == receipt["source_task_ack_state"] == "acknowledged", reason)
    if att["ack_event_emission_id"] is None:
        # Native receipt-authorized ACK does not populate the emission link.
        _require(_time(receipt["applied_at"]) <= _time(att["acknowledged_at"]), reason)
    else:
        _require(att["ack_event_emission_id"] == emission["id"], reason)
    parsed = parse_registered_worker_event(redis_stream=receipt["redis_stream"], consumer_group=receipt["consumer_group"],
                                           message_id=receipt["message_id"], payload=receipt["payload_json"])
    facts = parsed.receipt_facts(source_task_attestation_id=uuid.UUID(att["id"]))
    for field, value in facts.items():
        if field == "worker_started_at":
            _require(_time(receipt[field]) == _time(value), reason)
        else:
            _require(receipt[field] == (str(value) if isinstance(value, uuid.UUID) else value), reason)
    _require(parsed.source_task_payload_sha256 == dispatch["payload_sha256"] and
             str(parsed.source_task_dispatch_key) == dispatch["dispatch_key"], reason)
    for field in ("redis_stream", "consumer_group", "message_id", "payload_sha256", "payload_json", "event_type"):
        _require(emission[field] == receipt[field], reason)
    _require(receipt["redis_stream"] == "vp:events" and receipt["consumer_group"] == "orchestrator", reason)
    for field in ("prepared_at", "emitted_at", "resolved_at"):
        _require(_time(emission[field]) <= cert.observed_at, reason)
    _require(_time(emission["prepared_at"]) <= _time(emission["emitted_at"]) <= _time(emission["resolved_at"]) and
             _time(receipt["accepted_at"]) <= _time(receipt["applied_at"]) <= _time(receipt["acknowledged_at"]) <= cert.observed_at and
             _time(receipt["source_task_acknowledged_at"]) == _time(att["acknowledged_at"]) and
             _time(att["worker_started_at"]) <= _time(att["attested_at"]) <= _time(att["acknowledged_at"]), reason)
    for delivery in deliveries:
        _require(delivery["receipt_id"] == receipt["id"] and delivery["resolution_state"] == "accepted" and
                 delivery["reason_code"] is None and delivery["ack_state"] == "acknowledged" and
                 _time(delivery["accepted_at"]) <= _time(delivery["acknowledged_at"]) <= cert.observed_at and
                 delivery["payload_sha256"] == receipt["payload_sha256"], reason)
        _require(delivery["redis_stream"] == receipt["redis_stream"] and delivery["consumer_group"] == receipt["consumer_group"], reason)
        _redis_terminal(observations, kind="event", stream=delivery["redis_stream"], group=delivery["consumer_group"],
                        message=delivery["message_id"], key=None, sha=delivery["payload_sha256"], now=now)
    _require(any(d["message_id"] == receipt["message_id"] for d in deliveries), reason)
    reg = _one([r for r in graph["worker_registrations"] if r["id"] == att["worker_registration_id"]], reason)
    grant = _one([g for g in graph["worker_admission_grants"] if g["id"] == reg["grant_id"]], reason)
    _require(reg["redis_consumer_id"] == att["worker_id"] and 0 < _int(att["worker_lease_epoch"]) <= _int(reg["lease_epoch"]) and
             _time(grant["activated_at"]) <= _time(reg["registered_at"]) <= _time(att["worker_started_at"]) and
             _fields(reg, "service_name worker_type worker_host capabilities_json image_identity database_principal") ==
             _fields(grant, "service_name worker_type worker_host capabilities_json image_identity database_principal") and
             grant["redis_stream"] == dispatch["redis_stream"] and grant["redis_group"] == dispatch["consumer_group"], reason)
    for fingerprint in ("database_fingerprint", "redis_fingerprint", "storage_fingerprint"):
        _hash(reg[fingerprint])
    _require(node["worker_registration_id"] == att["worker_registration_id"] and node["worker_lease_epoch"] == att["worker_lease_epoch"] and
             _time(node["started_at"]) == _time(att["worker_started_at"]), reason)
    if receipt["event_type"] == "node_completed":
        _require(node["status"] == "SUCCEEDED" and node["worker_id"] == att["worker_id"] and
                 receipt["payload_json"].get("output_artifact_id") == node["output_artifact_id"], reason)
    else:
        _require(receipt["event_type"] == "node_failed" and node["id"] == cert.upload_node_id and
                 node["status"] == "CANCELLED" and node["worker_id"] is None, reason)


def _assess_retired(rows: dict, cert: RetiredPreuploadCertificate, snapshot: OwnedHistorySnapshot,
                    now: datetime) -> tuple[tuple[str, ...], tuple[str, ...], tuple[TerminalPath, ...]]:
    reason = "owned_history_retired_changed"
    _require(cert.observed_at <= snapshot.observed_at <= now, reason)
    retained = cert.retained_facts
    mappings = (("operation", "youtube_upload_operations", cert.operation_id), ("task", "production_tasks", cert.task_id),
                ("job", "jobs", cert.job_id), ("upload_node", "node_executions", cert.upload_node_id),
                ("account", "publishing_accounts", cert.legacy_account_id), ("channel", "channel_profiles", cert.legacy_channel_profile_id),
                ("manual_seed", "manual_seeds", retained.task.as_dict()["manual_seed_id"]))
    current = {}
    for key, table, row_id in mappings:
        row = _one([r for r in rows[table] if r["id"] == row_id], "owned_history_retired_orphan")
        _complete_row(row, table)
        _require(FrozenJSON.from_value(row) == getattr(retained, key), reason)
        current[key] = row
    op, task, job, upload, account, channel, seed = (current[k] for k in ("operation", "task", "job", "upload_node", "account", "channel", "manual_seed"))
    _require(op["production_task_id"] == task["id"] and op["job_id"] == task["job_id"] == job["id"] and
             op["node_execution_id"] == upload["id"] and task["target_account_id"] == account["id"] and
             task["channel_profile_id"] == account["channel_profile_id"] == channel["id"] and
             (account["platform"] or "youtube") == "youtube" and account["platform_account_id"] == "", reason)
    _require(op["status"] == "reserved" and op["privacy"] == "unlisted" and op["receipt_json"] == {} and
             all(op[k] is None for k in ("request_attempted_at", "manager_task_id", "platform_video_id", "completed_at", "error_message")), reason)
    _hash(op["content_sha256"])
    _require(task["state"] == "held" and task["blocked_by_guard"] == task["failure_reason"] == "operator_canary_failure" and
             job["status"] == upload["status"] == "CANCELLED" and job["error_message"] == upload["error_message"] == "operator_canary_failure" and
             channel["halt_reason"] == "operator_canary_failure" and channel["intake_paused_at"] is not None and upload["worker_id"] is None, reason)
    transition = task["transition_history_json"][-1]
    _exact(transition, "from to actor at")
    cancelled = _time(job["completed_at"])
    _require(transition["from"] == "producing" and transition["to"] == "held" and transition["actor"] == "operator_canary_failure" and
             _time(transition["at"]) == _time(task["state_updated_at"]) == cancelled == _time(upload["completed_at"]) and
             _time(channel["intake_paused_at"]) <= _time(channel["halted_at"]) <= cancelled <= cert.observed_at, reason)
    _require(not any(p["production_task_id"] == task["id"] for p in rows["publication_records"]) and
             not any(p["production_task_id"] == task["id"] for p in rows["publication_promotion_operations"]) and
             not any(j["parent_job_id"] == job["id"] for j in rows["jobs"] if j["id"] != job["id"]) and
             not any(t["id"] != task["id"] and (t["target_account_id"] == account["id"] or t["channel_profile_id"] == channel["id"] or
                     t.get("job_id") == job["id"]) for t in rows["production_tasks"]) and
             not any(s.get("guarded_job_id") == job["id"] for s in rows["runtime_schedules"]), reason)
    graph = _terminal_graph(rows, job_id=cert.job_id, upload_node_id=cert.upload_node_id,
                            task_id=cert.task_id, legacy_channel_profile_id=cert.legacy_channel_profile_id)
    TerminalGraph.parse(graph)
    _require(_terminal_projection(graph) == _terminal_projection(cert.terminal_graph.as_dict()), reason)
    _require(not graph["legacy_worker_event_resolutions"], "owned_history_retired_unsupported_legacy_resolution")
    _require(not graph["worker_redis_marker_cleanup_authorizations"] and not graph["worker_redis_marker_repair_audits"],
             "owned_history_retired_marker_maintenance")
    for q in graph["channel_ops_queue_items"]:
        _require(q["channel_profile_id"] == channel["id"] and q["locked_at"] is None and q["locked_by"] is None and
                 (q["status"] == "succeeded" and _queue_clean(q) and q["attempt_count"] == 1 or
                  q["status"] == "dead_lettered" and q["last_error"] == "operator_canary_failure" and
                  _time(q["dead_letter_at"]) == cancelled and q["attempt_count"] in {0, 1}), reason)
        _require(q["parent_queue_item_id"] is None or any(p["id"] == q["parent_queue_item_id"] for p in graph["channel_ops_queue_items"]), reason)
    nodes = {n["id"]: n for n in graph["node_executions"]}
    definition = PipelineDefinition.model_validate(job["pipeline_snapshot"])
    _require(len(nodes) == len(definition.nodes) and {n["node_id"] for n in nodes.values()} == {n.id for n in definition.nodes}, reason)
    by_name = {n["node_id"]: n for n in nodes.values()}
    _require(len({e.id for e in definition.edges}) == len(definition.edges) and
             all(e.source in by_name and e.target in by_name for e in definition.edges) and
             len(topological_sort(definition)) == len(nodes), reason)
    source_ids, paths = set(), []
    artifacts = {a["id"]: a for a in graph["artifacts"]}
    for node in nodes.values():
        specified = _one([n.model_dump() for n in definition.nodes if n.id == node["node_id"]], reason)
        config = {**specified["data"]["config"]}
        if specified["data"].get("asset_id"):
            config["asset_id"] = specified["data"]["asset_id"]
        _require(node["job_id"] == job["id"] and node["node_type"] == specified["type"] and node["node_config"] == config and
                 node["status"] in {"SUCCEEDED", "CANCELLED"} and _time(node["completed_at"]) <= cancelled, reason)
        if node["status"] == "CANCELLED":
            _require(node["worker_id"] is None and node["error_message"] == "operator_canary_failure" and
                     _time(node["completed_at"]) == cancelled and node["output_artifact_id"] is None, reason)
        else:
            _require(node["error_message"] is None and node["output_artifact_id"] in artifacts and
                     artifacts[node["output_artifact_id"]]["node_execution_id"] == node["id"], reason)
        related = [d for d in graph["worker_task_dispatches"] if d["node_execution_id"] == node["id"]]
        upstream = {e.targetHandle: by_name[e.source]["output_artifact_id"] for e in definition.edges if e.target == node["node_id"]}
        _require(len(upstream) == sum(e.target == node["node_id"] for e in definition.edges) and
                 sorted(upstream.values()) == sorted(node["input_artifact_ids"]) and
                 all(by_name[e.source]["status"] == "SUCCEEDED" for e in definition.edges if e.target == node["node_id"]), reason)
        if node["node_type"] == "source":
            _require(node["status"] == "SUCCEEDED" and not related and node["input_artifact_ids"] == [] and
                     all(node[k] is None for k in ("worker_registration_id", "worker_lease_epoch", "worker_id")) and
                     _time(node["started_at"]) <= _time(node["completed_at"]), reason)
            asset_id = node["node_config"]["asset_id"]
            source_ids.add(asset_id)
            source = _one([s.asset.as_dict() for s in retained.source_assets if s.asset.as_dict()["id"] == asset_id], reason)
            fresh = _one([a for a in rows["assets"] if a["id"] == asset_id], reason)
            artifact = artifacts[node["output_artifact_id"]]
            _require(fresh == source and all(artifact[k] == source[k] for k in ("filename", "mime_type", "file_size", "storage_backend", "storage_path")) and
                     artifact["media_info"].get("source_asset_id") == asset_id and artifact["media_info"].get("asset_id") == asset_id, reason)
            paths.append(TerminalPath(node["id"], "synchronous_source"))
        else:
            _require(bool(related), reason)
    _require(source_ids == {s.asset.as_dict()["id"] for s in retained.source_assets} and bool(source_ids) and
             seed["channel_profile_id"] == channel["id"] and seed["target_account_id"] == account["id"], reason)
    constraints = seed["constraints_json"]
    seed_assets = set(constraints.get("input_asset_ids") or [])
    if constraints.get("input_asset_id"):
        seed_assets.add(constraints["input_asset_id"])
    _require(seed_assets == source_ids and op["input_artifact_id"] in artifacts and upload["input_artifact_ids"] == [op["input_artifact_id"]], reason)
    _require(all(a["job_id"] == job["id"] and a["node_execution_id"] in nodes for a in artifacts.values()), reason)
    dispatch_keys = {d["dispatch_key"] for d in graph["worker_task_dispatches"]}
    _require(len(dispatch_keys) == len(graph["worker_task_dispatches"]), reason)
    origins = [(d["origin_receipt_id"], d["node_execution_id"]) for d in graph["worker_task_dispatches"] if d["origin_receipt_id"] is not None]
    _require(len(origins) == len(set(origins)), reason)
    _require(all(a["dispatch_key"] in dispatch_keys for a in graph["worker_task_delivery_attestations"]), reason)
    att_ids = {a["id"] for a in graph["worker_task_delivery_attestations"]}
    _require(all(e["source_task_attestation_id"] in att_ids for e in graph["worker_event_emissions"] + graph["registered_worker_event_receipts"] + graph["registered_worker_event_deliveries"]), reason)
    expected_redis = set()
    for dispatch in graph["worker_task_dispatches"]:
        node = nodes[dispatch["node_execution_id"]]
        payload = dispatch["payload_json"]
        node_type = NodeTypeRegistry.get().get_type(node["node_type"])
        _require(node_type is not None and dispatch["job_id"] == job["id"] and
                 dispatch["redis_stream"] == f"vp:tasks:{node_type.worker_type}" and
                 dispatch["consumer_group"] == f"{node_type.worker_type}-workers" and
                 payload["job_id"] == job["id"] and payload["node_execution_id"] == node["id"] and
                 payload["node_id"] == node["node_id"] and payload["node_type"] == node["node_type"] and
                 payload["dispatch_key"] == dispatch["dispatch_key"] and
                 canonical_redis_payload_sha256(payload) == dispatch["payload_sha256"] and
                 FrozenJSON.from_json(payload["config"]).as_dict() == node["node_config"], reason)
        inputs = FrozenJSON.from_json(payload["input_artifacts"]).as_dict()
        expected_inputs = {e.targetHandle: by_name[e.source]["output_artifact_id"] for e in definition.edges if e.target == node["node_id"]}
        _require(inputs == expected_inputs and sorted(inputs.values()) == sorted(node["input_artifact_ids"]) and
                 all(value in artifacts for value in inputs.values()), reason)
        atts = [a for a in graph["worker_task_delivery_attestations"] if a["dispatch_key"] == dispatch["dispatch_key"]]
        _require(dispatch["delivery_error"] is None and _time(dispatch["created_at"]) <= cert.observed_at, reason)
        if dispatch["resolution_state"] == "cancelled":
            _require(not atts and node["status"] == "CANCELLED" and dispatch["delivery_state"] in {"pending", "cancelled"} and
                     all(dispatch[k] is None for k in ("delivery_attempted_at", "redis_message_id", "delivered_at", "acknowledged_at")) and
                     cancelled <= _time(dispatch["cancelled_at"]) <= cert.observed_at and
                     all(node[k] is None for k in ("worker_registration_id", "worker_lease_epoch", "worker_id", "started_at")), reason)
            paths.append(TerminalPath(dispatch["id"], "never_delivered_cancelled"))
        else:
            _require(dispatch["delivery_state"] == "delivered" and dispatch["resolution_state"] == "acknowledged" and
                     dispatch["cancelled_at"] is None and bool(_REDIS_ID.fullmatch(dispatch["redis_message_id"])) and
                     _time(dispatch["delivery_attempted_at"]) <= _time(dispatch["delivered_at"]) <=
                     _time(dispatch["acknowledged_at"]) <= cert.observed_at, reason)
            if atts:
                _receipt_path(dispatch, _one(atts, reason), graph, node, cert, snapshot.redis_observations, now)
                paths.append(TerminalPath(dispatch["id"], "receipt_backed"))
            else:
                # Native vp_require/acknowledge_cancelled_worker_task permits this
                # exact delivered retry, without fabricating a second attestation.
                origin = _one([r for r in graph["registered_worker_event_receipts"] if r["id"] == dispatch["origin_receipt_id"]], reason)
                _require(node["id"] == cert.upload_node_id and node["status"] == "CANCELLED" and
                         origin["event_type"] == "node_failed" and origin["node_execution_id"] == node["id"] and
                         origin["application_state"] == "applied" and origin["ack_state"] == "acknowledged" and
                         _time(dispatch["acknowledged_at"]) >= cancelled and
                         not any(e["payload_json"].get("task_dispatch_key") == dispatch["dispatch_key"] for e in graph["worker_event_emissions"]), reason)
                original_att = _one([a for a in graph["worker_task_delivery_attestations"] if a["id"] == origin["source_task_attestation_id"]], reason)
                _require(original_att["dispatch_key"] != dispatch["dispatch_key"] and
                         original_att["worker_registration_id"] == node["worker_registration_id"] and
                         original_att["worker_lease_epoch"] == node["worker_lease_epoch"] and
                         _time(original_att["worker_started_at"]) == _time(node["started_at"]), reason)
                paths.append(TerminalPath(dispatch["id"], "delivered_cancelled_ack"))
        _redis_terminal(snapshot.redis_observations, kind="task", stream=dispatch["redis_stream"], group=dispatch["consumer_group"],
                        message=dispatch["redis_message_id"], key=dispatch["dispatch_key"], sha=dispatch["payload_sha256"], now=now)
        expected_redis.add(("task", dispatch["redis_stream"], dispatch["consumer_group"], dispatch["redis_message_id"], dispatch["dispatch_key"]))
    for delivery in graph["registered_worker_event_deliveries"]:
        expected_redis.add(("event", delivery["redis_stream"], delivery["consumer_group"], delivery["message_id"], None))
    _require(expected_redis == {(r.kind, r.redis_stream, r.consumer_group, r.message_id, r.dispatch_key) for r in snapshot.redis_observations} and
             len(expected_redis) == len(snapshot.redis_observations), reason)
    return tuple(sorted({s.content_sha256 for s in retained.source_assets})), (op["content_sha256"],), tuple(paths)


def assess_owned_history(snapshot: OwnedHistorySnapshot, *, now: datetime) -> OwnedHistoryAssessment:
    try:
        age = (_time(now) - snapshot.observed_at).total_seconds()
        _require(0 <= age <= MAX_OBSERVATION_AGE_SECONDS, "owned_history_observation_stale")
        rows = snapshot.rows.as_dict()
        tasks = {r["id"]: r for r in rows["production_tasks"]}
        accounts = {r["id"]: r for r in rows["publishing_accounts"]}
        channels = {r["id"]: r for r in rows["channel_profiles"]}
        bindings, certificate, authority = _approved_authority(rows, _time(now))
        source_hashes: tuple[str, ...] = ()
        render_hashes: tuple[str, ...] = ()
        terminal_paths: tuple[TerminalPath, ...] = ()
        if certificate is not None:
            source_hashes, render_hashes, terminal_paths = _assess_retired(rows, certificate, snapshot, _time(now))
        classifications = []
        for op in rows["youtube_upload_operations"]:
            task = tasks.get(op.get("production_task_id"), {})
            account = accounts.get(task.get("target_account_id"), {})
            channel = channels.get(task.get("channel_profile_id"), {})
            _require(bool(task and account and channel) and account.get("channel_profile_id") == channel["id"],
                     "owned_history_orphan")
            identity = account.get("platform_account_id")
            if certificate is not None and op["id"] == certificate.operation_id:
                classifications.append(OperationClassification(op["id"], "retired_unassigned_preupload", None, account["id"]))
                continue
            if account["id"] in bindings:
                binding = bindings[account["id"]]
                classifications.append(OperationClassification(op["id"], "history_only", binding.canonical_platform_channel_id, account["id"]))
                continue
            _require((account.get("platform") or "youtube") == "youtube" and
                     isinstance(identity, str) and bool(_UC.fullmatch(identity)), "owned_history_unclassified")
            classifications.append(OperationClassification(op["id"], "direct", identity, account["id"]))
        members = tuple(sorted({a["id"] for a in accounts.values() if
                                (a.get("platform") or "youtube") == "youtube" and a.get("platform_account_id") == snapshot.platform_channel_id} |
                               {b.legacy_account_id for b in bindings.values() if b.canonical_platform_channel_id == snapshot.platform_channel_id}))
        items = {i["production_task_id"]: i for i in rows["owned_seed_inventory_items"] if
                 i.get("production_task_id") is not None and i.get("platform_channel_id") == snapshot.platform_channel_id}
        _require(all(i in tasks for i in items), "owned_inventory_missing_task")
        _require(len(items) == sum(i.get("production_task_id") is not None and i.get("platform_channel_id") == snapshot.platform_channel_id
                                  for i in rows["owned_seed_inventory_items"]), "owned_history_item_authority")
        for task_id, item in items.items():
            inv = _one([r for r in rows["owned_seed_inventories"] if r["id"] == item["inventory_id"] and r.get("approved_at") is not None],
                       "owned_history_item_authority")
            entry = _one([e for e in inv["manifest_json"]["entries"] if e["id"] == item["id"]], "owned_history_item_authority")
            _require(all(entry[k] == item[k] for k in ("manual_seed_id", "asset_id", "content_sha256")) and
                     inv["platform_channel_id"] == snapshot.platform_channel_id and
                     inv["channel_profile_id"] == tasks[task_id]["channel_profile_id"] and
                     inv["target_account_id"] == tasks[task_id]["target_account_id"], "owned_history_item_authority")
        stable, completed_items, wait_reason = [], [], None
        for task in tasks.values():
            if task.get("target_account_id") not in members and task["id"] not in items:
                continue
            h = _task_history(rows, task)
            if task.get("state") in {"held", "failed", "rejected"} and not h["operations"] and task["id"] not in items:
                continue
            item = items.get(task["id"])
            if item:
                _require(task.get("manual_seed_id") == item.get("manual_seed_id"), "owned_inventory_history_identity")
            wait, complete, effect = _normal_history(h, item, _time(now))
            wait_reason = wait or wait_reason
            if complete:
                _require(item is not None)
                completed_items.append(cast(dict, item)["id"])
            if effect is not None:
                stable.append(effect)
        return OwnedHistoryAssessment(None, tuple(classifications), members, authority_sha256=history_sha256(authority),
                                      stable_history_sha256=history_sha256(stable), wait_reason=wait_reason,
                                      completed_item_ids=tuple(completed_items), retired_source_sha256=source_hashes,
                                      retired_render_sha256=render_hashes, terminal_paths=terminal_paths)
    except OwnedHistoryError as error:
        return OwnedHistoryAssessment(str(error))
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, IndexError, RegisteredWorkerEventError):
        return OwnedHistoryAssessment("owned_history_invalid")
