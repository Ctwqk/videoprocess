from __future__ import annotations

from sqlalchemy import CheckConstraint, UniqueConstraint

from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventReceipt,
    WorkerEventDispatch,
)


def test_registered_event_receipt_schema_binds_event_claim_task_and_states() -> None:
    table = RegisteredWorkerEventReceipt.__table__

    assert {
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "payload_json",
        "event_type",
        "job_id",
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
        "source_task_stream",
        "source_task_group",
        "source_task_message_id",
        "application_state",
        "ack_state",
        "accepted_at",
        "applied_at",
        "acknowledged_at",
    } <= set(table.columns.keys())

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("redis_stream", "consumer_group", "message_id") in unique_columns

    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "length(payload_sha256) = 64" in checks[
        "ck_registered_worker_event_receipt_sha256"
    ]
    assert "application_state IN ('accepted', 'applied')" in checks[
        "ck_registered_worker_event_receipt_application_state"
    ]
    assert "ack_state IN ('pending', 'acknowledged')" in checks[
        "ck_registered_worker_event_receipt_ack_state"
    ]


def test_worker_event_dispatch_schema_is_receipt_owned_and_idempotent() -> None:
    table = WorkerEventDispatch.__table__

    assert {
        "receipt_id",
        "dispatch_key",
        "node_execution_id",
        "redis_stream",
        "payload_sha256",
        "payload_json",
        "delivery_state",
        "redis_message_id",
        "created_at",
        "delivered_at",
    } <= set(table.columns.keys())

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("dispatch_key",) in unique_columns
    assert ("receipt_id", "node_execution_id") in unique_columns
