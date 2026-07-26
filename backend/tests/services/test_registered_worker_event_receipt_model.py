from __future__ import annotations

from sqlalchemy import CheckConstraint, Index, UniqueConstraint

from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerTaskDispatch,
    WorkerTaskDeliveryAttestation,
)


def test_worker_task_delivery_attestation_binds_dispatch_claim_and_payload() -> None:
    table = WorkerTaskDeliveryAttestation.__table__

    assert {
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "dispatch_key",
        "job_id",
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
        "ack_state",
        "acknowledged_at",
        "attested_at",
    } <= set(table.columns.keys())

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("redis_stream", "consumer_group", "message_id") in unique_columns
    assert (
        "node_execution_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "worker_id",
        "worker_started_at",
    ) in unique_columns
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "('pending', 'authorized', 'acknowledged')" in checks[
        "ck_worker_task_delivery_attestation_ack_state"
    ]


def test_registered_event_receipt_schema_binds_attestation_claim_and_states() -> None:
    table = RegisteredWorkerEventReceipt.__table__

    assert {
        "source_task_attestation_id",
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


def test_registered_event_delivery_schema_aliases_each_event_identity() -> None:
    table = RegisteredWorkerEventDelivery.__table__

    assert {
        "source_task_attestation_id",
        "receipt_id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "resolution_state",
        "reason_code",
        "ack_state",
        "accepted_at",
        "acknowledged_at",
    } <= set(table.columns.keys())

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("redis_stream", "consumer_group", "message_id") in unique_columns


def test_worker_task_dispatch_schema_covers_initial_and_receipt_tasks() -> None:
    table = WorkerTaskDispatch.__table__

    assert {
        "origin_receipt_id",
        "dispatch_key",
        "job_id",
        "node_execution_id",
        "redis_stream",
        "consumer_group",
        "payload_sha256",
        "payload_json",
        "delivery_state",
        "delivery_attempted_at",
        "delivery_error",
        "redis_message_id",
        "resolution_state",
        "acknowledged_at",
        "cancelled_at",
        "created_at",
        "delivered_at",
    } <= set(table.columns.keys())

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("dispatch_key",) in unique_columns
    assert ("origin_receipt_id", "node_execution_id") in unique_columns
    unresolved_initial_indexes = [
        index
        for index in table.indexes
        if isinstance(index, Index)
        and index.name == "uq_worker_task_dispatch_unresolved_initial_node"
    ]
    assert len(unresolved_initial_indexes) == 1
    assert unresolved_initial_indexes[0].unique is True
    assert (
        "origin_receipt_id IS NULL"
        in str(
            unresolved_initial_indexes[0].dialect_options["postgresql"][
                "where"
            ]
        )
    )
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "('pending', 'attempting', 'delivered', 'uncertain', 'cancelled')" in (
        checks["ck_worker_task_dispatch_state"]
    )
    assert "('unresolved', 'cancel_authorized', 'acknowledged', 'cancelled')" in (
        checks["ck_worker_task_dispatch_resolution_state"]
    )
