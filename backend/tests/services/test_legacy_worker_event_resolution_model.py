from __future__ import annotations

from pathlib import Path

from sqlalchemy import CheckConstraint, UniqueConstraint

from app.models.legacy_worker_event_resolution import LegacyWorkerEventResolution


def test_legacy_worker_event_resolution_model_has_fail_closed_constraints():
    table = LegacyWorkerEventResolution.__table__

    assert table.name == "legacy_worker_event_resolutions"
    assert {
        "id",
        "redis_stream",
        "consumer_group",
        "message_id",
        "payload_sha256",
        "payload_json",
        "event_type",
        "job_id",
        "node_execution_id",
        "resolution_reason",
        "operator_id",
        "observed_job_status",
        "observed_node_status",
        "observed_task_state",
        "observed_channel_halted_at",
        "recorded_at",
        "acknowledged_at",
    } == set(table.columns.keys())

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("redis_stream", "consumer_group", "message_id") in unique_columns

    checks = {
        str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "length(payload_sha256) = 64" in checks
    assert "event_type = 'node_failed'" in checks
    assert "resolution_reason = 'terminal_cancelled'" in checks
    assert "observed_job_status = 'CANCELLED'" in checks
    assert "observed_node_status = 'CANCELLED'" in checks
    assert "observed_task_state = 'held'" in checks


def test_migration_033_creates_and_drops_legacy_resolution_table():
    migration = (
        Path(__file__).resolve().parents[2]
        / "alembic/versions/033_legacy_worker_event_resolutions.py"
    ).read_text()

    assert 'revision: str = "033_legacy_worker_event_resolutions"' in migration
    assert 'down_revision: Union[str, None] = "032_channelops_leader_epoch"' in migration
    assert "op.create_table(" in migration
    assert '"legacy_worker_event_resolutions"' in migration
    assert 'op.drop_table("legacy_worker_event_resolutions")' in migration
