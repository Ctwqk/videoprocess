"""archive terminal legacy worker events

Revision ID: 033_legacy_worker_event_resolutions
Revises: 032_channelops_leader_epoch
Create Date: 2026-07-25 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "033_legacy_worker_event_resolutions"
down_revision: Union[str, None] = "032_channelops_leader_epoch"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "legacy_worker_event_resolutions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("redis_stream", sa.String(length=255), nullable=False),
        sa.Column("consumer_group", sa.String(length=255), nullable=False),
        sa.Column("message_id", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "node_execution_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("resolution_reason", sa.String(length=64), nullable=False),
        sa.Column("operator_id", sa.String(length=255), nullable=False),
        sa.Column("observed_job_status", sa.String(length=32), nullable=False),
        sa.Column("observed_node_status", sa.String(length=32), nullable=False),
        sa.Column("observed_task_state", sa.String(length=32), nullable=False),
        sa.Column(
            "observed_channel_halted_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "length(btrim(redis_stream)) > 0",
            name="ck_legacy_worker_event_resolution_stream_nonempty",
        ),
        sa.CheckConstraint(
            "length(btrim(consumer_group)) > 0",
            name="ck_legacy_worker_event_resolution_group_nonempty",
        ),
        sa.CheckConstraint(
            "length(btrim(message_id)) > 0",
            name="ck_legacy_worker_event_resolution_message_nonempty",
        ),
        sa.CheckConstraint(
            "length(payload_sha256) = 64",
            name="ck_legacy_worker_event_resolution_sha256_length",
        ),
        sa.CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_legacy_worker_event_resolution_sha256_format",
        ),
        sa.CheckConstraint(
            "event_type = 'node_failed'",
            name="ck_legacy_worker_event_resolution_event_type",
        ),
        sa.CheckConstraint(
            "resolution_reason = 'terminal_cancelled'",
            name="ck_legacy_worker_event_resolution_reason",
        ),
        sa.CheckConstraint(
            "length(btrim(operator_id)) > 0",
            name="ck_legacy_worker_event_resolution_operator_nonempty",
        ),
        sa.CheckConstraint(
            "observed_job_status = 'CANCELLED'",
            name="ck_legacy_worker_event_resolution_job_status",
        ),
        sa.CheckConstraint(
            "observed_node_status = 'CANCELLED'",
            name="ck_legacy_worker_event_resolution_node_status",
        ),
        sa.CheckConstraint(
            "observed_task_state = 'held'",
            name="ck_legacy_worker_event_resolution_task_state",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_legacy_worker_event_resolution_identity",
        ),
    )


def downgrade() -> None:
    op.drop_table("legacy_worker_event_resolutions")
