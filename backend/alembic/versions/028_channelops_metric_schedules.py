"""add durable publication metric schedules

Revision ID: 028_channelops_metric_schedules
Revises: 027_publication_promotion_operations
Create Date: 2026-07-21 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "028_channelops_metric_schedules"
down_revision: Union[str, None] = "027_publication_promotion_operations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "publication_metric_schedules",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "publication_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("snapshot_stage", sa.String(length=16), nullable=False),
        sa.Column("effective_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("grace_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default="pending",
            nullable=False,
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "available_fields_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'[]'::json"),
            nullable=False,
        ),
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "snapshot_stage IN ('1h','6h','24h','72h','7d')",
            name="ck_metric_schedule_stage",
        ),
        sa.CheckConstraint(
            "status IN ('pending','succeeded','expired')",
            name="ck_metric_schedule_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_metric_schedule_attempt_count",
        ),
        sa.CheckConstraint(
            "due_at >= effective_start_at",
            name="ck_metric_schedule_due_order",
        ),
        sa.CheckConstraint(
            "grace_until >= due_at",
            name="ck_metric_schedule_grace_order",
        ),
        sa.ForeignKeyConstraint(
            ["publication_id"],
            ["publication_records.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "publication_id",
            "snapshot_stage",
            name="uq_metric_schedule_publication_stage",
        ),
    )
    op.create_index(
        "ix_metric_schedules_status_due",
        "publication_metric_schedules",
        ["status", "due_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_metric_schedules_status_due",
        table_name="publication_metric_schedules",
    )
    op.drop_table("publication_metric_schedules")
