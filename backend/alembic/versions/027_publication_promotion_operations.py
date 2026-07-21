"""add durable publication promotion operations

Revision ID: 027_publication_promotion_operations
Revises: 026_autoflow_authority_fence
Create Date: 2026-07-21 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "027_publication_promotion_operations"
down_revision: Union[str, None] = "026_autoflow_authority_fence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "publication_promotion_operations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("publication_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("production_task_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("queue_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("platform_video_id", sa.String(length=255), nullable=False),
        sa.Column("target_privacy", sa.String(length=32), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempt_key", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "decision_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column("observed_privacy", sa.String(length=32), nullable=True),
        sa.Column("observed_publish_status", sa.String(length=64), nullable=True),
        sa.Column(
            "evidence_json",
            postgresql.JSON(astext_type=sa.Text()),
            server_default=sa.text("'{}'::json"),
            nullable=False,
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("request_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["production_task_id"],
            ["production_tasks.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["publication_id"],
            ["publication_records.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "publication_id",
            name="uq_publication_promotion_operations_publication",
        ),
        sa.UniqueConstraint(
            "queue_item_id",
            name="uq_publication_promotion_operations_queue_item",
        ),
        sa.UniqueConstraint(
            "attempt_key",
            name="uq_publication_promotion_operations_attempt_key",
        ),
        sa.CheckConstraint(
            "target_privacy IN ('private', 'unlisted')",
            name="ck_publication_promotion_operations_target_privacy",
        ),
        sa.CheckConstraint(
            "status IN ('reserved', 'submitting', 'confirmed', 'finalized', 'uncertain')",
            name="ck_publication_promotion_operations_status",
        ),
    )


def downgrade() -> None:
    op.drop_table("publication_promotion_operations")
