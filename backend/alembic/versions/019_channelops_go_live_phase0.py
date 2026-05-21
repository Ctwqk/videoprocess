"""channelops go live phase0

Revision ID: 019_channelops_go_live_phase0
Revises: 018_go_orchestrator_owner
Create Date: 2026-05-21 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "019_channelops_go_live_phase0"
down_revision = "018_go_orchestrator_owner"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "feedback_snapshots",
        sa.Column("metrics_completeness_score", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column(
        "feedback_snapshots",
        sa.Column("available_fields_json", sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")),
    )
    op.create_index(
        "ix_takedown_events_publication_event_detected",
        "takedown_events",
        ["publication_id", "event_type", "detected_at"],
    )
    op.alter_column("feedback_snapshots", "metrics_completeness_score", server_default=None)
    op.alter_column("feedback_snapshots", "available_fields_json", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_takedown_events_publication_event_detected", table_name="takedown_events")
    op.drop_column("feedback_snapshots", "available_fields_json")
    op.drop_column("feedback_snapshots", "metrics_completeness_score")
