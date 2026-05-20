"""add go orchestrator owner

Revision ID: 018_go_orchestrator_owner
Revises: 017_channel_ops_self_driving
Create Date: 2026-05-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "018_go_orchestrator_owner"
down_revision = "017_channel_ops_self_driving"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "orchestrator_owner",
            sa.String(length=32),
            nullable=False,
            server_default="python",
        ),
    )
    op.create_check_constraint(
        "ck_jobs_orchestrator_owner",
        "jobs",
        "orchestrator_owner IN ('python', 'go')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_jobs_orchestrator_owner", "jobs", type_="check")
    op.drop_column("jobs", "orchestrator_owner")
