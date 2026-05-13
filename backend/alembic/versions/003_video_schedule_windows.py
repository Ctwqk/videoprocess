"""video schedule windows

Revision ID: 003
Revises: 002
Create Date: 2026-04-11
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'WAITING_WINDOW'")

    op.create_table(
        "runtime_schedules",
        sa.Column("service_name", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_by", sa.String(length=255), server_default="system", nullable=False),
        sa.PrimaryKeyConstraint("service_name"),
    )

    op.execute(
        """
        INSERT INTO runtime_schedules (service_name, state, updated_by)
        VALUES ('videoprocess', 'OPEN', 'migration')
        ON CONFLICT (service_name) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("runtime_schedules")
