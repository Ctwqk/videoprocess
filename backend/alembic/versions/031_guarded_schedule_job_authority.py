"""persist guarded schedule job authority

Revision ID: 031_guarded_schedule_job_authority
Revises: 030_channelops_intake_pause
Create Date: 2026-07-22 00:00:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "031_guarded_schedule_job_authority"
down_revision: Union[str, None] = "030_channelops_intake_pause"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "runtime_schedules",
        sa.Column("guarded_job_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_runtime_schedules_guarded_job_id_jobs",
        "runtime_schedules",
        "jobs",
        ["guarded_job_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_runtime_schedules_guarded_job_id_jobs",
        "runtime_schedules",
        type_="foreignkey",
    )
    op.drop_column("runtime_schedules", "guarded_job_id")
