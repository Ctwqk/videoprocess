"""autoflow review state compatibility

Revision ID: 005
Revises: 004
Create Date: 2026-05-16
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE autoflow_plans ADD COLUMN IF NOT EXISTS request_json JSON NOT NULL DEFAULT '{}'::json")
        op.execute("ALTER TABLE autoflow_plans ADD COLUMN IF NOT EXISTS review_approved_at TIMESTAMP WITH TIME ZONE")
        op.execute("ALTER TABLE autoflow_plans ADD COLUMN IF NOT EXISTS public_approved_at TIMESTAMP WITH TIME ZONE")
        op.execute("ALTER TABLE autoflow_plans ADD COLUMN IF NOT EXISTS review_notes TEXT")
        op.execute("ALTER TABLE autoflow_plans ADD COLUMN IF NOT EXISTS rejected_reason TEXT")
        op.execute("ALTER TABLE autoflow_runs ADD COLUMN IF NOT EXISTS error_message TEXT")
        return

    plan_columns = _columns("autoflow_plans")
    if "request_json" not in plan_columns:
        op.add_column(
            "autoflow_plans",
            sa.Column("request_json", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        )
    if "review_approved_at" not in plan_columns:
        op.add_column("autoflow_plans", sa.Column("review_approved_at", sa.DateTime(timezone=True), nullable=True))
    if "public_approved_at" not in plan_columns:
        op.add_column("autoflow_plans", sa.Column("public_approved_at", sa.DateTime(timezone=True), nullable=True))
    if "review_notes" not in plan_columns:
        op.add_column("autoflow_plans", sa.Column("review_notes", sa.Text(), nullable=True))
    if "rejected_reason" not in plan_columns:
        op.add_column("autoflow_plans", sa.Column("rejected_reason", sa.Text(), nullable=True))

    run_columns = _columns("autoflow_runs")
    if "error_message" not in run_columns:
        op.add_column("autoflow_runs", sa.Column("error_message", sa.Text(), nullable=True))


def downgrade() -> None:
    # The current 004 migration already owns these columns for fresh databases.
    # Keep downgrade as a no-op so downgrading from 005 to 004 preserves that schema.
    return


def _columns(table_name: str) -> set[str]:
    bind = op.get_bind()
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}
