"""fence AutoFlow authority, launch, and execute request revisions

Revision ID: 026_autoflow_authority_fence
Revises: 025_autoflow_revision_idempotency
Create Date: 2026-07-19 00:00:02.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "026_autoflow_authority_fence"
down_revision: Union[str, None] = "025_autoflow_revision_idempotency"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_TRIGGER_FUNCTION = "autoflow_plan_authority_fence"
_TRIGGER_NAME = "trg_autoflow_plan_authority_fence"


def upgrade() -> None:
    op.add_column(
        "autoflow_plans",
        sa.Column(
            "execution_revision",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.add_column(
        "autoflow_plans",
        sa.Column("approved_revision", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "autoflow_runs",
        sa.Column("request_fingerprint", sa.String(length=64), nullable=True),
    )

    if op.get_bind().dialect.name != "postgresql":
        return

    # Authority created before revision binding cannot be proven current.
    op.execute(
        """
        UPDATE autoflow_plans
        SET review_approved_at = NULL,
            public_approved_at = NULL,
            agent_approved_by = NULL,
            approved_revision_hash = NULL,
            approved_revision = NULL,
            rights_json = (
                COALESCE(rights_json, '{}'::json)::jsonb
                - 'review_approved'
                - 'public_approved'
                - 'agent_approval'
                - 'publish_allowed'
            )::json
        WHERE review_approved_at IS NOT NULL
           OR public_approved_at IS NOT NULL
           OR agent_approved_by IS NOT NULL
           OR approved_revision_hash IS NOT NULL
        """
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {_TRIGGER_FUNCTION}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            canonical_changed boolean := FALSE;
            invalid_state boolean := FALSE;
            approval_written boolean := FALSE;
            canonical_old_rights jsonb;
            canonical_new_rights jsonb;
        BEGIN
            canonical_new_rights := (
                COALESCE(NEW.rights_json, '{{}}'::json)::jsonb
                - 'review_approved'
                - 'public_approved'
                - 'agent_approval'
                - 'publish_allowed'
            );
            invalid_state := lower(COALESCE(NEW.status, '')) IN ('blocked', 'rejected')
                OR lower(COALESCE(canonical_new_rights ->> 'status', '')) IN ('blocked', 'rejected');
            approval_written := NEW.review_approved_at IS NOT NULL
                OR NEW.public_approved_at IS NOT NULL
                OR NEW.agent_approved_by IS NOT NULL
                OR NEW.approved_revision_hash IS NOT NULL
                OR NEW.approved_revision IS NOT NULL;

            IF TG_OP = 'INSERT' THEN
                NEW.execution_revision := 1;
            ELSE
                canonical_old_rights := (
                    COALESCE(OLD.rights_json, '{{}}'::json)::jsonb
                    - 'review_approved'
                    - 'public_approved'
                    - 'agent_approval'
                    - 'publish_allowed'
                );
                canonical_changed := OLD.prompt IS DISTINCT FROM NEW.prompt
                    OR COALESCE(OLD.request_json, '{{}}'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.request_json, '{{}}'::json)::jsonb
                    OR COALESCE(OLD.intent_json, '{{}}'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.intent_json, '{{}}'::json)::jsonb
                    OR OLD.template_id IS DISTINCT FROM NEW.template_id
                    OR COALESCE(OLD.pipeline_definition, '{{}}'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.pipeline_definition, '{{}}'::json)::jsonb
                    OR COALESCE(OLD.storyboard_json, 'null'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.storyboard_json, 'null'::json)::jsonb
                    OR COALESCE(OLD.candidates_json, '[]'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.candidates_json, '[]'::json)::jsonb
                    OR COALESCE(OLD.metadata_json, '{{}}'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.metadata_json, '{{}}'::json)::jsonb
                    OR COALESCE(OLD.validation_json, '{{}}'::json)::jsonb
                        IS DISTINCT FROM COALESCE(NEW.validation_json, '{{}}'::json)::jsonb
                    OR canonical_old_rights IS DISTINCT FROM canonical_new_rights;
                IF canonical_changed THEN
                    NEW.execution_revision := OLD.execution_revision + 1;
                ELSE
                    NEW.execution_revision := OLD.execution_revision;
                END IF;
            END IF;

            IF canonical_changed OR invalid_state
                OR (approval_written AND NEW.approved_revision IS DISTINCT FROM NEW.execution_revision)
            THEN
                NEW.review_approved_at := NULL;
                NEW.public_approved_at := NULL;
                NEW.agent_approved_by := NULL;
                NEW.approved_revision_hash := NULL;
                NEW.approved_revision := NULL;
                NEW.rights_json := (
                    COALESCE(NEW.rights_json, '{{}}'::json)::jsonb
                    - 'review_approved'
                    - 'public_approved'
                    - 'agent_approval'
                    - 'publish_allowed'
                )::json;
            END IF;
            RETURN NEW;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_TRIGGER_NAME}
        BEFORE INSERT OR UPDATE ON autoflow_plans
        FOR EACH ROW EXECUTE FUNCTION {_TRIGGER_FUNCTION}()
        """
    )


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute(f"DROP TRIGGER IF EXISTS {_TRIGGER_NAME} ON autoflow_plans")
        op.execute(f"DROP FUNCTION IF EXISTS {_TRIGGER_FUNCTION}()")
    op.drop_column("autoflow_runs", "request_fingerprint")
    op.drop_column("autoflow_plans", "approved_revision")
    op.drop_column("autoflow_plans", "execution_revision")
