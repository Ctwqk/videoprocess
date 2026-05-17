"""deployed schema compatibility marker

Revision ID: 005
Revises: 004
Create Date: 2026-05-17
"""

from typing import Sequence, Union


revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
