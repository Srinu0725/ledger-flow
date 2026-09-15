"""add processing status to outbox enum

Revision ID: 599606e893fc
Revises: fd0dbd78a5a4
Create Date: 2026-09-15
"""

from typing import Sequence, Union

from alembic import op


revision: str = "599606e893fc"
down_revision: Union[str, Sequence[str], None] = "fd0dbd78a5a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TYPE outboxstatus ADD VALUE IF NOT EXISTS 'PROCESSING'"
    )


def downgrade() -> None:
    # PostgreSQL does not support directly removing an enum value.
    pass