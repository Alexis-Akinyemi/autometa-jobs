"""widen runs.input_uri to text (presigned https URLs)

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # varchar(n) -> text is a catalog-only change in Postgres: no table rewrite.
    op.alter_column("runs", "input_uri", type_=sa.Text(), existing_type=sa.String(512), existing_nullable=True)


def downgrade() -> None:
    # Values longer than 512 chars are truncated on the way down; they would not
    # fit the old column otherwise. Those URLs are only useful while the run lives.
    op.alter_column(
        "runs",
        "input_uri",
        type_=sa.String(512),
        existing_type=sa.Text(),
        existing_nullable=True,
        postgresql_using="left(input_uri, 512)",
    )
