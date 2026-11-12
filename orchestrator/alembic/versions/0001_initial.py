"""initial schema (mirrors migrations/001_init.sql)

Revision ID: 0001
Revises:
Create Date: 2026-05-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


RUN_STATUS = postgresql.ENUM(
    "queued",
    "starting",
    "running",
    "completed",
    "failed",
    "cancelled",
    "timed_out",
    "quota_blocked",
    name="run_status",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    RUN_STATUS.create(bind, checkfirst=True)

    op.create_table(
        "pipelines",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(128), nullable=False, unique=True),
        sa.Column("system_prompt", sa.Text, nullable=False),
        sa.Column("config_jsonb", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pipeline_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipelines.id"), nullable=False),
        sa.Column("status", RUN_STATUS, nullable=False, server_default="queued"),
        sa.Column("scaleway_job_run_id", sa.String(64)),
        sa.Column("input_uri", sa.String(512)),
        sa.Column("output_uri", sa.String(512)),
        sa.Column("summary", sa.Text),
        sa.Column("error_text", sa.Text),
        sa.Column("idempotency_key", sa.String(128)),
        sa.Column("hmac_key", sa.String(128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.Column("token_usage", postgresql.JSONB),
        sa.UniqueConstraint("pipeline_id", "idempotency_key", name="uq_runs_pipeline_idem"),
    )
    op.create_index("idx_runs_status", "runs", ["status"])
    op.create_index("idx_runs_pipeline", "runs", ["pipeline_id", sa.text("created_at DESC")])

    op.create_table(
        "run_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("payload_jsonb", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("run_id", "seq", name="uq_runevent_seq"),
    )
    op.create_index("idx_run_events_run", "run_events", ["run_id", "seq"])

    op.create_table(
        "schedules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pipeline_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pipelines.id"), nullable=False),
        sa.Column("cron", sa.String(64), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True)),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("schedules")
    op.drop_index("idx_run_events_run", table_name="run_events")
    op.drop_table("run_events")
    op.drop_index("idx_runs_pipeline", table_name="runs")
    op.drop_index("idx_runs_status", table_name="runs")
    op.drop_table("runs")
    op.drop_table("pipelines")
    bind = op.get_bind()
    RUN_STATUS.drop(bind, checkfirst=True)
