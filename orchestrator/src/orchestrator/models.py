import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class RunStatus(str, enum.Enum):
    queued = "queued"
    starting = "starting"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    timed_out = "timed_out"
    quota_blocked = "quota_blocked"


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    system_prompt: Mapped[str] = mapped_column(Text)
    config: Mapped[dict] = mapped_column("config_jsonb", JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    runs: Mapped[list["Run"]] = relationship(back_populates="pipeline")


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (UniqueConstraint("pipeline_id", "idempotency_key", name="uq_runs_pipeline_idem"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("pipelines.id"))
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus, name="run_status"), default=RunStatus.queued, index=True)
    scaleway_job_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input_uri: Mapped[str | None] = mapped_column(Text)
    """s3://bucket/key or an https:// (often presigned) URL; a presigned one already runs to ~400 chars."""
    output_uri: Mapped[str | None] = mapped_column(String(512))
    summary: Mapped[str | None] = mapped_column(Text)
    error_text: Mapped[str | None] = mapped_column(Text)
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    hmac_key: Mapped[str | None] = mapped_column(String(128))
    """Per-run HMAC key, set at dispatch, cleared on terminal status."""
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    token_usage: Mapped[dict | None] = mapped_column(JSONB)

    pipeline: Mapped[Pipeline] = relationship(back_populates="runs")
    events: Mapped[list["RunEvent"]] = relationship(back_populates="run", order_by="RunEvent.seq")


class RunEvent(Base):
    __tablename__ = "run_events"
    __table_args__ = (UniqueConstraint("run_id", "seq", name="uq_runevent_seq"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id"), index=True)
    seq: Mapped[int]
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column("payload_jsonb", JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    run: Mapped[Run] = relationship(back_populates="events")


TERMINAL_STATUSES = {
    RunStatus.completed,
    RunStatus.failed,
    RunStatus.cancelled,
    RunStatus.timed_out,
}
