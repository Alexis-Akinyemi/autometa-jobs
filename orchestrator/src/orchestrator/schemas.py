from datetime import datetime
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, Field, field_serializer, field_validator

from orchestrator.agents import validate_config
from orchestrator.models import RunStatus

INPUT_URI_SCHEMES = ("s3", "https")


class PipelineCreate(BaseModel):
    name: str
    system_prompt: str
    config: dict = Field(default_factory=dict)

    @field_validator("config")
    @classmethod
    def _validate_agents(cls, v: dict) -> dict:
        return validate_config(v)


class PipelineUpdate(BaseModel):
    name: str | None = None
    system_prompt: str | None = None
    config: dict | None = None

    @field_validator("config")
    @classmethod
    def _validate_agents(cls, v: dict | None) -> dict | None:
        return validate_config(v)


class PipelineRead(BaseModel):
    id: UUID
    name: str
    system_prompt: str
    config: dict
    created_at: datetime

    model_config = {"from_attributes": True}


class RunTrigger(BaseModel):
    input_uri: str | None = None
    idempotency_key: str | None = None
    parameters: dict = Field(default_factory=dict)

    @field_validator("input_uri")
    @classmethod
    def _supported_scheme(cls, value: str | None) -> str | None:
        value = (value or "").strip()
        if not value:
            return None
        if urlsplit(value).scheme not in INPUT_URI_SCHEMES:
            raise ValueError("input_uri must be an s3:// or https:// URI")
        return value


class RunRead(BaseModel):
    id: UUID
    pipeline_id: UUID
    status: RunStatus
    scaleway_job_run_id: str | None
    input_uri: str | None
    output_uri: str | None
    summary: str | None
    error_text: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    last_heartbeat_at: datetime | None

    model_config = {"from_attributes": True}

    @field_serializer("input_uri")
    def _without_query(self, value: str | None) -> str | None:
        # Why: a presigned URL carries its signature in the query string.
        return value and value.split("?", 1)[0]


class EventIn(BaseModel):
    seq: int
    event_type: str
    payload: dict = Field(default_factory=dict)


class ResultIn(BaseModel):
    output_uri: str
    summary: str | None = None
    token_usage: dict | None = None


class HeartbeatIn(BaseModel):
    pass
