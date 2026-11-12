"""runs.input_uri: storage width, accepted schemes, and what the API echoes back."""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from orchestrator.models import Run, RunStatus
from orchestrator.schemas import RunRead, RunTrigger
from pydantic import ValidationError
from sqlalchemy import Text

VERSIONS = Path(__file__).resolve().parent.parent / "alembic" / "versions"
LONG_URL = "https://bucket.example.test/job-inputs/run.jsonl?" + "X-Amz-Signature=" + "a" * 2000


def _load_revision(filename: str):
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), VERSIONS / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run_read(input_uri: str | None) -> RunRead:
    return RunRead(
        id=uuid4(),
        pipeline_id=uuid4(),
        status=RunStatus.queued,
        scaleway_job_run_id=None,
        input_uri=input_uri,
        output_uri=None,
        summary=None,
        error_text=None,
        created_at=datetime.now(timezone.utc),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
    )


def test_model_column_is_unbounded_text():
    col_type = Run.__table__.c.input_uri.type
    assert isinstance(col_type, Text)
    assert getattr(col_type, "length", None) is None


def test_migration_0002_follows_0001():
    mod = _load_revision("0002_runs_input_uri_text.py")
    assert (mod.revision, mod.down_revision) == ("0002", "0001")
    assert callable(mod.upgrade) and callable(mod.downgrade)


@pytest.mark.parametrize(
    "given,stored",
    [
        (LONG_URL, LONG_URL),
        ("s3://pipometa/inputs/x/in.json", "s3://pipometa/inputs/x/in.json"),
        ("  HTTPS://bucket.example.test/in.json\n", "HTTPS://bucket.example.test/in.json"),
        ("", None),
        (None, None),
    ],
)
def test_trigger_accepts_s3_and_https(given, stored):
    assert RunTrigger(input_uri=given).input_uri == stored


@pytest.mark.parametrize(
    "given", ["http://bucket.example.test/in.json", "ftp://host.test/f", "pipometa/inputs/in.json"]
)
def test_trigger_refuses_other_schemes(given):
    with pytest.raises(ValidationError, match="s3:// or https://"):
        RunTrigger(input_uri=given)


@pytest.mark.parametrize(
    "stored,shown",
    [
        (LONG_URL, "https://bucket.example.test/job-inputs/run.jsonl"),
        ("s3://pipometa/inputs/x/in.json", "s3://pipometa/inputs/x/in.json"),
        (None, None),
    ],
)
def test_run_read_hides_query(stored, shown):
    assert _run_read(stored).model_dump()["input_uri"] == shown
