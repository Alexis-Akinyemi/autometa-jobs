"""Dispatch step.

Picks one queued run, fetches the OAuth token, calls Scaleway Jobs `start`,
and flips the run to `running`. Designed to be invoked by the cron-triggered
`/admin/tick` (and by an inline best-effort call when a run is created).

Concurrency 1 is enforced by a Postgres advisory lock around the whole
function — no second tick can interleave with this one.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import select, text

from orchestrator import scaleway
from orchestrator.agents import AgentsConfigError, encode_agents, normalize_agents
from orchestrator.auth import new_hmac_key
from orchestrator.config import settings
from orchestrator.db import SessionLocal
from orchestrator.models import Pipeline, Run, RunEvent, RunStatus

log = logging.getLogger(__name__)

DISPATCH_LOCK_KEY = 0xB16070  # arbitrary 32-bit advisory lock id


async def _dispatch_one() -> bool:
    """Pick one queued run and start a Scaleway Job for it. Returns True if work was done."""
    async with SessionLocal() as session:
        # The advisory lock serialises this whole transaction across instances —
        # no need for a row-level FOR UPDATE on top.
        await session.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": DISPATCH_LOCK_KEY})

        row = await session.execute(select(Run).where(Run.status == RunStatus.queued).order_by(Run.created_at).limit(1))
        run = row.scalar_one_or_none()
        if run is None:
            return False

        pipeline = await session.get(Pipeline, run.pipeline_id)
        config = (pipeline.config or {}) if pipeline else {}
        pipeline_name = pipeline.name if pipeline else ""

        # Before claiming the run: a bad stored config needs no secret and no Job.
        try:
            agents = agents_env_value(config)
        except AgentsConfigError as e:
            run.status = RunStatus.failed
            run.error_text = str(e)
            run.finished_at = datetime.now(timezone.utc)
            await session.commit()
            return True

        run.status = RunStatus.starting
        run.hmac_key = new_hmac_key()
        run.started_at = datetime.now(timezone.utc)
        await session.commit()
        await session.refresh(run)

    try:
        oauth_token = await scaleway.access_secret_value(settings.secret_oauth_token_id)
    except Exception as e:
        log.exception("failed to fetch oauth token")
        await _mark_failed(run.id, f"secret fetch failed: {e}")
        return True

    job_def_id = config.get("scaleway_job_definition_id")
    if not job_def_id:
        await _mark_failed(run.id, "pipeline config missing scaleway_job_definition_id")
        return True

    env = build_worker_env(
        run_id=str(run.id),
        hmac_key=run.hmac_key or "",
        input_uri=run.input_uri,
        pipeline_name=pipeline_name,
        system_prompt=(pipeline.system_prompt if pipeline else "") or "",
        config=config,
        agents=agents,
        oauth_token=oauth_token,
    )

    try:
        job_run = await scaleway.start_job(job_def_id, env)
    except Exception as e:
        log.exception("failed to start scaleway job")
        await _mark_failed(run.id, f"scaleway start_job failed: {e}")
        return True

    async with SessionLocal() as session:
        run = await session.get(Run, run.id)
        if run is None:
            return True
        run.scaleway_job_run_id = job_run.get("id")
        run.status = RunStatus.running
        session.add(
            RunEvent(
                run_id=run.id,
                seq=0,
                event_type="dispatched",
                payload={"job_run_id": run.scaleway_job_run_id},
            )
        )
        await session.commit()

    return True


async def _mark_failed(run_id, reason: str) -> None:
    async with SessionLocal() as session:
        run = await session.get(Run, run_id)
        if run is None:
            return
        run.status = RunStatus.failed
        run.error_text = reason
        run.finished_at = datetime.now(timezone.utc)
        await session.commit()


def agents_env_value(config: dict) -> str | None:
    """PIPOMETA_AGENTS value for this pipeline config, or None when no sub-agents.

    Re-validates: a config may predate agents validation or be edited in SQL.
    Raises AgentsConfigError (bad shape, too many agents, over the size cap).
    """
    raw = config.get("agents")
    if raw is None:
        return None
    return encode_agents(normalize_agents(raw))


def build_worker_env(
    *,
    run_id: str,
    hmac_key: str,
    input_uri: str | None,
    pipeline_name: str,
    system_prompt: str,
    config: dict,
    agents: str | None,
    oauth_token: str,
) -> dict[str, str]:
    """Environment injected into the worker Job for one run."""
    allowed_tools = config.get("allowed_tools") or ""
    if isinstance(allowed_tools, list):
        allowed_tools = ",".join(allowed_tools)
    max_turns = config.get("max_turns")
    model = config.get("model") or ""

    env = {
        # Run identity / callback channel
        "PIPOMETA_RUN_ID": run_id,
        "PIPOMETA_PIPELINE_NAME": pipeline_name,
        "PIPOMETA_ORCHESTRATOR_URL": settings.public_url,
        "PIPOMETA_RUN_HMAC_KEY": hmac_key,
        # Pipeline contract
        "PIPOMETA_SYSTEM_PROMPT": system_prompt,
        "PIPOMETA_ALLOWED_TOOLS": str(allowed_tools),
        "PIPOMETA_MAX_TURNS": str(max_turns) if max_turns else "",
        "PIPOMETA_MODEL": str(model),
        # I/O
        "PIPOMETA_INPUT_URI": input_uri or "",
        "PIPOMETA_OUTPUT_BUCKET": settings.s3_bucket,
        "PIPOMETA_OUTPUT_FORMAT": str(config.get("output_format") or "md"),
        "PIPOMETA_S3_ENDPOINT": settings.s3_endpoint,
        # Auth
        "CLAUDE_CODE_OAUTH_TOKEN": oauth_token,
        # Defensive: empty string (not unset) blocks the API-key fallback path.
        "ANTHROPIC_API_KEY": "",
        # boto3 credentials for the Scaleway S3-compatible bucket.
        "AWS_ACCESS_KEY_ID": settings.scaleway_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.scaleway_secret_key,
        "AWS_DEFAULT_REGION": settings.scaleway_region,
    }
    # Native Claude sub-agents (claude --agents). Only set when configured, so
    # a pipeline without agents gets exactly the environment it got before.
    if agents is not None:
        env["PIPOMETA_AGENTS"] = agents
    return env
