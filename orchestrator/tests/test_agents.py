import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from orchestrator.agents import MAX_AGENTS, MAX_AGENTS_JSON_BYTES, AgentsConfigError, normalize_agents
from orchestrator.dispatch import agents_env_value, build_worker_env
from orchestrator.models import RunStatus
from orchestrator.schemas import PipelineCreate, PipelineUpdate
from pydantic import ValidationError

from orchestrator import dispatch

TRIAGE = {
    "description": "Sorts raw records into categories. Use for bulk extraction.",
    "prompt": "You classify records. Answer with JSON only.",
    "model": "haiku",
    "tools": ["Read", "Bash"],
    "maxTurns": 10,
}
ANALYST = {
    "description": "Reasons over the sorted records and drafts findings.",
    "prompt": "You are a careful analyst.",
    "model": "sonnet",
}


def _create(config: dict) -> PipelineCreate:
    return PipelineCreate(name="p", system_prompt="s", config=config)


# --- schema: accepted -------------------------------------------------------


def test_config_without_agents_is_untouched():
    config = {"scaleway_job_definition_id": "jd", "max_turns": 4, "allowed_tools": ["Bash"]}
    assert _create(config).config == config


def test_null_agents_means_no_subagents():
    assert _create({"agents": None}).config == {"agents": None}


def test_valid_agents_accepted_and_normalized():
    config = _create({"max_turns": 20, "agents": {"triage": TRIAGE, "analyst-2": {**ANALYST, "tools": None}}}).config
    assert config["max_turns"] == 20
    assert config["agents"]["triage"] == TRIAGE
    # null keys are dropped: the CLI then applies its own default.
    assert config["agents"]["analyst-2"] == ANALYST


@pytest.mark.parametrize("model", ["haiku", "sonnet", "opus", "inherit"])
def test_each_allowed_model(model):
    assert normalize_agents({"a": {**ANALYST, "model": model}})["a"]["model"] == model


def test_update_validates_too():
    assert PipelineUpdate(config={"agents": {"triage": TRIAGE}}).config["agents"]["triage"] == TRIAGE
    assert PipelineUpdate(name="x").config is None


def test_max_agent_count_is_accepted():
    agents = {f"a{i}": ANALYST for i in range(MAX_AGENTS)}
    assert len(normalize_agents(agents)) == MAX_AGENTS


# --- schema: refused --------------------------------------------------------


@pytest.mark.parametrize(
    "agents, needle",
    [
        ({}, "at least one agent"),
        ([], "valid dictionary"),
        ("triage", "valid dictionary"),
        ({"Triage": ANALYST}, "invalid agent name"),
        ({"tri_age": ANALYST}, "invalid agent name"),
        ({"-triage": ANALYST}, "invalid agent name"),
        ({"a" * 65: ANALYST}, "invalid agent name"),
        ({f"a{i}": ANALYST for i in range(MAX_AGENTS + 1)}, f"at most {MAX_AGENTS}"),
        ({"a": {"prompt": "p"}}, "a.description"),
        ({"a": {"description": "d"}}, "a.prompt"),
        ({"a": {**ANALYST, "description": "   "}}, "blank"),
        ({"a": {**ANALYST, "prompt": ""}}, "a.prompt"),
        ({"a": {**ANALYST, "model": "gpt-4"}}, "a.model"),
        ({"a": {**ANALYST, "model": "claude-haiku-5-5"}}, "a.model"),
        ({"a": {**ANALYST, "maxTurns": 0}}, "a.maxTurns"),
        ({"a": {**ANALYST, "maxTurns": 10_000}}, "a.maxTurns"),
        ({"a": {**ANALYST, "maxTurns": True}}, "a.maxTurns"),
        ({"a": {**ANALYST, "maxTurns": "5"}}, "a.maxTurns"),
        ({"a": {**ANALYST, "maxTurns": 5.0}}, "a.maxTurns"),
        ({"a": {**ANALYST, "max_turns": 5}}, "a.max_turns"),  # typo of a CLI key: refused, not ignored
        ({"a": {**ANALYST, "tools": "Bash"}}, "a.tools"),
        ({"a": {**ANALYST, "tools": [" "]}}, "blank"),
    ],
)
def test_invalid_agents_refused(agents, needle):
    with pytest.raises(AgentsConfigError, match="invalid config.agents") as exc:
        normalize_agents(agents)
    assert needle in str(exc.value)
    with pytest.raises(ValidationError):
        _create({"agents": agents})
    with pytest.raises(ValidationError):
        PipelineUpdate(config={"agents": agents})


def test_total_size_capped():
    big = {"a": {**ANALYST, "prompt": "x" * MAX_AGENTS_JSON_BYTES}}
    with pytest.raises(AgentsConfigError, match="exceeds"):
        normalize_agents(big)
    with pytest.raises(ValidationError):
        _create({"agents": big})


def test_size_counts_utf8_bytes():
    # 3-byte characters: under the cap in characters, over it in bytes.
    big = {"a": {**ANALYST, "prompt": "€" * (MAX_AGENTS_JSON_BYTES // 2)}}
    with pytest.raises(AgentsConfigError, match="exceeds"):
        normalize_agents(big)


# --- dispatch env -------------------------------------------------------------


def _env(config: dict) -> dict[str, str]:
    return build_worker_env(
        run_id="run-1",
        hmac_key="k",
        input_uri=None,
        pipeline_name="p",
        system_prompt="s",
        config=config,
        agents=agents_env_value(config),
        oauth_token="tok",
    )


BASE_KEYS = {
    "PIPOMETA_RUN_ID",
    "PIPOMETA_PIPELINE_NAME",
    "PIPOMETA_ORCHESTRATOR_URL",
    "PIPOMETA_RUN_HMAC_KEY",
    "PIPOMETA_SYSTEM_PROMPT",
    "PIPOMETA_ALLOWED_TOOLS",
    "PIPOMETA_MAX_TURNS",
    "PIPOMETA_MODEL",
    "PIPOMETA_INPUT_URI",
    "PIPOMETA_OUTPUT_BUCKET",
    "PIPOMETA_OUTPUT_FORMAT",
    "PIPOMETA_S3_ENDPOINT",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
}


def test_env_without_agents_has_no_agents_variable():
    env = _env({"allowed_tools": ["Bash", "Read"], "max_turns": 4, "model": "haiku"})
    assert set(env) == BASE_KEYS
    assert env["PIPOMETA_ALLOWED_TOOLS"] == "Bash,Read"
    assert env["PIPOMETA_MAX_TURNS"] == "4"
    assert env["ANTHROPIC_API_KEY"] == ""
    assert set(_env({"agents": None})) == BASE_KEYS


def test_env_with_agents_carries_compact_json():
    env = _env({"agents": {"triage": TRIAGE, "analyst": ANALYST}})
    assert set(env) == BASE_KEYS | {"PIPOMETA_AGENTS"}
    assert json.loads(env["PIPOMETA_AGENTS"]) == {"triage": TRIAGE, "analyst": ANALYST}
    assert ", " not in env["PIPOMETA_AGENTS"] and ": " not in env["PIPOMETA_AGENTS"]  # compact


def test_dispatch_revalidates_stored_config():
    # A config written before validation existed (or edited in SQL) is refused at dispatch.
    with pytest.raises(AgentsConfigError, match="invalid config.agents"):
        agents_env_value({"agents": {"Bad Name": ANALYST}})
    with pytest.raises(AgentsConfigError, match="exceeds"):
        agents_env_value({"agents": {"a": {**ANALYST, "prompt": "x" * MAX_AGENTS_JSON_BYTES}}})


def test_dispatch_fails_bad_agents_before_claiming_the_run(mocker):
    run = SimpleNamespace(id=uuid4(), pipeline_id=uuid4(), status=RunStatus.queued, hmac_key=None)
    stored = {"scaleway_job_definition_id": "job-def", "agents": {"Bad Name": ANALYST}}
    session = mocker.AsyncMock()
    session.execute.side_effect = [None, mocker.Mock(scalar_one_or_none=mocker.Mock(return_value=run))]
    session.get.return_value = SimpleNamespace(name="p", system_prompt="s", config=stored)
    mocker.patch.object(dispatch, "SessionLocal").return_value.__aenter__.return_value = session
    read_secret = mocker.patch.object(dispatch.scaleway, "access_secret_value")

    assert asyncio.run(dispatch._dispatch_one()) is True

    read_secret.assert_not_called()
    assert run.status == RunStatus.failed and run.hmac_key is None
    assert "invalid config.agents" in run.error_text
