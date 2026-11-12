"""Native Claude sub-agents declared in a pipeline config.

`config.agents` mirrors the claude CLI `--agents` option: a JSON object
keyed by agent name, each value an agent definition. The worker passes it
through verbatim, so only keys the CLI actually reads are accepted here
(the CLI silently ignores unknown keys; we reject them so a typo such as
`max_turns` instead of `maxTurns` fails loudly at pipeline creation instead
of dropping a cost guard at run time).

Validated once on POST/PATCH /pipelines (422 on error) and again at
dispatch, because a config may predate this module or be edited in SQL.
"""

import json
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictInt, ValidationError, field_validator

AGENT_NAME_PATTERN = r"^[a-z0-9][a-z0-9-]{0,63}$"
MAX_AGENTS = 8
# Upper bound on the compact JSON passed as PIPOMETA_AGENTS and then as one
# argv string to `claude --agents`. Scaleway documents no per-variable size
# limit for Jobs (Serverless Functions: 64 KiB per variable); Linux caps one
# argv/env string at 128 KiB (MAX_ARG_STRLEN). 32 KiB stays well under both.
MAX_AGENTS_JSON_BYTES = 32 * 1024
MAX_SUBAGENT_TURNS = 200

AgentModel = Literal["haiku", "sonnet", "opus", "inherit"]


class AgentsConfigError(ValueError):
    pass


class AgentSpec(BaseModel):
    """One sub-agent definition, keys as read by `claude --agents`."""

    model_config = ConfigDict(extra="forbid")

    description: str = Field(min_length=1, max_length=2000)
    prompt: str = Field(min_length=1)
    model: AgentModel | None = None
    tools: list[str] | None = None
    disallowedTools: list[str] | None = None  # noqa: N815 - CLI key
    # Strict: `true` or "5" would otherwise be coerced into a silent 1 or 5.
    maxTurns: StrictInt | None = Field(default=None, ge=1, le=MAX_SUBAGENT_TURNS)  # noqa: N815 - CLI key

    @field_validator("description", "prompt")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be blank")
        return v

    @field_validator("tools", "disallowedTools")
    @classmethod
    def _tool_names(cls, v: list[str] | None) -> list[str] | None:
        if v is not None and any(not t.strip() for t in v):
            raise ValueError("tool names must not be blank")
        return v


class AgentsConfig(RootModel[dict[str, AgentSpec]]):
    @field_validator("root")
    @classmethod
    def _names_and_count(cls, v: dict[str, AgentSpec]) -> dict[str, AgentSpec]:
        if not v:
            raise ValueError("agents must declare at least one agent (omit the key to disable)")
        if len(v) > MAX_AGENTS:
            raise ValueError(f"at most {MAX_AGENTS} agents per pipeline, got {len(v)}")
        bad = [name for name in v if not _valid_name(name)]
        if bad:
            raise ValueError(f"invalid agent name(s) {bad}: lowercase letters, digits and '-' only, max 64 chars")
        return v


def _valid_name(name: str) -> bool:
    return re.fullmatch(AGENT_NAME_PATTERN, name) is not None


def normalize_agents(raw: object) -> dict:
    """Validate `config.agents` and return it as a plain dict without null keys.

    Raises AgentsConfigError with a readable message.
    """
    try:
        parsed = AgentsConfig.model_validate(raw)
    except ValidationError as e:
        details = "; ".join(f"{'.'.join(str(p) for p in err['loc']) or 'agents'}: {err['msg']}" for err in e.errors())
        raise AgentsConfigError(f"invalid config.agents: {details}") from None
    agents = {name: spec.model_dump(exclude_none=True) for name, spec in parsed.root.items()}
    size = len(encode_agents(agents).encode("utf-8"))
    if size > MAX_AGENTS_JSON_BYTES:
        raise AgentsConfigError(
            f"invalid config.agents: serialized size {size} bytes exceeds the {MAX_AGENTS_JSON_BYTES}-byte limit "
            "(it travels as one environment variable and one CLI argument); shorten the agent prompts"
        )
    return agents


def encode_agents(agents: dict) -> str:
    return json.dumps(agents, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def validate_config(config: dict | None) -> dict | None:
    """Pipeline-config hook: normalize `agents` when set (absent or null means no sub-agents)."""
    if config is None or config.get("agents") is None:
        return config
    return {**config, "agents": normalize_agents(config["agents"])}
