import json

import pytest
from worker.runner import AgentsConfigError, _load_agents, build_command

from worker import runner

AGENTS = {
    "triage": {"description": "Sorts records", "prompt": "Classify.", "model": "haiku", "maxTurns": 5},
    "analyst": {"description": "Reasons", "prompt": "Analyse.", "model": "sonnet"},
}
AGENTS_JSON = json.dumps(AGENTS, separators=(",", ":"))


def legacy_command(system_prompt: str, env: dict) -> list[str]:
    """The worker's command line before sub-agents existed (prompt on stdin), kept verbatim as a golden reference."""
    cmd = [
        runner.CLAUDE_BIN,
        "--output-format",
        "stream-json",
        "--verbose",
        "--system-prompt",
        system_prompt,
        "--dangerously-skip-permissions",
        "-p",
    ]
    if extra_tools := (env.get("PIPOMETA_ALLOWED_TOOLS") or "").strip():
        cmd.extend(["--allowedTools", extra_tools])
    if max_turns := (env.get("PIPOMETA_MAX_TURNS") or "").strip():
        cmd.extend(["--max-turns", max_turns])
    if model := (env.get("PIPOMETA_MODEL") or "").strip():
        cmd.extend(["--model", model])
    return cmd


LEGACY_ENVS = [
    {},
    {"PIPOMETA_ALLOWED_TOOLS": "", "PIPOMETA_MAX_TURNS": "", "PIPOMETA_MODEL": ""},
    {"PIPOMETA_ALLOWED_TOOLS": "Read,Bash,Glob,Grep", "PIPOMETA_MAX_TURNS": "300", "PIPOMETA_MODEL": "haiku"},
    {"PIPOMETA_ALLOWED_TOOLS": "Bash(git *),Read", "PIPOMETA_MAX_TURNS": "4"},
    {"PIPOMETA_MODEL": "claude-sonnet-5-5", "PIPOMETA_AGENTS": ""},
    {"PIPOMETA_ALLOWED_TOOLS": "Bash", "PIPOMETA_AGENTS": "   "},
]


@pytest.mark.parametrize("env", LEGACY_ENVS)
def test_command_without_agents_is_byte_identical(env):
    assert build_command("sys", env, {}) == legacy_command("sys", env)


def test_agents_appended_after_legacy_flags():
    env = {"PIPOMETA_MAX_TURNS": "20", "PIPOMETA_MODEL": "opus"}
    cmd = build_command("sys", env, AGENTS)
    assert cmd[:-2] == legacy_command("sys", env)
    assert cmd[-2:] == ["--agents", AGENTS_JSON]


@pytest.mark.parametrize("tools", ["", "Read,Bash(git *)"])
def test_allowed_tools_passed_through_with_agents(tools):
    cmd = build_command("s", {"PIPOMETA_ALLOWED_TOOLS": tools}, AGENTS)
    assert cmd[:-2] == legacy_command("s", {"PIPOMETA_ALLOWED_TOOLS": tools})


@pytest.mark.parametrize(
    "raw, needle",
    [
        ("{not json", "not valid JSON"),
        ("[]", "non-empty JSON object"),
        ("{}", "non-empty JSON object"),
        ('{"a": "x"}', "must be an object"),
        ('{"a": {"prompt": "p"}}', "description"),
        ('{"a": {"description": "d", "prompt": "  "}}', "prompt"),
    ],
)
def test_invalid_agents_rejected(raw, needle):
    with pytest.raises(AgentsConfigError, match=needle):
        _load_agents(raw)


# --- end-to-end on a fake CLI -------------------------------------------------


def _stream(*events: dict) -> str:
    return "\n".join(json.dumps(e) for e in events) + "\n"


TOOL_ID = "toolu_parent_1"
# Shape recorded from claude 2.1.322 with --agents (one delegated sub-agent, run in background).
SUBAGENT_STREAM = _stream(
    {"type": "system", "subtype": "init", "agents": ["general-purpose", "triage", "analyst"]},
    {
        "type": "assistant",
        "parent_tool_use_id": None,
        "message": {
            "content": [
                {
                    "type": "tool_use",
                    "id": TOOL_ID,
                    "name": "Agent",
                    "input": {"description": "Sort", "subagent_type": "triage", "prompt": "Sort these."},
                }
            ]
        },
    },
    {
        "type": "system",
        "subtype": "task_started",
        "task_id": "t1",
        "tool_use_id": TOOL_ID,
        "description": "Sort",
        "subagent_type": "triage",
        "is_backgrounded": True,
        "spawn_depth": 1,
        "task_type": "local_agent",
        "prompt": "Sort these.",
    },
    {
        "type": "user",
        "parent_tool_use_id": None,
        "message": {"content": [{"type": "tool_result", "tool_use_id": TOOL_ID, "content": "Async agent launched"}]},
    },
    {
        "type": "assistant",
        "parent_tool_use_id": TOOL_ID,
        "message": {"content": [{"type": "text", "text": "SUBAGENT-ONLY: 3 categories"}]},
    },
    {
        "type": "user",
        "parent_tool_use_id": TOOL_ID,
        "message": {"content": [{"type": "tool_result", "tool_use_id": "toolu_child", "content": "ls output"}]},
    },
    {
        "type": "system",
        "subtype": "task_notification",
        "task_id": "t1",
        "tool_use_id": TOOL_ID,
        "status": "completed",
        "summary": "3 categories",
        "output_file": "/tmp/x",
        "usage": {"total_tokens": 1673, "tool_uses": 0, "duration_ms": 975},
    },
    {
        "type": "assistant",
        "parent_tool_use_id": None,
        "message": {"content": [{"type": "text", "text": "Final: 3 categories."}]},
    },
    {
        "type": "result",
        "subtype": "success",
        "num_turns": 2,
        "total_cost_usd": 0.01,
        "usage": {"output_tokens": 10},
        "modelUsage": {"claude-haiku": {"outputTokens": 10}},
        "subagent_stats": {"spawned": 1, "completed": 1, "by_type": {"triage": 1}},
    },
)


def _by_type(events):
    by_type: dict[str, list[dict]] = {}
    for event_type, payload in events:
        by_type.setdefault(event_type, []).append(payload)
    return by_type


def test_subagent_events_traced_and_kept_out_of_artifact(fake_cli, run_worker):
    cli = fake_cli(SUBAGENT_STREAM)
    run = run_worker(PIPOMETA_AGENTS=AGENTS_JSON, PIPOMETA_ALLOWED_TOOLS="Read")
    assert run.rc == 0

    argv = cli.argv()
    # The prompt goes in on stdin; in argv it only appears as the system prompt.
    assert cli.stdin() == "Orchestrate."
    assert argv.count("Orchestrate.") == 1 and argv[argv.index("--system-prompt") + 1] == "Orchestrate."
    assert argv[argv.index("--agents") + 1] == AGENTS_JSON
    assert argv[argv.index("--allowedTools") + 1] == "Read"

    by_type = _by_type(run.events)
    # The delegation call itself, from the main agent.
    calls = [b for p in by_type["assistant_message"] for b in p["blocks"] if b["type"] == "tool_use"]
    assert calls == [
        {
            "type": "tool_use",
            "name": "Agent",
            "id": TOOL_ID,
            "input": {"description": "Sort", "subagent_type": "triage", "prompt": "Sort these."},
        }
    ]
    # The sub-agent's own messages and tool results, tagged with the delegating call.
    sub_msgs = [p for p in by_type["assistant_message"] if p.get("parent_tool_use_id") == TOOL_ID]
    assert [b["text"] for p in sub_msgs for b in p["blocks"]] == ["SUBAGENT-ONLY: 3 categories"]
    sub_results = [p for p in by_type["tool_result"] if p.get("parent_tool_use_id") == TOOL_ID]
    assert [p["tool_use_id"] for p in sub_results] == ["toolu_child"]
    assert "parent_tool_use_id" not in by_type["tool_result"][0]  # main-agent result stays as before
    # Task lifecycle events keep their identifying fields, not the prompt or file paths.
    started, notified = (p for p in by_type["system"] if p["subtype"].startswith("task_"))
    assert started["subagent_type"] == "triage" and started["tool_use_id"] == TOOL_ID
    assert "prompt" not in started
    assert notified["status"] == "completed" and notified["usage"]["total_tokens"] == 1673
    assert "output_file" not in notified
    # Result carries sub-agent counters and per-model usage.
    usage = run.results[0]["token_usage"]
    assert usage["subagent_stats"]["by_type"] == {"triage": 1}
    assert usage["modelUsage"] == {"claude-haiku": {"outputTokens": 10}}

    # Artifact: main agent's text only.
    assert run.artifacts == ["Final: 3 categories."]


def test_invalid_agents_fail_the_run_before_reading_input(fake_cli, run_worker, mocker):
    cli = fake_cli(SUBAGENT_STREAM)
    read_prompt = mocker.patch.object(runner, "_read_prompt", return_value="unused")
    run = run_worker(PIPOMETA_AGENTS="{oops")
    assert run.rc == 1
    read_prompt.assert_not_awaited()
    assert not cli.started()
    errors = [p for t, p in run.events if t == "error"]
    assert errors and errors[0]["type"] == "AgentsConfigError"
    assert "PIPOMETA_AGENTS is not valid JSON" in errors[0]["message"]
    assert run.artifacts == [] and run.results == []


def test_run_without_agents_uses_legacy_command(fake_cli, run_worker):
    cli = fake_cli(_stream({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}))
    run = run_worker(PIPOMETA_MAX_TURNS="4")
    assert run.rc == 0 and run.artifacts == ["hi"]
    assert cli.argv() == legacy_command("Orchestrate.", {"PIPOMETA_MAX_TURNS": "4"})[1:]


def test_large_prompt_reaches_cli_on_stdin(fake_cli, run_worker, mocker):
    cli = fake_cli(_stream({"type": "assistant", "message": {"content": [{"type": "text", "text": "ok"}]}}))
    # Over Linux's 128 KiB cap on a single argv string.
    mocker.patch.object(runner, "_read_prompt", return_value="x" * 300_000)
    assert run_worker().rc == 0
    assert cli.stdin() == "x" * 300_000
    assert max(map(len, cli.argv())) < 1000
