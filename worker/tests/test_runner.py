import asyncio
import json
import os
import stat
import sys

import pytest
from worker.runner import AgentsConfigError, build_command

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
    assert build_command("sys", env) == legacy_command("sys", env)


def test_agents_appended_after_legacy_flags():
    env = {"PIPOMETA_MAX_TURNS": "20", "PIPOMETA_MODEL": "opus", "PIPOMETA_AGENTS": AGENTS_JSON}
    cmd = build_command("sys", env)
    assert cmd[:-2] == legacy_command("sys", env)
    assert cmd[-2:] == ["--agents", AGENTS_JSON]


def test_unrestricted_tools_stay_unrestricted_with_agents():
    cmd = build_command("s", {"PIPOMETA_AGENTS": AGENTS_JSON})
    assert "--allowedTools" not in cmd


def _allowed(cmd: list[str]) -> str:
    return cmd[cmd.index("--allowedTools") + 1]


def test_delegation_tool_added_to_restricted_tools():
    cmd = build_command("s", {"PIPOMETA_ALLOWED_TOOLS": "Read,Bash(git *)", "PIPOMETA_AGENTS": AGENTS_JSON})
    assert _allowed(cmd) == "Read,Bash(git *),Task"


@pytest.mark.parametrize("tools", ["Read,Task", "Task", "Agent,Read", "Read, Task", "Read,Task(triage)"])
def test_delegation_tool_not_duplicated(tools):
    cmd = build_command("s", {"PIPOMETA_ALLOWED_TOOLS": tools, "PIPOMETA_AGENTS": AGENTS_JSON})
    assert _allowed(cmd) == tools


def test_delegation_tool_not_added_without_agents():
    assert _allowed(build_command("s", {"PIPOMETA_ALLOWED_TOOLS": "Read"})) == "Read"


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
        build_command("s", {"PIPOMETA_AGENTS": raw})


# --- end-to-end on a fake CLI -------------------------------------------------


class FakeClient:
    def __init__(self):
        self.events: list[tuple[str, dict]] = []
        self.results: list[dict] = []

    async def event(self, seq, event_type, payload):
        self.events.append((event_type, payload))

    async def heartbeat(self):
        pass

    async def result(self, **kwargs):
        self.results.append(kwargs)

    async def aclose(self):
        pass


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


@pytest.fixture
def fake_cli(tmp_path, monkeypatch):
    def make(stream: str):
        stream_file = tmp_path / "stream.jsonl"
        stream_file.write_text(stream)
        argv_file = tmp_path / "argv.json"
        stdin_file = tmp_path / "stdin.txt"
        script = tmp_path / "claude"
        script.write_text(
            f"#!{sys.executable}\n"
            "import json, sys\n"
            f"open({str(argv_file)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
            f"open({str(stdin_file)!r}, 'w').write(sys.stdin.read())\n"
            f"sys.stdout.write(open({str(stream_file)!r}).read())\n"
        )
        script.chmod(script.stat().st_mode | stat.S_IXUSR)
        monkeypatch.setattr(runner, "CLAUDE_BIN", str(script))
        return argv_file

    return make


def _run(monkeypatch, env: dict) -> tuple[int, FakeClient, list[str]]:
    client = FakeClient()
    uploaded: list[str] = []
    monkeypatch.setattr(runner, "OrchestratorClient", lambda: client)
    monkeypatch.setattr(runner, "_upload_artifact", lambda text: uploaded.append(text) or "s3://b/k/output.md")
    for key in ("PIPOMETA_INPUT_URI", "PIPOMETA_ALLOWED_TOOLS", "PIPOMETA_MAX_TURNS", "PIPOMETA_MODEL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PIPOMETA_SYSTEM_PROMPT", "Orchestrate.")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    rc = asyncio.run(runner.run_pipeline(asyncio.Event()))
    return rc, client, uploaded


def test_subagent_events_traced_and_kept_out_of_artifact(fake_cli, monkeypatch):
    argv_file = fake_cli(SUBAGENT_STREAM)
    rc, client, uploaded = _run(monkeypatch, {"PIPOMETA_AGENTS": AGENTS_JSON, "PIPOMETA_ALLOWED_TOOLS": "Read"})
    assert rc == 0

    argv = json.loads(argv_file.read_text())
    # The prompt goes in on stdin; in argv it only appears as the system prompt.
    assert (argv_file.parent / "stdin.txt").read_text() == "Orchestrate."
    assert argv.count("Orchestrate.") == 1 and argv[argv.index("--system-prompt") + 1] == "Orchestrate."
    assert argv[argv.index("--agents") + 1] == AGENTS_JSON
    assert argv[argv.index("--allowedTools") + 1] == "Read,Task"

    by_type: dict[str, list[dict]] = {}
    for event_type, payload in client.events:
        by_type.setdefault(event_type, []).append(payload)

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
    usage = client.results[0]["token_usage"]
    assert usage["subagent_stats"]["by_type"] == {"triage": 1}
    assert usage["modelUsage"] == {"claude-haiku": {"outputTokens": 10}}

    # Artifact: main agent's text only.
    assert uploaded == ["Final: 3 categories."]


def test_invalid_agents_fail_the_run_before_starting_cli(fake_cli, monkeypatch):
    argv_file = fake_cli(SUBAGENT_STREAM)
    rc, client, uploaded = _run(monkeypatch, {"PIPOMETA_AGENTS": "{oops"})
    assert rc == 1
    assert not argv_file.exists()  # the CLI never started
    errors = [p for t, p in client.events if t == "error"]
    assert errors and errors[0]["type"] == "AgentsConfigError"
    assert "PIPOMETA_AGENTS is not valid JSON" in errors[0]["message"]
    assert uploaded == [] and client.results == []


def test_run_without_agents_uses_legacy_command(fake_cli, monkeypatch):
    argv_file = fake_cli(_stream({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}))
    monkeypatch.delenv("PIPOMETA_AGENTS", raising=False)
    rc, _client, uploaded = _run(monkeypatch, {"PIPOMETA_MAX_TURNS": "4"})
    assert rc == 0 and uploaded == ["hi"]
    env = {**os.environ}
    assert json.loads(argv_file.read_text()) == legacy_command("Orchestrate.", env)[1:]
