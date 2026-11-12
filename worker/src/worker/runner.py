"""The agent loop. Spawns the claude CLI directly (no SDK), parses its
stream-json output, streams events back to the orchestrator, uploads the
artifact to S3, and reports a result before exiting.

Mirrors the autometa/matometa production pattern: env-var auth, JSON Lines
on stdout, stderr drained in parallel.
"""

import asyncio
import contextlib
import io
import json
import logging
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from urllib.parse import urlsplit

import boto3
import httpx

from worker.client import OrchestratorClient

log = logging.getLogger(__name__)

CLAUDE_BIN = os.environ.get("CLAUDE_CLI", "claude")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("PIPOMETA_S3_ENDPOINT", "https://s3.fr-par.scw.cloud"),
        region_name="fr-par",
    )


async def _heartbeat_task(client: OrchestratorClient, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await client.heartbeat()
        except Exception:
            log.exception("heartbeat failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass


# Input bundles fetched over https (e.g. a presigned URL on the caller's own
# bucket) are capped in size and total time so a wrong link can neither fill
# the container's memory nor hold the single dispatch slot.
_INPUT_MAX_BYTES = 50 * 1024 * 1024
_INPUT_FETCH_TIMEOUT_S = 60


def _redact_uri(uri: str) -> str:
    """scheme://host/path only: no query (presigned URLs carry their signature
    there), no fragment, no userinfo. Safe to put in errors and run events."""
    try:
        parts = urlsplit(uri)
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
    except ValueError:
        return "<unparseable uri>"
    if not parts.scheme:
        return "<uri without scheme>"
    return f"{parts.scheme}://{host}{parts.path}"


async def _refuse_plain_http(request: httpx.Request) -> None:
    # Why: the bundle is the agent's instructions; a redirect must not downgrade it to http.
    if request.url.scheme != "https":
        raise ValueError(f"input fetch refused a non-https location: {_redact_uri(str(request.url))}")


async def _fetch_https(uri: str) -> str:
    safe = _redact_uri(uri)
    data = bytearray()
    try:
        async with (
            asyncio.timeout(_INPUT_FETCH_TIMEOUT_S),
            httpx.AsyncClient(
                timeout=_INPUT_FETCH_TIMEOUT_S,
                follow_redirects=True,
                headers={"User-Agent": "pipometa-worker"},
                event_hooks={"request": [_refuse_plain_http]},
            ) as http,
            http.stream("GET", uri) as resp,
        ):
            resp.raise_for_status()
            declared = resp.headers.get("Content-Length", "")
            if declared.isdigit() and int(declared) > _INPUT_MAX_BYTES:
                raise ValueError(f"input too large ({declared} bytes > {_INPUT_MAX_BYTES}): {safe}")
            async for chunk in resp.aiter_bytes():
                data += chunk
                if len(data) > _INPUT_MAX_BYTES:
                    raise ValueError(f"input too large (> {_INPUT_MAX_BYTES} bytes): {safe}")
    # `from None`: httpx exceptions carry the full signed URL.
    except httpx.HTTPStatusError as e:
        raise ValueError(f"input fetch failed (HTTP {e.response.status_code}): {safe}") from None
    except TimeoutError, httpx.TimeoutException:
        raise ValueError(f"input fetch timed out after {_INPUT_FETCH_TIMEOUT_S}s: {safe}") from None
    except (httpx.HTTPError, httpx.InvalidURL) as e:
        raise ValueError(f"input fetch failed ({type(e).__name__}): {safe}") from None
    return data.decode("utf-8-sig")


def _fetch_s3(uri: str) -> str:
    bucket, _, key = uri[5:].partition("/")
    s3 = _s3_client()
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig")


def _prompt_from_body(body: str) -> str:
    """A JSON object's `prompt` key if present, the raw body otherwise."""
    try:
        doc = json.loads(body)
    except json.JSONDecodeError:
        return body
    if not isinstance(doc, dict) or "prompt" not in doc:
        return body
    prompt = doc["prompt"]
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("input bundle: `prompt` must be a non-empty string")
    return prompt


async def _read_prompt() -> str:
    input_uri = (os.environ.get("PIPOMETA_INPUT_URI") or "").strip()
    if not input_uri:
        # No per-run input bundle: a self-contained pipeline carries its whole
        # task in the system prompt. Use it as the driving prompt so the CLI
        # has something to act on (--system-prompt alone never triggers work).
        explicit_default = os.environ.get("PIPOMETA_DEFAULT_PROMPT")
        if explicit_default:
            return explicit_default
        system_prompt = (os.environ.get("PIPOMETA_SYSTEM_PROMPT") or "").strip()
        if system_prompt:
            return system_prompt
        return "Say hello in 3 lines, in French. End with a haiku."
    scheme = urlsplit(input_uri).scheme
    if scheme == "s3":
        body = await asyncio.to_thread(_fetch_s3, input_uri)
    elif scheme == "https":
        body = await _fetch_https(input_uri)
    else:
        # Plain http:// is refused: the bundle is the agent's instructions.
        raise ValueError(f"unsupported input uri scheme (expected s3:// or https://): {_redact_uri(input_uri)}")
    return _prompt_from_body(body)


# Output formats a pipeline may declare (config.output_format) → (filename, content-type).
_OUTPUT_FORMATS = {
    "md": ("output.md", "text/markdown; charset=utf-8"),
    "csv": ("output.csv", "text/csv; charset=utf-8"),
    "json": ("output.json", "application/json; charset=utf-8"),
    "txt": ("output.txt", "text/plain; charset=utf-8"),
}


def _upload_artifact(text: str) -> str:
    bucket = os.environ["PIPOMETA_OUTPUT_BUCKET"]
    run_id = os.environ["PIPOMETA_RUN_ID"]
    pipeline = os.environ.get("PIPOMETA_PIPELINE_NAME", "unknown")
    fmt = (os.environ.get("PIPOMETA_OUTPUT_FORMAT") or "md").lower()
    filename, content_type = _OUTPUT_FORMATS.get(fmt, _OUTPUT_FORMATS["md"])
    ts = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"runs/{ts}/{pipeline}/{run_id}/{filename}"
    s3 = _s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=io.BytesIO(text.encode("utf-8")),
        ContentType=content_type,
    )
    return f"s3://{bucket}/{key}"


async def _drain_stderr(stream: asyncio.StreamReader, sink: list[str]) -> None:
    while True:
        line = await stream.readline()
        if not line:
            return
        s = line.decode("utf-8", "replace").rstrip()
        if s:
            sink.append(s)
            if len(sink) > 200:
                del sink[: len(sink) - 200]


def _parse_assistant_blocks(event: dict) -> tuple[list[dict], list[str]]:
    """Return (event-payload-blocks, raw-text-fragments) from a claude 'assistant' event."""
    blocks: list[dict] = []
    text_parts: list[str] = []
    msg = event.get("message", {}) or {}
    for block in msg.get("content", []) or []:
        bt = block.get("type")
        if bt == "text":
            t = (block.get("text") or "").strip()
            if t:
                blocks.append({"type": "text", "text": t})
                text_parts.append(t)
        elif bt == "tool_use":
            blocks.append({
                "type": "tool_use",
                "name": block.get("name"),
                "id": block.get("id"),
                "input": block.get("input"),
            })
    return blocks, text_parts


class AgentsConfigError(ValueError):
    pass


def _load_agents(raw: str) -> dict:
    """Parse and check PIPOMETA_AGENTS, the sub-agents to pass to `claude --agents`.

    The orchestrator already validated it against the full schema; this is a
    last structural check so a corrupt value fails the run loudly instead of
    starting a session silently stripped of its sub-agents.
    """
    try:
        agents = json.loads(raw)
    except json.JSONDecodeError as e:
        raise AgentsConfigError(f"PIPOMETA_AGENTS is not valid JSON: {e}") from None
    if not isinstance(agents, dict) or not agents:
        raise AgentsConfigError("PIPOMETA_AGENTS must be a non-empty JSON object keyed by agent name")
    for name, spec in agents.items():
        if not isinstance(spec, dict):
            raise AgentsConfigError(f"PIPOMETA_AGENTS[{name!r}] must be an object")
        for key in ("description", "prompt"):
            if not isinstance(spec.get(key), str) or not spec[key].strip():
                raise AgentsConfigError(f"PIPOMETA_AGENTS[{name!r}].{key} must be a non-empty string")
    return agents


def build_command(system_prompt: str, env: Mapping[str, str], agents: dict) -> list[str]:
    """argv for the claude CLI (the prompt goes in on stdin).

    Without sub-agents it is the historical command, unchanged.
    """
    # Worker container runs as non-root `runner`, so the CLI accepts
    # --dangerously-skip-permissions. The container itself is the security
    # boundary (gVisor sandbox, ephemeral); inside, the agent should have
    # full freedom. Under this flag PIPOMETA_ALLOWED_TOOLS (--allowedTools)
    # only pre-approves tools, it removes none.
    cmd = [
        CLAUDE_BIN,
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
    if agents:
        cmd.extend(["--agents", json.dumps(agents, ensure_ascii=False, separators=(",", ":"))])
    return cmd


# Keys of the CLI's background-task system events (task_started,
# task_updated, task_notification) worth keeping for tracing sub-agent runs.
# Prompts and output file paths are left out.
_TASK_EVENT_KEYS = (
    "task_id",
    "tool_use_id",
    "description",
    "subagent_type",
    "task_type",
    "is_backgrounded",
    "spawn_depth",
    "status",
    "summary",
    "usage",
)


def _system_event_payload(ev: dict) -> dict:
    payload = {"subtype": ev.get("subtype"), "message": ev.get("message")}
    if ev.get("subtype") == "init":
        payload["agents"] = ev.get("agents")
        payload["tools"] = ev.get("tools")
    elif str(ev.get("subtype") or "").startswith("task_"):
        for key in _TASK_EVENT_KEYS:
            if key in ev:
                payload[key] = ev[key][:2000] if isinstance(ev[key], str) else ev[key]
    return payload


def _result_usage(ev: dict) -> dict:
    usage = {
        "duration_ms": ev.get("duration_ms"),
        "duration_api_ms": ev.get("duration_api_ms"),
        "num_turns": ev.get("num_turns"),
        "total_cost_usd": ev.get("total_cost_usd"),
        "usage": ev.get("usage"),
        "subtype": ev.get("subtype"),
    }
    # Per-model usage and sub-agent counters, when the CLI reports them.
    for key in ("modelUsage", "subagent_stats"):
        if ev.get(key) is not None:
            usage[key] = ev[key]
    return usage


def _add_result(total: dict | None, result: dict) -> dict:
    """Fold one `result` event into the run's usage.

    With background sub-agents the CLI ends a run with several result events:
    turns, duration and token counts are per event; cost, per-model usage and
    sub-agent counters are already running totals, and so is API time except
    on a closing event with no turn, where it reads 0.
    """
    if total is None:
        return result
    previous = total.get("usage") or {}
    return {
        **result,
        "num_turns": (total.get("num_turns") or 0) + (result.get("num_turns") or 0),
        "duration_ms": (total.get("duration_ms") or 0) + (result.get("duration_ms") or 0),
        "duration_api_ms": max(total.get("duration_api_ms") or 0, result.get("duration_api_ms") or 0),
        "usage": {
            key: previous.get(key, 0) + value if isinstance(value, int) else value
            for key, value in (result.get("usage") or {}).items()
        },
    }


# The CLI's delegation tool, "Agent" in the stream (formerly "Task").
_DELEGATION_TOOLS = {"Agent", "Task"}


def _missing_agents(init: dict, agents: dict) -> list[str]:
    """Declared sub-agents the CLI's init event does not list as loaded."""
    loaded = set(init.get("agents") or [])
    return [name for name in agents if name not in loaded]


def _unlisted_tools(init: dict, agents: dict) -> dict[str, list[str]]:
    """Sub-agent tool names absent from the CLI's init tool list.

    Only a warning: the CLI loads some tools on demand and leaves them out of
    that list, so absence does not prove a typo.
    """
    listed = set(init.get("tools") or [])
    if not listed:
        return {}
    if listed & _DELEGATION_TOOLS:
        listed |= _DELEGATION_TOOLS
    unlisted = {}
    for name, spec in agents.items():
        tools = [t for key in ("tools", "disallowedTools") for t in spec.get(key) or []]
        if missing := [t for t in tools if t.split("(", 1)[0].strip() not in listed]:
            unlisted[name] = missing
    return unlisted


class _Deliverable:
    """The main agent's text, minus what it writes around its sub-agents.

    Sub-agents run in the background: the main agent announces them in the
    message that launches them and may write while they run ("the analyst is
    still working…"). Neither belongs in the artifact.
    """

    def __init__(self) -> None:
        self._parts: list[tuple[str | None, str]] = []
        self._launching: set[str] = set()
        self._running = False

    def on_system(self, ev: dict) -> None:
        if ev.get("subtype") == "background_tasks_changed":
            self._running = bool(ev.get("tasks"))

    def on_main_agent(self, ev: dict, parts: list[str]) -> None:
        msg = ev.get("message") or {}
        msg_id = msg.get("id")
        # The CLI streams a message one content block at a time, all under the same id.
        if any(b.get("type") == "tool_use" and b.get("name") in _DELEGATION_TOOLS for b in msg.get("content") or []):
            if msg_id:
                self._launching.add(msg_id)
                self._parts = [(m, part) for m, part in self._parts if m != msg_id]
            return
        if self._running or msg_id in self._launching:
            return
        self._parts.extend((msg_id, part) for part in parts)

    def text(self) -> str:
        return "\n\n".join(part for _, part in self._parts).strip()


async def run_pipeline(cancelled: asyncio.Event) -> int:
    client = OrchestratorClient()
    stop = asyncio.Event()
    hb = asyncio.create_task(_heartbeat_task(client, stop))

    seq = 1
    deliverable = _Deliverable()
    init_checked = False
    cli_stderr: list[str] = []
    token_usage: dict | None = None
    exit_code = 0

    try:
        await client.event(seq, "started", {"pid": os.getpid()})
        seq += 1

        system_prompt = (os.environ.get("PIPOMETA_SYSTEM_PROMPT") or "").strip() or (
            "You are a focused assistant. Be concise."
        )
        agents_raw = (os.environ.get("PIPOMETA_AGENTS") or "").strip()
        agents = _load_agents(agents_raw) if agents_raw else {}
        cmd = build_command(system_prompt, os.environ, agents)

        prompt = await _read_prompt()
        await client.event(seq, "prompt_loaded", {"chars": len(prompt)})
        seq += 1
        # Pass the env through unchanged. Dispatch sets ANTHROPIC_API_KEY="" so
        # the CLI sees an explicit empty value rather than nothing — empty blocks
        # the API-key fallback path; unset would let some code paths re-resolve.
        env = dict(os.environ)

        # Why: the prompt goes in on stdin, not argv: Linux caps a single
        # argument at 128 KiB and input bundles can be far larger.
        with tempfile.TemporaryFile() as prompt_file:
            prompt_file.write(prompt.encode("utf-8"))
            prompt_file.seek(0)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=prompt_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                limit=10 * 1024 * 1024,
            )
        stderr_task = asyncio.create_task(_drain_stderr(proc.stderr, cli_stderr))

        try:
            while True:
                if cancelled.is_set():
                    proc.terminate()
                    await client.event(seq, "cancelled", {})
                    seq += 1
                    exit_code = 130
                    break
                line = await proc.stdout.readline()
                if not line:
                    break
                line_str = line.decode("utf-8", "replace").strip()
                if not line_str:
                    continue
                try:
                    ev = json.loads(line_str)
                except json.JSONDecodeError:
                    await client.event(seq, "raw_line", {"line": line_str[:500]})
                    seq += 1
                    continue

                ev_type = ev.get("type")
                # Messages produced inside a sub-agent carry the id of the
                # delegating tool_use; the main agent's own messages carry none.
                parent_id = ev.get("parent_tool_use_id")
                if ev_type == "assistant":
                    blocks, parts = _parse_assistant_blocks(ev)
                    if blocks:
                        payload = {"blocks": blocks}
                        if parent_id:
                            payload["parent_tool_use_id"] = parent_id
                        await client.event(seq, "assistant_message", payload)
                        seq += 1
                    # The artifact is the main agent's text only; a sub-agent
                    # reports back to the main agent, not to the artifact.
                    if not parent_id:
                        deliverable.on_main_agent(ev, parts)
                elif ev_type == "user":
                    msg = ev.get("message", {}) or {}
                    for block in msg.get("content", []) or []:
                        if block.get("type") == "tool_result":
                            content = block.get("content", "")
                            payload = {
                                "tool_use_id": block.get("tool_use_id"),
                                "content": str(content)[:2000],
                            }
                            if parent_id:
                                payload["parent_tool_use_id"] = parent_id
                            await client.event(seq, "tool_result", payload)
                            seq += 1
                elif ev_type == "result":
                    result_usage = _result_usage(ev)
                    token_usage = _add_result(token_usage, result_usage)
                    await client.event(seq, "result", result_usage)
                    seq += 1
                elif ev_type == "system":
                    deliverable.on_system(ev)
                    # Why: progress events repeat what the sub-agent's own messages already trace.
                    if ev.get("subtype") != "task_progress":
                        await client.event(seq, "system", _system_event_payload(ev))
                        seq += 1
                    if ev.get("subtype") == "init" and agents and not init_checked:
                        init_checked = True
                        if unlisted := _unlisted_tools(ev, agents):
                            await client.event(seq, "agents_warning", {"unlisted_tools": unlisted})
                            seq += 1
                        if missing := _missing_agents(ev, agents):
                            with contextlib.suppress(ProcessLookupError):
                                proc.terminate()
                            raise AgentsConfigError(f"the claude CLI did not load sub-agent(s) {missing}")
                elif ev_type == "error":
                    await client.event(seq, "claude_error", ev)
                    seq += 1
                else:
                    await client.event(seq, "message", {"kind": ev_type})
                    seq += 1
        finally:
            await proc.wait()
            try:
                await asyncio.wait_for(stderr_task, timeout=2)
            except asyncio.TimeoutError:
                pass

        if proc.returncode != 0 and exit_code == 0:
            await client.event(
                seq,
                "error",
                {
                    "type": "ProcessError",
                    "exit_code": proc.returncode,
                    "cli_stderr": "\n".join(cli_stderr)[-3000:],
                },
            )
            seq += 1
            return 1

        artifact = deliverable.text() or "(empty)"
        output_uri = _upload_artifact(artifact)
        await client.result(output_uri=output_uri, summary=artifact[:280], token_usage=token_usage)
        return exit_code

    except Exception as e:
        log.exception("pipeline failed")
        try:
            await client.event(
                seq,
                "error",
                {
                    "type": type(e).__name__,
                    "message": str(e)[:2000],
                    "cli_stderr": "\n".join(cli_stderr)[-3000:],
                },
            )
        except Exception:
            pass
        return 1
    finally:
        stop.set()
        await hb
        await client.aclose()
