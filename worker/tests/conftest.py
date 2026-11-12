"""Run the worker against a fake claude CLI that replays a stream-json transcript."""

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from worker import runner


@pytest.fixture
def fake_cli(tmp_path, mocker):
    """Install a fake `claude` that records its argv and stdin, then prints a transcript."""

    def install(transcript: str) -> SimpleNamespace:
        (tmp_path / "stream.jsonl").write_text(transcript)
        script = tmp_path / "claude"
        script.write_text(
            f"#!{sys.executable}\n"
            "import json, pathlib, sys\n"
            f"here = pathlib.Path({str(tmp_path)!r})\n"
            "(here / 'argv.json').write_text(json.dumps(sys.argv[1:]))\n"
            "(here / 'stdin.txt').write_text(sys.stdin.read())\n"
            "sys.stdout.write((here / 'stream.jsonl').read_text())\n"
        )
        script.chmod(0o755)
        mocker.patch.object(runner, "CLAUDE_BIN", str(script))
        return SimpleNamespace(
            started=lambda: (tmp_path / "argv.json").exists(),
            argv=lambda: json.loads((tmp_path / "argv.json").read_text()),
            stdin=lambda: (tmp_path / "stdin.txt").read_text(),
        )

    return install


@pytest.fixture
def run_worker(mocker, monkeypatch):
    """Run the pipeline with a mocked orchestrator client and S3 upload."""

    def run(**env: str) -> SimpleNamespace:
        client = mocker.AsyncMock()
        mocker.patch.object(runner, "OrchestratorClient", return_value=client)
        upload = mocker.patch.object(runner, "_upload_artifact", return_value="s3://pipometa/runs/out.md")
        for key in (
            "PIPOMETA_INPUT_URI",
            "PIPOMETA_ALLOWED_TOOLS",
            "PIPOMETA_MAX_TURNS",
            "PIPOMETA_MODEL",
            "PIPOMETA_AGENTS",
        ):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("PIPOMETA_SYSTEM_PROMPT", "Orchestrate.")
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        rc = asyncio.run(runner.run_pipeline(asyncio.Event()))
        return SimpleNamespace(
            rc=rc,
            events=[(c.args[1], c.args[2]) for c in client.event.await_args_list],
            results=[c.kwargs for c in client.result.await_args_list],
            artifacts=[c.args[0] for c in upload.call_args_list],
        )

    return run
