"""runner.run_pipeline against a fake claude CLI."""

import asyncio
import sys

from worker import runner

# Echoes the length of the prompt it received on stdin as its only text block.
FAKE_CLI = f"""#!{sys.executable}
import json, sys
text = str(len(sys.stdin.read()))
print(json.dumps({{"type": "assistant", "message": {{"content": [{{"type": "text", "text": text}}]}}}}))
"""


def test_large_prompt_reaches_cli_on_stdin(mocker, tmp_path):
    cli = tmp_path / "claude"
    cli.write_text(FAKE_CLI)
    cli.chmod(0o755)
    mocker.patch.object(runner, "CLAUDE_BIN", str(cli))
    # Over Linux's 128 KiB cap on a single argv string.
    mocker.patch.object(runner, "_read_prompt", return_value="x" * 300_000)
    upload = mocker.patch.object(runner, "_upload_artifact", return_value="s3://pipometa/runs/out.md")
    client = mocker.AsyncMock()
    mocker.patch.object(runner, "OrchestratorClient", return_value=client)

    assert asyncio.run(runner.run_pipeline(asyncio.Event())) == 0

    upload.assert_called_once_with("300000")
    client.result.assert_awaited_once()
