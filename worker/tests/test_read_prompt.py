"""runner._read_prompt: s3:// and https:// input bundles, without network."""

import asyncio
import io
import json
import traceback

import httpx
import pytest

from worker import runner

# Shape of a presigned URL, with dummy values only.
SIGNED = (
    "https://bucket.example.test/job-inputs/run.jsonl"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=DUMMYKEY&X-Amz-Signature=deadbeefsecret"
)
SAFE = "https://bucket.example.test/job-inputs/run.jsonl"


def _read(monkeypatch, uri: str) -> str:
    monkeypatch.setenv("PIPOMETA_INPUT_URI", uri)
    return asyncio.run(runner._read_prompt())


def _serve(mocker, handler):
    """Route the worker's httpx client to `handler` instead of the network."""
    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)
    return mocker.patch.object(
        runner.httpx, "AsyncClient", side_effect=lambda **kw: real_client(transport=transport, **kw)
    )


def _body(content, **headers):
    return lambda request: httpx.Response(200, content=content, headers=headers)


def _refuse(exc):
    def handler(request):
        raise exc

    return handler


async def _cut_body():
    yield b'{"prompt": "par'
    raise httpx.RemoteProtocolError("peer closed connection without sending complete message body")


async def _slow_body():
    yield b"x"
    await asyncio.sleep(10)
    yield b"y"


@pytest.mark.parametrize(
    "body,expected",
    [
        (json.dumps({"prompt": "question"}).encode(), "question"),
        (b"not json at all", "not json at all"),
        (b'\xef\xbb\xbf{"prompt": "question"}', "question"),
    ],
)
def test_s3_body(mocker, monkeypatch, body, expected):
    s3 = mocker.patch.object(runner, "_s3_client").return_value
    s3.get_object.return_value = {"Body": io.BytesIO(body)}
    assert _read(monkeypatch, "s3://pipometa/inputs/x/in.json") == expected
    s3.get_object.assert_called_once_with(Bucket="pipometa", Key="inputs/x/in.json")


@pytest.mark.parametrize(
    "body,expected",
    [
        (json.dumps({"prompt": "q"}).encode(), "q"),
        (b'\xef\xbb\xbf{"prompt": "q"}', "q"),
        (b'{"a": 1}\n{"a": 2}\n', '{"a": 1}\n{"a": 2}\n'),
        (b"[1, 2]", "[1, 2]"),
        (b'{"other": 1}', '{"other": 1}'),
    ],
)
def test_https_body(mocker, monkeypatch, body, expected):
    client = _serve(mocker, _body(body))
    assert _read(monkeypatch, SIGNED) == expected
    assert client.call_args.kwargs["timeout"] == runner._INPUT_FETCH_TIMEOUT_S


@pytest.mark.parametrize("uri", ["HTTPS://bucket.example.test/in.json", f"  {SIGNED}\n"])
def test_https_scheme_is_case_and_space_tolerant(mocker, monkeypatch, uri):
    _serve(mocker, _body(b"ok"))
    assert _read(monkeypatch, uri) == "ok"


def test_https_redirect_followed(mocker, monkeypatch):
    def handler(request):
        if request.url.host == "bucket.example.test":
            return httpx.Response(302, headers={"Location": "https://mirror.example.test/run.jsonl"})
        return httpx.Response(200, content=b"moved")

    _serve(mocker, handler)
    assert _read(monkeypatch, SIGNED) == "moved"


@pytest.mark.parametrize(
    "uri,handler,expected",
    [
        (SIGNED, lambda request: httpx.Response(403), "HTTP 403"),
        (SIGNED, _refuse(httpx.ConnectError("boom")), "ConnectError"),
        (SIGNED, _refuse(httpx.ReadTimeout("slow")), "timed out"),
        (SIGNED, lambda request: httpx.Response(200, content=_cut_body()), "RemoteProtocolError"),
        (SIGNED.replace("deadbeef", "dead\x01beef"), _body(b"unreached"), "InvalidURL"),
        (
            SIGNED,
            lambda request: httpx.Response(302, headers={"Location": "http://elsewhere.test/x?X-Amz-Signature=s"}),
            "non-https",
        ),
    ],
)
def test_https_failure_hides_signature(mocker, monkeypatch, uri, handler, expected):
    _serve(mocker, handler)
    with pytest.raises(ValueError, match=expected) as exc:
        _read(monkeypatch, uri)
    assert "Signature" not in "".join(traceback.format_exception(exc.value))


def test_https_total_deadline(mocker, monkeypatch):
    mocker.patch.object(runner, "_INPUT_FETCH_TIMEOUT_S", 0.05)
    _serve(mocker, lambda request: httpx.Response(200, content=_slow_body()))
    with pytest.raises(ValueError, match="timed out") as exc:
        _read(monkeypatch, SIGNED)
    assert SAFE in str(exc.value)


@pytest.mark.parametrize(
    "handler",
    [_body(b"x" * 11), _body(b"x", **{"Content-Length": "999"})],
    ids=["body", "content-length"],
)
def test_https_size_cap(mocker, monkeypatch, handler):
    mocker.patch.object(runner, "_INPUT_MAX_BYTES", 10)
    _serve(mocker, handler)
    with pytest.raises(ValueError, match="too large") as exc:
        _read(monkeypatch, SIGNED)
    assert "Signature" not in str(exc.value)


def test_https_exactly_at_cap_is_accepted(mocker, monkeypatch):
    mocker.patch.object(runner, "_INPUT_MAX_BYTES", 10)
    _serve(mocker, _body(b"x" * 10))
    assert _read(monkeypatch, SIGNED) == "x" * 10


@pytest.mark.parametrize("prompt", [None, 42, ["a"], {"a": 1}, "", "  "])
def test_invalid_prompt_value_refused(mocker, monkeypatch, prompt):
    _serve(mocker, _body(json.dumps({"prompt": prompt}).encode()))
    with pytest.raises(ValueError, match="non-empty string"):
        _read(monkeypatch, SIGNED)


@pytest.mark.parametrize(
    "uri,shown,hidden",
    [
        (SIGNED.replace("https://", "http://", 1), "http://bucket.example.test/job-inputs/run.jsonl", "Signature"),
        ("ftp://user:pw@host.test/f.json?token=secret", "ftp://host.test/f.json", "secret"),
    ],
)
def test_unsupported_scheme_refused_without_fetch(mocker, monkeypatch, uri, shown, hidden):
    client = _serve(mocker, _body(b"should not be read"))
    with pytest.raises(ValueError, match="unsupported input uri scheme") as exc:
        _read(monkeypatch, uri)
    client.assert_not_called()
    assert shown in str(exc.value)
    assert hidden not in str(exc.value)


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("https://u:p@h.test:8443/a/b?x=1#frag", "https://h.test:8443/a/b"),
        ("just-a-key?sig=1", "<uri without scheme>"),
    ],
)
def test_redact_uri(uri, expected):
    assert runner._redact_uri(uri) == expected


def test_no_input_falls_back_to_system_prompt(monkeypatch):
    monkeypatch.setenv("PIPOMETA_DEFAULT_PROMPT", "")
    monkeypatch.setenv("PIPOMETA_SYSTEM_PROMPT", "do it")
    assert _read(monkeypatch, "") == "do it"
