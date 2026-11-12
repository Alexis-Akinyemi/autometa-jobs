"""Unit tests for runner._read_prompt: s3:// and https:// input bundles.

Run with: uv run --directory worker --with pytest pytest -q tests
No network: S3 and the https opener are replaced by fakes.
"""

import io
import json
import os
import unittest
import urllib.error
from email.message import Message
from unittest import mock

from worker import runner

# Shape of a presigned URL, with dummy values only.
SIGNED = (
    "https://bucket.example.test/job-inputs/run.jsonl"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=DUMMYKEY&X-Amz-Signature=deadbeefsecret"
)
SAFE = "https://bucket.example.test/job-inputs/run.jsonl"


class _FakeResponse(io.BytesIO):
    def __init__(self, data: bytes, content_length: str | None = None):
        super().__init__(data)
        self.headers = Message()
        if content_length is not None:
            self.headers["Content-Length"] = content_length


class _FakeOpener:
    def __init__(self, response=None, exc=None):
        self.response = response
        self.exc = exc
        self.calls = []

    def open(self, req, timeout=None):
        self.calls.append((req.full_url, timeout))
        if self.exc is not None:
            raise self.exc
        return self.response


def _env(uri: str):
    return mock.patch.dict(os.environ, {"PIPOMETA_INPUT_URI": uri})


class S3InputTest(unittest.TestCase):
    def _s3(self, body: bytes):
        s3 = mock.Mock()
        s3.get_object.return_value = {"Body": io.BytesIO(body)}
        return mock.patch.object(runner, "_s3_client", return_value=s3), s3

    def test_json_prompt_key(self):
        patcher, s3 = self._s3(json.dumps({"prompt": "question"}).encode())
        with patcher, _env("s3://pipometa/inputs/x/in.json"):
            self.assertEqual(runner._read_prompt(), "question")
        s3.get_object.assert_called_once_with(Bucket="pipometa", Key="inputs/x/in.json")

    def test_raw_body_fallback(self):
        patcher, _ = self._s3(b"not json at all")
        with patcher, _env("s3://pipometa/in.txt"):
            self.assertEqual(runner._read_prompt(), "not json at all")


class HttpsInputTest(unittest.TestCase):
    def _run(self, opener):
        with mock.patch.object(runner, "_HTTPS_OPENER", opener), _env(SIGNED):
            return runner._read_prompt()

    def test_json_prompt_key(self):
        opener = _FakeOpener(_FakeResponse(json.dumps({"prompt": "q"}).encode()))
        self.assertEqual(self._run(opener), "q")
        url, timeout = opener.calls[0]
        self.assertEqual(url, SIGNED)  # the full signed URL is what gets fetched
        self.assertEqual(timeout, runner._INPUT_HTTP_TIMEOUT_S)

    def test_jsonl_body_returned_raw(self):
        body = '{"a": 1}\n{"a": 2}\n'
        self.assertEqual(self._run(_FakeOpener(_FakeResponse(body.encode()))), body)

    def test_json_non_object_returned_raw(self):
        self.assertEqual(self._run(_FakeOpener(_FakeResponse(b"[1, 2]"))), "[1, 2]")

    def test_http_error_hides_query(self):
        exc = urllib.error.HTTPError(SIGNED, 403, "Forbidden", Message(), None)
        with self.assertRaises(ValueError) as ctx:
            self._run(_FakeOpener(exc=exc))
        msg = str(ctx.exception)
        self.assertIn("HTTP 403", msg)
        self.assertIn(SAFE, msg)
        self.assertNotIn("?", msg)
        self.assertNotIn("Signature", msg)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertTrue(ctx.exception.__suppress_context__)

    def test_url_error_hides_query(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(_FakeOpener(exc=urllib.error.URLError(OSError("boom"))))
        self.assertNotIn("Signature", str(ctx.exception))
        self.assertIn(SAFE, str(ctx.exception))

    def test_timeout(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(_FakeOpener(exc=TimeoutError()))
        self.assertIn("timed out", str(ctx.exception))
        self.assertNotIn("Signature", str(ctx.exception))

    def test_size_cap_from_body(self):
        with mock.patch.object(runner, "_INPUT_MAX_BYTES", 10):
            with self.assertRaises(ValueError) as ctx:
                self._run(_FakeOpener(_FakeResponse(b"x" * 11)))
        self.assertIn("too large", str(ctx.exception))
        self.assertNotIn("Signature", str(ctx.exception))

    def test_size_cap_from_content_length(self):
        with mock.patch.object(runner, "_INPUT_MAX_BYTES", 10):
            with self.assertRaises(ValueError) as ctx:
                self._run(_FakeOpener(_FakeResponse(b"x", content_length="999")))
        self.assertIn("too large", str(ctx.exception))

    def test_exactly_at_cap_is_accepted(self):
        with mock.patch.object(runner, "_INPUT_MAX_BYTES", 10):
            self.assertEqual(self._run(_FakeOpener(_FakeResponse(b"x" * 10))), "x" * 10)

    def test_redirect_to_http_refused(self):
        handler = runner._HttpsOnlyRedirectHandler()
        req = mock.Mock()
        with self.assertRaises(ValueError) as ctx:
            handler.redirect_request(req, None, 302, "Found", {}, "http://elsewhere.test/x?sig=secret")
        self.assertNotIn("secret", str(ctx.exception))


class RefusedSchemesTest(unittest.TestCase):
    def test_plain_http_refused_without_fetch(self):
        opener = _FakeOpener(_FakeResponse(b"should not be read"))
        with mock.patch.object(runner, "_HTTPS_OPENER", opener), _env(SIGNED.replace("https://", "http://", 1)):
            with self.assertRaises(ValueError) as ctx:
                runner._read_prompt()
        self.assertEqual(opener.calls, [])
        msg = str(ctx.exception)
        self.assertIn("unsupported input uri scheme", msg)
        self.assertIn("http://bucket.example.test/job-inputs/run.jsonl", msg)
        self.assertNotIn("Signature", msg)

    def test_unknown_scheme_refused_without_query(self):
        with _env("ftp://user:pw@host.test/f.json?token=secret"):
            with self.assertRaises(ValueError) as ctx:
                runner._read_prompt()
        msg = str(ctx.exception)
        self.assertIn("ftp://host.test/f.json", msg)
        self.assertNotIn("secret", msg)
        self.assertNotIn("pw", msg)


class RedactUriTest(unittest.TestCase):
    def test_strips_query_fragment_userinfo(self):
        self.assertEqual(runner._redact_uri("https://u:p@h.test:8443/a/b?x=1#frag"), "https://h.test:8443/a/b")

    def test_no_scheme(self):
        self.assertEqual(runner._redact_uri("just-a-key?sig=1"), "<uri without scheme>")


class NoInputTest(unittest.TestCase):
    def test_system_prompt_fallback_unchanged(self):
        env = {"PIPOMETA_INPUT_URI": "", "PIPOMETA_DEFAULT_PROMPT": "", "PIPOMETA_SYSTEM_PROMPT": "do it"}
        with mock.patch.dict(os.environ, env):
            self.assertEqual(runner._read_prompt(), "do it")


if __name__ == "__main__":
    unittest.main()
