"""runs.input_uri must hold presigned https URLs (well past 512 chars).

Run with: uv run --directory orchestrator python -m unittest discover -s tests
The migration's round-trip against Postgres is checked by hand (see PR).
"""

import importlib.util
import unittest
from pathlib import Path

from orchestrator.models import Run
from orchestrator.schemas import RunTrigger
from sqlalchemy import Text

VERSIONS = Path(__file__).resolve().parent.parent / "alembic" / "versions"
LONG_URL = "https://bucket.example.test/job-inputs/run.jsonl?" + "X-Amz-Signature=" + "a" * 2000


def _load_revision(filename: str):
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), VERSIONS / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class InputUriColumnTest(unittest.TestCase):
    def test_model_column_is_unbounded_text(self):
        col_type = Run.__table__.c.input_uri.type
        self.assertIsInstance(col_type, Text)
        self.assertIsNone(getattr(col_type, "length", None))

    def test_trigger_schema_does_not_cap_length(self):
        self.assertEqual(RunTrigger(input_uri=LONG_URL).input_uri, LONG_URL)

    def test_migration_0002_follows_0001(self):
        mod = _load_revision("0002_runs_input_uri_text.py")
        self.assertEqual((mod.revision, mod.down_revision), ("0002", "0001"))
        self.assertTrue(callable(mod.upgrade) and callable(mod.downgrade))


if __name__ == "__main__":
    unittest.main()
