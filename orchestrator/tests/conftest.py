"""Settings are read at import time: give them harmless placeholder values."""

import os

for key, value in {
    "PIPOMETA_DATABASE_URL": "postgresql+asyncpg://test:test@localhost:5432/test",
    "PIPOMETA_API_KEY": "test-key",
    "PIPOMETA_SCALEWAY_PROJECT_ID": "test-project",
    "PIPOMETA_SCALEWAY_ACCESS_KEY": "test-access",
    "PIPOMETA_SCALEWAY_SECRET_KEY": "test-secret",
    "PIPOMETA_SECRET_OAUTH_TOKEN_ID": "test-secret-id",
    "PIPOMETA_WORKER_IMAGE": "example.invalid/worker:latest",
    "PIPOMETA_PUBLIC_URL": "https://orchestrator.example.invalid",
}.items():
    os.environ.setdefault(key, value)
