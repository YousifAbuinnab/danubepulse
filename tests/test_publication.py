import socket

import pytest
from scripts.check_publication import forbidden


@pytest.mark.parametrize(
    "path",
    [
        ".env",
        ".env.production",
        "private/backups/application.dump",
        "db.sqlite3",
        "danubepulse.db-wal",
        "data/bronze/response.json",
        "data/lakehouse/status.json",
        "data/energy-imports/a65.xml",
        "terraform/production.tfstate.backup",
        "secrets/server.key",
        "reports/private.log",
        ".aws/credentials",
    ],
)
def test_publication_guard_rejects_private_or_generated_paths(path):
    assert forbidden(path)


@pytest.mark.parametrize(
    "path",
    [
        ".env.example",
        "data/fixtures/demo_hydrology.json",
        "airflow/fixtures/interview_demo.json",
        "tests/fixtures/ehyd_features.json",
        "data/energy-imports/.gitkeep",
        "docs/streaming.md",
        "src/danubepulse/energy.py",
        "migrations/versions/0009_streaming_offsets.py",
    ],
)
def test_publication_guard_allows_explicit_source_and_synthetic_fixtures(path):
    assert not forbidden(path)


def test_application_tests_reject_external_ingestion_connections():
    with socket.socket() as connection:
        with pytest.raises(AssertionError, match="External network connection forbidden"):
            connection.connect(("198.51.100.1", 443))
