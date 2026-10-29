import ipaddress
import os
import socket
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.schema import CreateSchema, DropSchema

from danubepulse.db import make_session_factory


@pytest.fixture(autouse=True)
def forbid_external_test_connections(monkeypatch):
    """Tests may use local PostgreSQL/HTTP only, never provider APIs or cloud services."""
    original = socket.socket.connect

    def connect(sock, address):
        if isinstance(address, tuple):
            host = address[0]
            try:
                local = ipaddress.ip_address(host).is_loopback
            except ValueError:
                local = host == "localhost"
            if not local:
                raise AssertionError("External network connection forbidden in application tests.")
        return original(sock, address)

    monkeypatch.setattr(socket.socket, "connect", connect)


@pytest.fixture
def database_engine() -> Iterator[Engine]:
    """Run real migrations; isolate PostgreSQL tests in a unique disposable schema."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    schema = f"test_danubepulse_{uuid4().hex}"
    admin_engine = None
    if database_url:
        admin_engine = create_engine(database_url)
        with admin_engine.begin() as connection:
            connection.execute(CreateSchema(schema))
        engine = create_engine(
            database_url,
            connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"},
        )
    else:
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    try:
        with engine.begin() as connection:
            config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine
    finally:
        engine.dispose()
        if admin_engine is not None:
            with admin_engine.begin() as connection:
                connection.execute(DropSchema(schema, cascade=True))
            admin_engine.dispose()


@pytest.fixture
def session_factory(database_engine):
    factory = make_session_factory("sqlite://")
    factory.kw["bind"].dispose()
    factory.configure(bind=database_engine)
    return factory
