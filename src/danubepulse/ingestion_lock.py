"""Serialize source ingestion across API, CLI and scheduled callers."""

import logging
import threading
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")
_local_locks = {domain: threading.Lock() for domain in ("river", "weather", "energy")}
_lock_ids = {"river": 64821001, "weather": 64821002, "energy": 64821003}


class IngestionBusy(RuntimeError):
    """Another caller is already ingesting this source."""


def serialized_ingestion(
    domain: str,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            session = args[0] if args else kwargs["session"]
            if not isinstance(session, Session):
                raise TypeError("Source ingestion requires a SQLAlchemy session.")
            bind = session.get_bind()
            if bind.dialect.name != "postgresql":
                lock = _local_locks[domain]
                if not lock.acquire(blocking=False):
                    raise IngestionBusy(f"{domain} ingestion is already running.")
                try:
                    return function(*args, **kwargs)
                finally:
                    lock.release()
            engine = bind.engine if isinstance(bind, Connection) else bind
            if not isinstance(engine, Engine):
                raise TypeError("PostgreSQL ingestion requires an engine.")
            # A separate session-level lock survives the pipeline's internal commits.
            with engine.connect() as connection:
                acquired = connection.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": _lock_ids[domain]}
                )
                if not acquired:
                    logger.warning("ingestion_already_running", extra={"status": domain})
                    raise IngestionBusy(f"{domain} ingestion is already running.")
                try:
                    return function(*args, **kwargs)
                finally:
                    connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": _lock_ids[domain]}
                    )

        return wrapped

    return decorate
