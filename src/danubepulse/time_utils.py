"""UTC storage and Austrian-local presentation helpers."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

VIENNA = ZoneInfo("Europe/Vienna")


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_utc(value: str | datetime) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def vienna_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(VIENNA)
