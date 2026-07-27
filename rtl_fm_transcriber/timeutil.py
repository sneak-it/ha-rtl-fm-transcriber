"""Timestamps in the configured timezone."""

import logging
from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)


def resolve_zone(tz_name: str = "UTC"):
    """Resolve an IANA timezone name, falling back to UTC."""
    if not tz_name or tz_name == "UTC":
        return UTC
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError) as e:
        logger.warning(f"Unknown timezone '{tz_name}': {e}, using UTC")
        return UTC


def now_local(tz_name: str = "UTC") -> datetime:
    """Current time in the configured timezone."""
    return datetime.now(resolve_zone(tz_name))


def format_timestamp(tz_name: str = "UTC") -> str:
    """ISO-8601 timestamp with offset, correct across DST transitions."""
    return now_local(tz_name).isoformat()
