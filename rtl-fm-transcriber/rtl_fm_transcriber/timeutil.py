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


def local_time(epoch: float, tz_name: str = "UTC") -> datetime:
    """An epoch time as a datetime in the configured timezone."""
    return datetime.fromtimestamp(epoch, resolve_zone(tz_name))
