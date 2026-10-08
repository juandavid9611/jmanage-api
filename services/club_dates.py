"""Date helpers for club matches linked to calendar events."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONE = "America/Bogota"


def event_start_to_local_date(start: int | str, tz_name: str | None) -> str:
    """Convert a calendar event `start` (epoch seconds/ms or ISO string) to YYYY-MM-DD in the account timezone."""
    tz = ZoneInfo(tz_name or DEFAULT_TIMEZONE)
    if isinstance(start, (int, float)) or (isinstance(start, str) and start.strip().lstrip("-").isdigit()):
        ts = float(start)
        if abs(ts) > 1e10:  # milliseconds
            ts /= 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz).date().isoformat()
    dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.date().isoformat()  # naive: already local wall time
    return dt.astimezone(tz).date().isoformat()
