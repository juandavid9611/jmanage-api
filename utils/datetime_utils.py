from datetime import datetime
from typing import Union
import pytz
from babel.dates import format_datetime

# Re-exported names for easy imports in services
__all__ = ["parse_timestamp_to_datetime", "format_datetime_pretty_es"]

Number = Union[int, float]


def parse_timestamp_to_datetime(timestamp: Union[Number, str]) -> datetime:
    """
    Convert a unix timestamp (seconds or milliseconds) into a timezone-aware
    datetime in America/Bogota.

    - Accepts int/float/string.
    - Heuristic for ms vs s keeps your original threshold (> 10**10).
      If you prefer a stricter check, change to > 10**12.
    """
    if isinstance(timestamp, str):
        timestamp = float(timestamp.strip())

    if timestamp > 10**10:
        timestamp = timestamp / 1000.0

    bogota_tz = pytz.timezone("America/Bogota")
    # returns tz-aware datetime
    return datetime.fromtimestamp(timestamp, tz=bogota_tz)


def format_datetime_pretty_es(dt: datetime) -> str:
    """
    Format a datetime into Spanish (Colombia) like:
    'Martes 8 de Octubre de 2025 - 5:30 PM'

    - Uses Babel (locale='es_CO')
    - Capitalizes day-of-week and month words
    - Normalizes AM/PM to 'AM'/'PM'
    """
    # dt should already be tz-aware; if it's naive, treat it as Bogota time
    if dt.tzinfo is None:
        dt = pytz.timezone("America/Bogota").localize(dt)

    formatted = format_datetime(
        dt,
        "EEEE d 'de' MMMM 'de' yyyy '-' h:mm a",
        locale="es_CO",
    )

    words = formatted.split()
    if len(words) >= 4:
        words[0] = words[0].capitalize()   # Day name
        words[3] = words[3].capitalize()   # Month name
    formatted = " ".join(words)
    formatted = formatted.replace("a. m.", "AM").replace("p. m.", "PM")

    return formatted

def try_parsing_date(text: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try :
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    raise ValueError('no valid date format found')