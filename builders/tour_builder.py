from typing import List, Optional, Union
from uuid import uuid4
from api.schemas.calendar import PutCalendarEvent
from api.schemas.tours import PutTour
from utils.datetime_utils import parse_timestamp_to_datetime, format_datetime_pretty_es

SERVICE_BY_GROUP = {
    "male": "Vittoria Masculino",
    "female": "Vittoria Femenino",
}

def _dedup_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in items:
        if s and s not in seen:
            out.append(s)
            seen.add(s)
    return out

def _training_title(start_ts: Optional[Union[int, float, str]]) -> str:
    if start_ts is not None:
        dt = parse_timestamp_to_datetime(start_ts)
        pretty = format_datetime_pretty_es(dt)
        return f"Entrenamiento {pretty}"
    return "Entrenamiento"

def _compute_title(evt: PutCalendarEvent) -> str:
    if evt.category == "training":
        return _training_title(evt.start)
    return evt.title or (evt.category.capitalize() if evt.category else "Evento")

def _services_for(evt: PutCalendarEvent) -> List[str]:
    raw = [SERVICE_BY_GROUP.get(evt.group), evt.category]
    return _dedup_preserve_order([s for s in raw if s])

def build_tour_from_calendar_event(evt: PutCalendarEvent) -> PutTour:
    """
    Pure builder used by the create_calendar endpoint.
    """
    return PutTour(
        id=f"{uuid4().hex}",
        name=_compute_title(evt),
        images=[],
        publish="draft",
        services=_services_for(evt),
        available={"startDate": evt.start, "endDate": evt.end},
        tourGuides=[],
        bookers={},
        content="",
        tags=[],
        location=evt.location,
        scores={"home": 0, "away": 0},
        calendarEventId=evt.id,
        eventType=evt.category,
        group=evt.group,
    )
