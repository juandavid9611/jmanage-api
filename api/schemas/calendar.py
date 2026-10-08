from pydantic import BaseModel


class PutCalendarEvent(BaseModel):
    id: str
    allDay: bool | None = None
    color: str | None = None
    location: str | None = None
    description: str | None
    # TODO start and end can be int (timestamp) or str (ISO format)
    start: int | str
    end: int | str
    title: str
    category: str | None
    createTour: bool | None = True
    group: str
    tourId: str | None = None
    # Club match linked to this event. On update, "" unlinks (None means "leave as is").
    clubMatchId: str | None = None

class ParticipationRequest(BaseModel):
    value: bool