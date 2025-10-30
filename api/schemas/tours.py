from pydantic import BaseModel


class PutTour(BaseModel):
    id: str | None = None
    name: str | None = None
    images: list[str] | None = None
    publish: str | None = None
    services: list[str] | None = None
    available: dict | None = None
    tourGuides: list[str] | None = None
    bookers: dict | None = None
    content: str | None = None
    tags: list[str] | None = None
    location: str | None = None
    scores: dict | None = None
    createdAt: str | None = None
    calendarEventId: str | None = None
    group: str | None = None
    eventType: str | None = None

class PatchProperty(BaseModel):
    name: str
    value: str