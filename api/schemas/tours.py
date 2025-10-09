from typing import Optional
from pydantic import BaseModel


class PutTour(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    images: Optional[list] = None
    publish: Optional[str] = None
    services: Optional[list] = None
    available: Optional[dict] = None
    tourGuides: Optional[list] = None
    bookers: Optional[dict] = None
    content: Optional[str] = None
    tags: Optional[list] = None
    location: Optional[str] = None
    scores: Optional[dict] = None
    createdAt: Optional[str] = None
    calendarEventId: Optional[str] = None
    group: Optional[str] = None
    eventType: Optional[str] = None

class PatchProperty(BaseModel):
    name: str
    value: str