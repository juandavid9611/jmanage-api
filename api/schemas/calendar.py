from pydantic import BaseModel
from typing import Optional, Union


class PutCalendarEvent(BaseModel):
    id: str
    allDay: Optional[bool] = None
    color: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str]
    # TODO start and end can be int (timestamp) or str (ISO format)
    start: int
    end: Union[int, str]
    title: str
    category: Optional[str]
    createTour: Optional[bool] = True
    group: str
    tourId: Optional[str] = None