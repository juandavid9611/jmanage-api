from pydantic import BaseModel


class NotificationOut(BaseModel):
    id: str
    title: str
    body: str
    isUnRead: bool
    createdAt: int  # epoch ms
    type: str
