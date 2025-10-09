from typing import Optional
from pydantic import BaseModel


class PutWorkspace(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    logo: Optional[str] = None
    plan: Optional[str] = None