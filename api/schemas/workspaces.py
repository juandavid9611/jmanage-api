from pydantic import BaseModel


class PutWorkspace(BaseModel):
    id: str | None = None
    name: str | None = None
    logo: str | None = None
    plan: str | None = None