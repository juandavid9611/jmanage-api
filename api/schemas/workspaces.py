from pydantic import BaseModel


class CreateWorkspace(BaseModel):
    """Schema for creating a new workspace (id is auto-generated)"""
    name: str
    logo: str | None = None
    plan: str | None = None


class PutWorkspace(BaseModel):
    """Schema for updating a workspace (id cannot be changed)"""
    id: str | None = None  # Ignored in updates
    name: str | None = None
    logo: str | None = None
    plan: str | None = None