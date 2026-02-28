from pydantic import BaseModel


class CreateMembership(BaseModel):
    """Schema for creating a new membership"""
    user_id: str
    account_id: str
    role: str = "user"
    status: str = "active"
    workspace_id: str | None = None


class UpdateMembership(BaseModel):
    """Schema for updating a membership"""
    role: str | None = None
    status: str | None = None
    workspace_id: str | None = None
