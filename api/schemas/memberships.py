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


class UpdateMembershipRole(BaseModel):
    """Body for PATCH /memberships/{user_id}"""
    role: str


from typing import Literal
from pydantic import ConfigDict, Field, model_validator
from core.casing import camel_alias


class BulkMembershipRequest(BaseModel):
    """Body for POST /memberships/bulk (camelCase on the wire)."""
    model_config = ConfigDict(alias_generator=camel_alias, populate_by_name=True)

    user_ids: list[str] = Field(min_length=1, max_length=200)
    workspace_id: str = Field(min_length=1)
    role: Literal["admin", "user", "team_owner", "coach"] = "user"
    mode: Literal["add", "move", "remove"]
    from_workspace_id: str | None = None

    @model_validator(mode="after")
    def _check_move(self):
        if self.mode == "move":
            if not self.from_workspace_id:
                raise ValueError("fromWorkspaceId is required for mode 'move'")
            if self.from_workspace_id == self.workspace_id:
                raise ValueError("fromWorkspaceId must differ from workspaceId")
        return self
