from typing import Optional

from pydantic import BaseModel, StringConstraints
from typing_extensions import Annotated

from core.casing import camel_alias

WorkspaceName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=80)]


class CreateWorkspace(BaseModel):
    """Schema for creating a new workspace (id is auto-generated). camelCase on the wire."""
    model_config = {"alias_generator": camel_alias, "populate_by_name": True}

    name: WorkspaceName
    logo: Optional[str] = None


class PutWorkspace(BaseModel):
    """Schema for updating a workspace (id cannot be changed)"""
    id: str | None = None  # Ignored in updates
    name: str | None = None
    logo: str | None = None
    plan: str | None = None
