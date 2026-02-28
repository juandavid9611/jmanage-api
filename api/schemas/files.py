from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
from core.casing import camel_alias


class CamelModel(BaseModel):
    model_config = {
        "alias_generator": camel_alias,
        "populate_by_name": True,
        "from_attributes": True,
    }


class FileSpec(BaseModel):
    file_name: str
    content_type: str


class FileOut(CamelModel):
    id: str
    name: str
    url: str | None  # S3 key, converted to presigned URL when returned
    tags: List[str]
    size: int
    created_at: datetime | str
    modified_at: datetime | str
    type: str
    is_favorited: bool


class FileCreate(CamelModel):
    name: str
    size: int
    type: str


class FileUpdate(CamelModel):
    name: Optional[str] = None
    tags: Optional[List[str]] = None
    is_favorited: Optional[bool] = None
