from typing import List, Optional
from pydantic import BaseModel, model_validator
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


ALLOWED_IMAGE_TYPES = {
    "image/jpeg": {".jpg", ".jpeg"},
    "image/png": {".png"},
    "image/webp": {".webp"},
    "image/gif": {".gif"},
}
MAX_IMAGE_BYTES = 5 * 1024 * 1024


class ImageFileSpec(FileSpec):
    """FileSpec for image uploads: restricted content types, extension match, optional size cap."""
    size: Optional[int] = None  # bytes; when given it is validated and pinned in the presigned URL

    @model_validator(mode="after")
    def _check_image(self):
        ctype = (self.content_type or "").lower()
        if ctype not in ALLOWED_IMAGE_TYPES:
            raise ValueError(f"content_type must be one of {sorted(ALLOWED_IMAGE_TYPES)}")
        name = (self.file_name or "").lower()
        if not any(name.endswith(ext) for ext in ALLOWED_IMAGE_TYPES[ctype]):
            raise ValueError("file_name extension does not match content_type")
        if self.size is not None and not (0 < self.size <= MAX_IMAGE_BYTES):
            raise ValueError(f"size must be between 1 and {MAX_IMAGE_BYTES} bytes")
        return self


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
