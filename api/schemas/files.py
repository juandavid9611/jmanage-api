from pydantic import BaseModel


class FileSpec(BaseModel):
    file_name: str
    content_type: str
