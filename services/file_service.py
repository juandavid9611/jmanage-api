from repositories.file_repo_ddb import FileRepo
from typing import Dict, Any, Optional
from api.schemas.files import FileOut, FileCreate, FileUpdate, FileSpec
from repositories.s3_adapter import S3Adapter



class FileService:
    def __init__(self, repo: FileRepo, s3: S3Adapter):
        self.repo = repo
        self.s3 = s3

    def create_file(self, data: FileCreate, account_id: str) -> FileOut:
        """Create file metadata"""
        raw = self.repo.create(data.model_dump(by_alias=False), account_id)
        return self._map_file(raw, get_presigned_url=False)  # URL is None initially
    
    def list_files(self, account_id: str) -> list[FileOut]:
        """List all files for an account"""
        items = self.repo.list_all(account_id)
        return [self._map_file(it) for it in items]

    def get_file(self, file_id: str, account_id: str, get_presigned_url: bool = True) -> Optional[FileOut]:
        """Get aingle file by ID"""
        raw = self.repo.get_by_id(file_id, account_id)
        return self._map_file(raw, get_presigned_url) if raw else None

    def update_file(self, file_id: str, account_id: str, data: FileUpdate) -> Optional[FileOut]:
        """Update file metadata"""
        update_data = data.model_dump(exclude_none=True, by_alias=False)
        raw = self.repo.update(file_id, account_id, update_data)
        return self._map_file(raw) if raw else None

    def delete_file(self, file_id: str, account_id: str) -> bool:
        """Delete file from both DynamoDB and S3"""
        # Get file info first to get S3 key
        file_data = self.repo.get_by_id(file_id, account_id)
        if not file_data:
            return False
        
        # Delete from DynamoDB
        deleted = self.repo.delete(file_id, account_id)
        
        # Delete from S3 if it exists
        if deleted and file_data.get("url"):
            try:
                self.s3.delete_file(file_data["url"])
            except Exception as e:
                print(f"Warning: Failed to delete S3 file {file_data['url']}: {e}")
        
        return deleted
    
    def generate_put_presigned_url(self, file_id: str, account_id: str, file_spec: FileSpec) -> dict[str, str]:
        """Generate presigned URL for uploading file to S3"""
        file_data = self.get_file(file_id, account_id, get_presigned_url=False)
        if not file_data:
            raise ValueError(f"File {file_id} not found")
        
        if not isinstance(file_spec, FileSpec):
            raise TypeError("File must be a FileSpec instance.")
        
        file_name = file_spec.file_name
        file_content_type = file_spec.content_type
        if not file_name or not file_content_type:
            raise ValueError("File 'file_name' and 'content_type' cannot be empty.")
        
        result = self.s3.presign_file_put(
            account_id=account_id,
            file_id=file_id,
            filename=file_name,
            content_type=file_content_type
        )
        return result
    
    def add_file(self, file_id: str, account_id: str, file_name: str) -> str:
        """Add file key to file record after successful upload"""
        # Get file without URL conversion to work with raw S3 keys
        existing = self.get_file(file_id, account_id, get_presigned_url=False)
        if not existing:
            raise ValueError(f"File {file_id} not found")
        
        # Generate S3 key from filename
        key = self.s3._kb.file(account_id, file_id, file_name)
        
        # Update URL field with the key (single string, not array)
        self.repo.update(file_id, account_id, {"url": key})
        
        return key
    
    def _map_file(self, item: Dict[str, Any], get_presigned_url: bool = True) -> FileOut:
        """Map raw file data to FileOut schema with optional S3 URL conversion"""
        url = item.get("url")
        file_type = item.get("type", "")
        
        # Convert S3 key to presigned GET URL if requested and URL exists
        if get_presigned_url and url:
            # Map file type to content type
            content_type = self._get_content_type(file_type)
            url = self.s3.presign_get_from_explicit_key(key=url, content_type=content_type)
        
        return FileOut(
            id=item["id"],
            name=item.get("name", ""),
            url=url,
            tags=item.get("tags", []),
            size=item.get("size", 0),
            created_at=item.get("created_at"),
            modified_at=item.get("modified_at"),
            type=file_type,
            is_favorited=item.get("is_favorited", False),
        )
    
    def _get_content_type(self, file_type: str) -> str:
        """Map file type to MIME content type"""
        type_mapping = {
            "pdf": "application/pdf",
            "doc": "application/msword",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "xls": "application/vnd.ms-excel",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "ppt": "application/vnd.ms-powerpoint",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "txt": "text/plain",
            "csv": "text/csv",
            "json": "application/json",
            "xml": "application/xml",
            "zip": "application/zip",
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "gif": "image/gif",
            "svg": "image/svg+xml",
            "mp4": "video/mp4",
            "mp3": "audio/mpeg",
            "wav": "audio/wav",
        }
        return type_mapping.get(file_type.lower(), "application/octet-stream")
