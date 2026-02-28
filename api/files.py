from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Body

from api.schemas.files import FileCreate, FileOut, FileUpdate, FileSpec
from auth import PermissionChecker, get_account_id
from di import get_file_service
from services.file_service import FileService


router = APIRouter(prefix="/files", tags=["files"])


@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def list_files(
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """List all files for the account"""
    return svc.list_files(account_id)


@router.post("", response_model=FileOut, dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def create_file(
    payload: FileCreate,
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """Create file metadata (URLs will be added after upload)"""
    file = svc.create_file(payload, account_id)
    return file


@router.get("/{file_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin", "user"]))])
async def get_file(
    file_id: str,
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """Get single file by ID"""
    file = svc.get_file(file_id, account_id)
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    return file


@router.put("/{file_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def update_file(
    file_id: str,
    payload: FileUpdate,
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """Update file metadata (name, tags, favorited)"""
    item = svc.update_file(file_id, account_id, payload)
    if not item:
        raise HTTPException(status_code=404, detail="File not found or not updated")
    return item


@router.delete("/{file_id}", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def delete_file(
    file_id: str,
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """Delete file from both DynamoDB and S3"""
    existing = svc.delete_file(file_id, account_id)
    if not existing:
        raise HTTPException(status_code=404, detail="File not found")
    return


@router.post("/{file_id}/generate-presigned-url", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def generate_file_presigned_url(
    file_id: str,
    file_spec: FileSpec = Body(...),
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """Generate presigned URL for uploading file to S3"""
    file = svc.get_file(file_id, account_id, get_presigned_url=False)
    if not file:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")
    
    try:
        result = svc.generate_put_presigned_url(file_id=file_id, account_id=account_id, file_spec=file_spec)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error generating presigned URL: {str(e)}")
    
    return result


@router.post("/{file_id}/add-file", dependencies=[Depends(PermissionChecker(required_permissions=["admin"]))])
async def add_file(
    file_id: str,
    file_name: str = Body(..., embed=True),
    account_id: str = Depends(get_account_id),
    svc: FileService = Depends(get_file_service)
):
    """Add file key to file record after successful upload"""
    try:
        key = svc.add_file(file_id, account_id, file_name)
        return {"key": key}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error adding file: {str(e)}")
