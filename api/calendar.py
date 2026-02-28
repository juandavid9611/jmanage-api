from fastapi import APIRouter, Depends, HTTPException, Query

from api.schemas.calendar import ParticipationRequest, PutCalendarEvent
from auth import PermissionChecker, WorkspacePermissionChecker, get_current_user, get_account_id
from di import get_calendar_service
from services.calendar_service import CalendarService
from services.tour_service import TourService


router = APIRouter(prefix="/calendar", tags=["calendar"])

@router.get("", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def get_calendar(
    workspace_id: str = Query(None), 
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
):
    items = svc.list_calendar_events(account_id, group=workspace_id)
    return items

@router.post(
    "",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin', 'coach']))]
)
async def create_calendar_event(
    put_calendar_event: PutCalendarEvent,
    workspace_id: str = Query(..., description="Workspace ID for this calendar event"),
    account_id: str = Depends(get_account_id),
    calendar_svc: CalendarService = Depends(get_calendar_service)
):
    """Create calendar event (requires workspace admin permission)"""
    # Set workspace in event data if not already set
    if not put_calendar_event.group:
        put_calendar_event.group = workspace_id
    elif put_calendar_event.group != workspace_id:
        raise HTTPException(
            status_code=400,
            detail="Event group must match workspace_id"
        )
    
    calendar_item = calendar_svc.create(put_calendar_event, account_id)
    return calendar_item

@router.put(
    "",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin', 'coach']))]
)
async def update_calendar_event(
    put_calendar_event: PutCalendarEvent,
    workspace_id: str = Query(..., description="Workspace ID that owns this event"),
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
):
    """Update calendar event (requires workspace admin permission)"""
    # Fetch and verify event exists
    existing_item = svc.get(put_calendar_event.id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Event {put_calendar_event.id} not found")
    
    # Verify event belongs to workspace
    if existing_item.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Event does not belong to workspace {workspace_id}"
        )
    
    svc.update(put_calendar_event.id, account_id, put_calendar_event)
    return {"updated_event_id": put_calendar_event.id}

@router.delete(
    "/{calendar_event_id}",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['admin', 'coach']))]
)
async def delete_calendar_event(
    calendar_event_id: str,
    workspace_id: str = Query(..., description="Workspace ID that owns this event"),
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
):
    """Delete calendar event (requires workspace admin permission)"""
    # Fetch and verify event exists
    event = svc.get(calendar_event_id, account_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {calendar_event_id} not found")
    
    # Verify event belongs to workspace
    if event.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Event does not belong to workspace {workspace_id}"
        )
    
    svc.delete(calendar_event_id, account_id)
    return {"deleted_event_id": calendar_event_id}

@router.post(
    "/{calendar_event_id}/participate",
    dependencies=[Depends(WorkspacePermissionChecker(required_permissions=['user', 'admin', 'coach']))]
)
async def participate_calendar_event(
    calendar_event_id: str,
    participate_data: ParticipationRequest,
    workspace_id: str = Query(..., description="Workspace ID that owns this event"),
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
) -> dict:
    """Participate in calendar event (requires workspace membership)"""
    # Fetch and verify event exists
    event = svc.get(calendar_event_id, account_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event {calendar_event_id} not found")
    
    # Verify event belongs to workspace
    if event.get("group") != workspace_id:
        raise HTTPException(
            status_code=400,
            detail=f"Event does not belong to workspace {workspace_id}"
        )
    
    result = svc.participate(calendar_event_id, account_id, user, participate_data)
    return {"participated_event_id": calendar_event_id}
