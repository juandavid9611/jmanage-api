from fastapi import APIRouter, Depends, HTTPException, Query

from api.schemas.calendar import ParticipationRequest, PutCalendarEvent
from auth import PermissionChecker, get_current_user, get_account_id
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

@router.post("", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def create_calendar_event(
    put_calendar_event: PutCalendarEvent,
    account_id: str = Depends(get_account_id),
    calendar_svc: CalendarService = Depends(get_calendar_service),
    ):
    calendar_item = calendar_svc.create(put_calendar_event, account_id)

    return calendar_item

@router.put("", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def update_calendar_event(
    put_calendar_event: PutCalendarEvent, 
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
):
    existing_item = svc.get(put_calendar_event.id, account_id)
    if not existing_item:
        raise HTTPException(status_code=404, detail=f"Event {put_calendar_event.id} not found")
    
    svc.update(put_calendar_event.id, account_id, put_calendar_event)
    return {"updated_event_id": put_calendar_event.id}

@router.delete("/{calendar_event_id}", dependencies=[Depends(PermissionChecker(required_permissions=['admin']))])
async def delete_calendar_event(
    calendar_event_id: str, 
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
):
    svc.delete(calendar_event_id, account_id)
    
    return {"deleted_event_id": calendar_event_id}

@router.post("/{calendar_event_id}/participate", dependencies=[Depends(PermissionChecker(required_permissions=['admin', 'user']))])
async def participate_calendar_event(
    calendar_event_id: str, 
    participate_data: ParticipationRequest,
    user: dict = Depends(get_current_user),
    account_id: str = Depends(get_account_id),
    svc: CalendarService = Depends(get_calendar_service)
    ) -> dict:
    result = svc.participate(calendar_event_id, account_id, user, participate_data)
    if not result:
        raise HTTPException(status_code=404, detail=f"Event {calendar_event_id} not found")
    return {"participated_event_id": calendar_event_id}
