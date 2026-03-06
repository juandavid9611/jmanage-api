from fastapi import APIRouter, Depends

from api.schemas.notifications import NotificationOut
from auth import get_current_user
from di import get_notification_repo
from repositories.notification_repo_ddb import NotificationRepo

router = APIRouter(prefix="/notifications", tags=["notifications"])


def _to_out(item: dict) -> NotificationOut:
    return NotificationOut(
        id=item["id"],
        title=item["title"],
        body=item.get("content", ""),
        isUnRead="read_at" not in item,
        createdAt=item["sent_at"],
        type=item.get("category", ""),
    )


@router.get("")
async def list_notifications(
    user: dict = Depends(get_current_user),
    repo: NotificationRepo = Depends(get_notification_repo),
):
    items = repo.list_by_user(user["email"])
    return {"notifications": [_to_out(item) for item in items]}


# NOTE: static route /read must be defined before /{notification_id}/read
@router.post("/read")
async def mark_all_notifications_read(
    user: dict = Depends(get_current_user),
    repo: NotificationRepo = Depends(get_notification_repo),
):
    repo.mark_all_read(user["email"])
    return {"ok": True}


@router.post("/{notification_id}/read")
async def mark_notification_read(
    notification_id: str,
    user: dict = Depends(get_current_user),
    repo: NotificationRepo = Depends(get_notification_repo),
):
    repo.mark_read(notification_id, user["email"])
    return {"ok": True}
