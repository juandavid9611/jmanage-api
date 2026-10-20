"""Public Clubs API — read-only, unauthenticated, opt-in per account.

Returns sanitized club aggregates only (no person-level data). Every reason for
not serving the data (unknown account, opt-in off, not a club) is the same 404.
"""

import re

from fastapi import APIRouter, Depends, HTTPException

from di import get_public_club_service
from services.public_club_service import PublicClubService

router = APIRouter(prefix="/public/clubs", tags=["public"])

_ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@router.get("/{account_id}/overview")
async def get_public_club_overview(
    account_id: str,
    svc: PublicClubService = Depends(get_public_club_service),
):
    overview = svc.get_overview(account_id) if _ACCOUNT_ID_RE.match(account_id) else None
    if overview is None:
        raise HTTPException(status_code=404, detail="Not found")
    return overview
