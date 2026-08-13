from fastapi import APIRouter, Depends, Query

from api.schemas.donations import ContributionCreate, ContributionOut, DonationSummary
from auth import RequireAnyAccountAdmin, get_current_user
from di import get_donation_service
from services.donation_service import DonationService

router = APIRouter(prefix="/donations", tags=["donations"])


@router.get("/summary", response_model=DonationSummary)
async def get_donation_summary(
    svc: DonationService = Depends(get_donation_service),
):
    return svc.get_summary()


@router.get("/contributions", response_model=list[ContributionOut])
async def list_donation_contributions(
    limit: int = Query(20, ge=1, le=100),
    svc: DonationService = Depends(get_donation_service),
):
    return svc.list_recent(limit)


@router.post(
    "/contributions",
    response_model=ContributionOut,
    dependencies=[Depends(RequireAnyAccountAdmin())],
)
async def create_donation_contribution(
    contribution: ContributionCreate,
    user: dict = Depends(get_current_user),
    svc: DonationService = Depends(get_donation_service),
):
    return svc.record_contribution(contribution, created_by_user_id=user["sub"])
