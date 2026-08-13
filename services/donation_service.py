from time import time
from typing import Any
from uuid import uuid4

from api.schemas.donations import ContributionCreate
from repositories.donation_repo_ddb import DonationRepo


class DonationService:
    def __init__(self, repo: DonationRepo):
        self.repo = repo

    def record_contribution(self, data: ContributionCreate, created_by_user_id: str) -> dict[str, Any]:
        item = {
            "id": str(uuid4()),
            "type": "contribution",
            "donor_name": data.donor_name,
            "anonymous": data.anonymous,
            "amount_cop": data.amount_cop,
            "message": data.message,
            "created_at": int(time()),
            "created_by_user_id": created_by_user_id,
        }
        self.repo.put_contribution(item)
        self.repo.increment_total(data.amount_cop)
        return self._map_contribution(item)

    def get_summary(self) -> dict[str, Any]:
        total = self.repo.get_total()
        if not total:
            return {"total_amount_cop": 0, "contribution_count": 0}
        return {
            "total_amount_cop": int(total["total_amount_cop"]),
            "contribution_count": int(total["contribution_count"]),
        }

    def list_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        return [self._map_contribution(item) for item in self.repo.list_contributions(limit)]

    def _map_contribution(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": item["id"],
            "donor_name": None if item.get("anonymous") else item.get("donor_name"),
            "amount_cop": int(item["amount_cop"]),
            "message": item.get("message"),
            "created_at": item["created_at"],
        }
