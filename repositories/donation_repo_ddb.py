from typing import Any

from .ddb_session import donation_table

_TOTAL_ID = "campaign_total_earthquake_2026"


class DonationRepo:
    def __init__(self):
        self._table = donation_table()

    def put_contribution(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def list_contributions(self, limit: int = 20) -> list[dict[str, Any]]:
        response = self._table.scan()
        items = [i for i in response.get("Items", []) if i.get("type") == "contribution"]
        while "LastEvaluatedKey" in response:
            response = self._table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(i for i in response.get("Items", []) if i.get("type") == "contribution")
        items.sort(key=lambda i: i["created_at"], reverse=True)
        return items[:limit]

    def get_total(self) -> dict[str, Any] | None:
        response = self._table.get_item(Key={"id": _TOTAL_ID})
        return response.get("Item")

    def increment_total(self, amount_cop: int) -> None:
        self._table.update_item(
            Key={"id": _TOTAL_ID},
            UpdateExpression="ADD total_amount_cop :amount, contribution_count :one",
            ExpressionAttributeValues={":amount": amount_cop, ":one": 1},
        )
