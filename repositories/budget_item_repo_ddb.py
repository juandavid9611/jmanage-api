from typing import Any

from boto3.dynamodb.conditions import Attr, Key

from .ddb_session import budget_item_table
from .ddb_transactions import query_all

ACCOUNT_GSI = "account_id_index"


class BudgetItemRepo:
    """DynamoDB repository for the BudgetItem catalog (one item per row, partition key `id`)."""

    def __init__(self):
        self._table = budget_item_table()

    def get(self, item_id: str, account_id: str) -> dict[str, Any] | None:
        """The item, or None when it does not exist or belongs to another account (tenant check)."""
        item = self._table.get_item(Key={"id": item_id}).get("Item")
        if item and item.get("account_id") != account_id:
            return None
        return item

    def get_for_workspace(self, item_id: str, account_id: str, workspace_id: str) -> dict[str, Any] | None:
        item = self.get(item_id, account_id)
        if item and item.get("workspace_id") != workspace_id:
            return None
        return item

    def list_by_workspace(self, account_id: str, workspace_id: str) -> list[dict[str, Any]]:
        """Every item (active and inactive) of the workspace, following pagination."""
        return query_all(
            self._table,
            IndexName=ACCOUNT_GSI,
            KeyConditionExpression=Key("account_id").eq(account_id),
            FilterExpression=Attr("workspace_id").eq(workspace_id),
        )

    def create(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(id)")

    def replace(self, item: dict[str, Any]) -> None:
        """Overwrite an existing item of the same account/workspace (fails if it vanished or moved)."""
        self._table.put_item(
            Item=item,
            ConditionExpression="account_id = :a AND workspace_id = :w",
            ExpressionAttributeValues={":a": item["account_id"], ":w": item["workspace_id"]},
        )
