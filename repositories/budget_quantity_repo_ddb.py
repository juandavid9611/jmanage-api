from typing import Any

from boto3.dynamodb.conditions import Attr, Key

from .ddb_session import budget_quantity_table
from .ddb_transactions import query_all

ACCOUNT_MONTH_GSI = "account_month_index"


def quantity_id(item_id: str, month: str) -> str:
    return f"bgq_{item_id}_{month}"


class BudgetQuantityRepo:
    """DynamoDB repository for BudgetQuantity rows (id = bgq_<item_id>_<YYYY-MM>)."""

    def __init__(self):
        self._table = budget_quantity_table()

    def list_for_month(self, account_id: str, workspace_id: str, month: str) -> list[dict[str, Any]]:
        return query_all(
            self._table,
            IndexName=ACCOUNT_MONTH_GSI,
            KeyConditionExpression=Key("account_id").eq(account_id) & Key("month").eq(month),
            FilterExpression=Attr("workspace_id").eq(workspace_id),
        )

    def apply(self, puts: list[dict[str, Any]], delete_ids: list[str]) -> None:
        """Upsert `puts` and delete `delete_ids` with batch writes (the writer resends UnprocessedItems).

        Every row is a full overwrite or a delete by key, so replaying the same call is harmless.
        """
        with self._table.batch_writer() as batch:
            for row in puts:
                batch.put_item(Item=row)
            for row_id in delete_ids:
                batch.delete_item(Key={"id": row_id})
