import os
from typing import Any

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from .ddb_session import bank_adjustment_table
from .ddb_transactions import query_all, tx_put


class AdjustmentExists(Exception):
    """An adjustment with that id already exists (idempotent retries)."""


class BankAdjustmentRepo:
    """Money that enters/leaves a bank account without being the payment of an expense.

    Adjustments created by payment requests have a deterministic id
    (`adj_pr_<paymentRequestId>_<submissionId>`), so a retry can never duplicate them.
    """

    def __init__(self):
        self._table = bank_adjustment_table()
        self._bank_gsi = os.getenv("BANK_ADJUSTMENT_BANK_GSI", "bank_account_id_index")

    @property
    def table(self):
        return self._table

    def get(self, adjustment_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._table.get_item(Key={"id": adjustment_id}).get("Item")
        return item if item and item.get("account_id") == account_id else None

    def list_by_bank_account(self, bank_account_id: str, account_id: str) -> list[dict[str, Any]]:
        items = query_all(
            self._table,
            IndexName=self._bank_gsi,
            KeyConditionExpression=Key("bank_account_id").eq(bank_account_id),
        )
        return [i for i in items if i.get("account_id") == account_id]

    def create(self, item: dict[str, Any]) -> dict[str, Any]:
        try:
            self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(id)")
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise AdjustmentExists(item["id"]) from exc
            raise
        return item

    def tx_create(self, item: dict[str, Any]) -> dict[str, Any]:
        """The same insert as a transaction item (use together with the payment-request update)."""
        return tx_put(self._table, item, condition="attribute_not_exists(id)")


def payment_request_adjustment_id(payment_request_id: str, submission_id: str) -> str:
    return f"adj_pr_{payment_request_id}_{submission_id}"
