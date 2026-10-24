import os
from typing import Any

from boto3.dynamodb.conditions import Key

from .ddb_session import expense_payment_table
from .ddb_transactions import query_all, tx_put


class ExpensePaymentRepo:
    """Ledger of the payments of expenses (one row per payment, never updated)."""

    def __init__(self):
        self._table = expense_payment_table()
        self._expense_gsi = os.getenv("EXPENSE_PAYMENT_EXPENSE_GSI", "expense_id_index")
        self._bank_gsi = os.getenv("EXPENSE_PAYMENT_BANK_GSI", "bank_account_id_index")
        self.page_limit: int | None = None

    @property
    def table(self):
        return self._table

    def get(self, payment_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._table.get_item(Key={"id": payment_id}).get("Item")
        return item if item and item.get("account_id") == account_id else None

    def _query(self, index: str, key_name: str, key_value: str, account_id: str) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "IndexName": index,
            "KeyConditionExpression": Key(key_name).eq(key_value),
        }
        if self.page_limit:
            kwargs["Limit"] = self.page_limit
        return [i for i in query_all(self._table, **kwargs) if i.get("account_id") == account_id]

    def list_by_expense(self, expense_id: str, account_id: str) -> list[dict[str, Any]]:
        return self._query(self._expense_gsi, "expense_id", expense_id, account_id)

    def list_by_bank_account(self, bank_account_id: str, account_id: str) -> list[dict[str, Any]]:
        return self._query(self._bank_gsi, "bank_account_id", bank_account_id, account_id)

    def tx_create(self, item: dict[str, Any]) -> dict[str, Any]:
        """Insert as a transaction item; an existing id (replay) fails the condition."""
        return tx_put(self._table, item, condition="attribute_not_exists(id)")
