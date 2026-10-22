import os
from typing import Any

from boto3.dynamodb.conditions import Key

from .ddb_session import payment_submission_table
from .ddb_transactions import query_all, tx_put, tx_update


class PaymentSubmissionRepo:
    """One item per installment (abono) of a payment request. No business rules here.

    Items: id, account_id (tenant), payment_request_id, user_id, amount, file_names, images (S3 keys),
    bank_account_id (nullable), status (pending_review|approved|rejected), source (player|manual|legacy),
    submitted_at, reviewed_by, reviewed_at.
    """

    def __init__(self):
        self._table_obj = None
        self._pr_gsi = os.getenv("PAYMENT_SUBMISSION_PR_GSI", "payment_request_id_index")

    @property
    def table(self):
        # Built on first use so the scheduler/other lambdas importing di do not need the table env.
        if self._table_obj is None:
            self._table_obj = payment_submission_table()
        return self._table_obj

    def get(self, submission_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.table.get_item(Key={"id": submission_id}).get("Item")
        return item if item and item.get("account_id") == account_id else None

    def list_by_payment_request(self, payment_request_id: str, account_id: str) -> list[dict[str, Any]]:
        items = query_all(
            self.table,
            IndexName=self._pr_gsi,
            KeyConditionExpression=Key("payment_request_id").eq(payment_request_id),
        )
        items = [i for i in items if i.get("account_id") == account_id]
        return sorted(items, key=lambda i: (i.get("submitted_at") or "", i["id"]))

    # ---- transaction items (use with ddb_transactions.transact_write) ----
    def tx_create(self, item: dict[str, Any]) -> dict[str, Any]:
        return tx_put(self.table, item, condition="attribute_not_exists(id)")

    def tx_review(self, submission_id: str, *, status: str, reviewed_by: str, reviewed_at: str,
                  bank_account_id: str | None = None) -> dict[str, Any]:
        """pending_review -> approved|rejected (fails when it was already reviewed)."""
        names = {"#st": "status"}
        values: dict[str, Any] = {
            ":new": status, ":pending": "pending_review", ":by": reviewed_by, ":at": reviewed_at,
        }
        update = "SET #st = :new, reviewed_by = :by, reviewed_at = :at"
        if bank_account_id:
            update += ", bank_account_id = :bank"
            values[":bank"] = bank_account_id
        return tx_update(self.table, {"id": submission_id}, update, condition="#st = :pending",
                         names=names, values=values)
