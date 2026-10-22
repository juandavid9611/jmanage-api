import os
from typing import Any

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from .bank_account_repo_ddb import ConditionFailed, conditional_update
from .ddb_session import expense_table
from .ddb_transactions import query_all, tx_update

_TENANT_GUARD = "attribute_exists(id) AND account_id = :tenant AND workspace_id = :ws"


class ExpenseRepo:
    """Expenses (egresos). Tenant column `account_id`; the bank account is stored as `bank_account_id`."""

    def __init__(self):
        self._table = expense_table()
        self._account_gsi = os.getenv("EXPENSE_ACCOUNT_GSI", "account_id_index")
        self.page_limit: int | None = None

    @property
    def table(self):
        return self._table

    def get(self, expense_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._table.get_item(Key={"id": expense_id}).get("Item")
        return item if item and item.get("account_id") == account_id else None

    def list_filtered(
        self,
        account_id: str,
        workspace_id: str,
        *,
        status: str | None = None,
        bank_account_id: str | None = None,
        team: str | None = None,
        category: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        """Server-side filters (FilterExpression over the paginated GSI query), expense_date desc then id."""
        cond = Attr("workspace_id").eq(workspace_id)
        for attr, value in (
            ("status", status),
            ("bank_account_id", bank_account_id),
            ("team", team),
            ("category", category),
        ):
            if value:
                cond = cond & Attr(attr).eq(value)
        if date_from:
            cond = cond & Attr("expense_date").gte(date_from)
        if date_to:
            cond = cond & Attr("expense_date").lte(date_to)
        kwargs: dict[str, Any] = {
            "IndexName": self._account_gsi,
            "KeyConditionExpression": Key("account_id").eq(account_id),
            "FilterExpression": cond,
        }
        if self.page_limit:
            kwargs["Limit"] = self.page_limit
        items = query_all(self._table, **kwargs)
        items.sort(key=lambda i: i["id"])
        items.sort(key=lambda i: i.get("expense_date", ""), reverse=True)
        return items

    def create(self, item: dict[str, Any]) -> dict[str, Any]:
        self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(id)")
        return item

    def update_guarded(
        self,
        expense_id: str,
        account_id: str,
        workspace_id: str,
        *,
        sets: dict[str, Any],
        removes: tuple[str, ...] = (),
        extra_condition: str = "",
        cond_names: dict[str, str] | None = None,
        cond_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Update guarded by tenant + workspace + `extra_condition`; ConditionFailed when any does not hold."""
        values = {":tenant": account_id, ":ws": workspace_id, **(cond_values or {})}
        condition = _TENANT_GUARD + (f" AND ({extra_condition})" if extra_condition else "")
        return conditional_update(
            self._table,
            {"id": expense_id},
            sets=sets,
            removes=removes,
            condition=condition,
            cond_names=cond_names,
            cond_values=values,
        )

    def delete_pending(self, expense_id: str, account_id: str, workspace_id: str) -> None:
        try:
            self._table.delete_item(
                Key={"id": expense_id},
                ConditionExpression=_TENANT_GUARD + " AND #st = :pendiente AND paid_amount = :zero",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":tenant": account_id, ":ws": workspace_id, ":pendiente": "pendiente", ":zero": 0,
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise ConditionFailed() from exc
            raise

    def tx_register_payment(
        self,
        expense: dict[str, Any],
        *,
        amount,
        bank_account_id: str,
        new_status: str,
        now: str,
        user_id: str,
    ) -> dict[str, Any]:
        """Transaction item: add the payment to the counter, only while approved and not over-paid."""
        max_paid = expense["total_amount"] - amount
        return tx_update(
            self._table,
            {"id": expense["id"]},
            "SET paid_amount = paid_amount + :a, bank_account_id = :bank, #st = :new, updated_at = :now, updated_by = :by",
            condition=(
                "account_id = :tenant AND workspace_id = :ws AND #st = :aprobado "
                "AND paid_amount <= :max AND total_amount = :total"
            ),
            names={"#st": "status"},
            values={
                ":a": amount, ":bank": bank_account_id, ":new": new_status, ":now": now, ":by": user_id,
                ":tenant": expense["account_id"], ":ws": expense["workspace_id"], ":aprobado": "aprobado",
                ":max": max_paid, ":total": expense["total_amount"],
            },
        )
