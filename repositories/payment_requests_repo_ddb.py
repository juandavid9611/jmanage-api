import os
from botocore.exceptions import ClientError
from .ddb_session import payment_request_table
from .ddb_transactions import tx_update
from boto3.dynamodb.conditions import Key, Attr
from typing import Iterable, Any

def _scan_all(table, **kwargs) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    start_key = None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return items

class PaymentRequestsRepo:
    """DynamoDB-backed repository for payment requests table. No business rules here."""
    def __init__(self):
        self._table = payment_request_table()
        self._user_gsi = os.getenv("PAYMENT_REQUEST_USER_GSI", "user_index")
        self._status_gsi = os.getenv("PAYMENT_REQUEST_STATUS_GSI", "status_index")
        self._account_gsi = os.getenv("PAYMENT_REQUEST_ACCOUNT_GSI", "account_id_index")

    def get(self, payment_request_id: str, account_id: str) -> dict[str, Any] | None:
        """Get payment request by ID, validating it belongs to the account"""
        resp = self._table.get_item(Key={"id": payment_request_id})
        item = resp.get("Item")
        
        # Validate account ownership
        if item and item.get("account_id") != account_id:
            return None
            
        return item

    def list_all(self, account_id: str) -> Iterable[dict[str, Any]]:
        """List all payment requests for the specified account"""
        try:
            # A query returns at most 1 MB per page: follow LastEvaluatedKey or large
            # accounts are silently truncated.
            items: list[dict[str, Any]] = []
            kwargs: dict[str, Any] = {
                "IndexName": self._account_gsi,
                "KeyConditionExpression": Key("account_id").eq(account_id),
            }
            while True:
                resp = self._table.query(**kwargs)
                items.extend(resp.get("Items", []))
                last = resp.get("LastEvaluatedKey")
                if not last:
                    return items
                kwargs["ExclusiveStartKey"] = last
        except Exception:
            # Fallback to scan with filter
            return _scan_all(
                self._table,
                FilterExpression=Attr("account_id").eq(account_id)
            )

    def list_filtered(self, account_id: str, user_id: str | None = None, group: str | None = None) -> Iterable[dict[str, Any]]:
        """List payment requests with optional filters within the specified account"""
        filter_expr = Attr("account_id").eq(account_id)
        if user_id:
            filter_expr = filter_expr & Attr("user_id").eq(user_id)
        if group:
            filter_expr = filter_expr & Attr("user_group").eq(group)
        
        return _scan_all(
            self._table,
            FilterExpression=filter_expr
        )

    
    def list_by_status(self, status: str, account_id: str) -> Iterable[dict[str, Any]]:
        """List payment requests by status within the specified account"""
        status = getattr(status, "value", status)
        return [i for i in self.list_by_status_all_accounts(status) if i.get("account_id") == account_id]

    def list_by_status_all_accounts(self, status: str) -> list[dict[str, Any]]:
        """All payment requests in `status`, across accounts (status GSI, paginated)."""
        status = getattr(status, "value", status)
        try:
            items: list[dict[str, Any]] = []
            kwargs: dict[str, Any] = {
                "IndexName": self._status_gsi,
                "KeyConditionExpression": Key("payment_status").eq(status),
            }
            while True:
                resp = self._table.query(**kwargs)
                items.extend(resp.get("Items", []))
                last = resp.get("LastEvaluatedKey")
                if not last:
                    return items
                kwargs["ExclusiveStartKey"] = last
        except Exception:
            return _scan_all(self._table, FilterExpression=Attr("payment_status").eq(status))


    # ------------- Writes -------------
    def put(self, item: dict[str, Any]) -> None:
        """Put payment request item (account_id must be in item)"""
        if "account_id" not in item:
            raise ValueError("account_id is required")
        self._table.put_item(Item=item)

    @staticmethod
    def _build_update(
        updates: dict[str, Any], remove: Iterable[str] = (), raw_sets: Iterable[str] = ()
    ) -> tuple[str, dict[str, str], dict[str, Any]]:
        names: dict[str, str] = {}
        values: dict[str, Any] = {}
        sets = list(raw_sets)
        for field, value in updates.items():
            value = getattr(value, "value", value)  # str-enums are stored as plain strings
            names[f"#{field}"] = field
            values[f":{field}"] = value
            sets.append(f"#{field} = :{field}")
        removes = []
        for field in remove:
            names[f"#{field}"] = field
            removes.append(f"#{field}")
        parts = []
        if sets:
            parts.append("SET " + ", ".join(sets))
        if removes:
            parts.append("REMOVE " + ", ".join(removes))
        return " ".join(parts), names, values

    def update(
        self,
        payment_request_id: str,
        account_id: str,
        updates: dict[str, Any],
        *,
        expected_status: str | None = None,
        remove: Iterable[str] = (),
    ) -> bool:
        """Update payment request, validating it belongs to the account.

        With `expected_status` the write only happens while the item still has that status
        (returns False when it changed meanwhile). `remove` drops attributes.
        """
        # Verify ownership
        current = self.get(payment_request_id, account_id)
        if not current:
            raise ValueError(f"Payment request {payment_request_id} not found in account {account_id}")

        # Prevent account_id from being changed
        updates = {k: v for k, v in updates.items() if k != "account_id"}
        expr, names, values = self._build_update(updates, remove)
        if not expr:
            return True
        kwargs: dict[str, Any] = {}
        if expected_status is not None:
            names["#__status"] = "payment_status"
            values[":__expected"] = getattr(expected_status, "value", expected_status)
            kwargs["ConditionExpression"] = "#__status = :__expected"
        if values:
            kwargs["ExpressionAttributeValues"] = values
        try:
            self._table.update_item(
                Key={"id": payment_request_id},
                UpdateExpression=expr,
                ExpressionAttributeNames=names,
                ReturnValues="ALL_NEW",
                **kwargs,
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise
        return True

    def tx_update(
        self,
        payment_request_id: str,
        updates: dict[str, Any],
        *,
        remove: Iterable[str] = (),
        raw_sets: Iterable[str] = (),
        condition: str | None = None,
        names: dict[str, str] | None = None,
        values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """The same update as a transaction item. `raw_sets` are extra SET clauses
        (e.g. `images = list_append(...)`); `names`/`values` carry their placeholders and the condition's."""
        expr, set_names, set_values = self._build_update(updates, remove, raw_sets)
        return tx_update(
            self._table,
            {"id": payment_request_id},
            expr,
            condition=condition,
            names={**set_names, **(names or {})} or None,
            values={**set_values, **(values or {})} or None,
        )

    def delete(self, payment_request_id: str, account_id: str) -> None:
        """Delete payment request, validating it belongs to the account"""
        # Verify ownership before deleting
        current = self.get(payment_request_id, account_id)
        if not current:
            raise ValueError(f"Payment request {payment_request_id} not found in account {account_id}")
        self._table.delete_item(Key={"id": payment_request_id})