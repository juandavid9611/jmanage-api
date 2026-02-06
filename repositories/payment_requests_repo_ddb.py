import os
from .ddb_session import payment_request_table
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
            resp = self._table.query(
                IndexName=self._account_gsi,
                KeyConditionExpression=Key("account_id").eq(account_id)
            )
            return resp.get("Items", [])
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
        try:
            resp = self._table.query(
                IndexName=self._status_gsi,
                KeyConditionExpression=Key("payment_status").eq(status),
                FilterExpression=Attr("account_id").eq(account_id)
            )
            return resp.get("Items", [])
        except Exception:
            return [i for i in self.list_all(account_id) if i.get("payment_status") == status]


    # ------------- Writes -------------
    def put(self, item: dict[str, Any]) -> None:
        """Put payment request item (account_id must be in item)"""
        if "account_id" not in item:
            raise ValueError("account_id is required")
        self._table.put_item(Item=item)

    def update(self, payment_request_id: str, account_id: str, updates: dict[str, Any]) -> None:
        """Update payment request, validating it belongs to the account"""
        # Verify ownership
        current = self.get(payment_request_id, account_id)
        if not current:
            raise ValueError(f"Payment request {payment_request_id} not found in account {account_id}")
        
        # Prevent account_id from being changed
        if "account_id" in updates:
            del updates["account_id"]
        
        update_expr_parts = []
        expr_attr_values = {}
        expr_attr_names = {}
        for field, value in updates.items():
            placeholder = f":{field}"
            name_placeholder = f"#{field}"
            update_expr_parts.append(f"{name_placeholder} = {placeholder}")
            expr_attr_values[placeholder] = value
            expr_attr_names[name_placeholder] = field
        if not update_expr_parts:
            return
        update_expression = "SET " + ", ".join(update_expr_parts)
        self._table.update_item(
            Key={"id": payment_request_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )

    def delete(self, payment_request_id: str, account_id: str) -> None:
        """Delete payment request, validating it belongs to the account"""
        # Verify ownership before deleting
        current = self.get(payment_request_id, account_id)
        if not current:
            raise ValueError(f"Payment request {payment_request_id} not found in account {account_id}")
        self._table.delete_item(Key={"id": payment_request_id})