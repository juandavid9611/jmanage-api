import os
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Attr, Key

from decimal import Decimal
from .ddb_session import order_table


def to_decimal(value: Any) -> Decimal:
    """Convierte int/float/str/None a Decimal de forma segura."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


class OrderRepo:
    def __init__(self):
        self.table = order_table()
        self._account_gsi = os.getenv("ORDER_ACCOUNT_GSI", "account_id_index")

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def create(self, payload: Dict[str, Any], account_id: str) -> Dict[str, Any]:
        """Create a new order for the specified account"""
        order_id = str(uuid.uuid4())
        now = self._now_iso()
        order_number = f"#{int(datetime.now().timestamp())}"

        items = []
        for i in payload.get("items", []):
            item_data = i.model_dump() if hasattr(i, "model_dump") else i
            if "price" in item_data:
                item_data["price"] = to_decimal(item_data["price"])
            items.append(item_data)

        def _dump(value):
            return value.model_dump() if hasattr(value, "model_dump") else (value or {})

        item = {
            "id": order_id,
            "account_id": account_id,
            "workspace_id": payload.get("workspace_id"),
            "order_number": order_number,
            "created_at": now,
            "taxes": to_decimal(payload.get("taxes", 0)),
            "items": items,
            "history": [
                {"type": "order_created", "title": "Orden creada", "time": now},
            ],
            "subtotal": to_decimal(payload.get("subtotal", 0)),
            "shipping": to_decimal(payload.get("shipping", 0)),
            "discount": to_decimal(payload.get("discount", 0)),
            "customer": _dump(payload.get("customer")),
            "delivery": _dump(payload.get("delivery")),
            "total_amount": to_decimal(payload.get("total_amount", 0)),
            "total_quantity": payload.get("total_quantity", 0),
            "shipping_address": _dump(payload.get("shipping_address")),
            "payment": _dump(payload.get("payment")),
            "status": "pending",
            "payment_request_id": None,
        }

        self.table.put_item(Item=item)
        return item

    def list_all(self, account_id: str, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all orders for the specified account, optionally filtered by workspace_id"""
        try:
            kwargs: Dict[str, Any] = {
                "IndexName": self._account_gsi,
                "KeyConditionExpression": Key("account_id").eq(account_id),
            }
            if workspace_id:
                kwargs["FilterExpression"] = Key("workspace_id").eq(workspace_id)
            response = self.table.query(**kwargs)
            return response.get("Items", [])
        except Exception:
            kwargs = {"FilterExpression": Key("account_id").eq(account_id)}
            if workspace_id:
                kwargs["FilterExpression"] = kwargs["FilterExpression"] & Key("workspace_id").eq(workspace_id)
            response = self.table.scan(**kwargs)
            return response.get("Items", [])

    def get_by_id(self, order_id: str, account_id: str) -> Optional[Dict[str, Any]]:
        """Get order by ID, validating it belongs to the account"""
        response = self.table.get_item(Key={"id": order_id})
        item = response.get("Item")

        if item and item.get("account_id") != account_id:
            return None

        return item

    def update(self, order_id: str, account_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update order, validating it belongs to the account"""
        current = self.get_by_id(order_id, account_id)
        if not current:
            return None

        updated_item = {**current, **payload}
        updated_item["account_id"] = account_id

        numeric_fields = ["taxes", "subtotal", "shipping", "discount", "total_amount"]
        for field in numeric_fields:
            if field in updated_item:
                updated_item[field] = to_decimal(updated_item[field])

        self.table.put_item(Item=updated_item)
        return updated_item

    def set_payment_request_id(self, order_id: str, account_id: str, payment_request_id: str) -> Optional[Dict[str, Any]]:
        """Link a payment request to an existing order."""
        current = self.get_by_id(order_id, account_id)
        if not current:
            return None
        self.table.update_item(
            Key={"id": order_id},
            UpdateExpression="SET payment_request_id = :prid",
            ExpressionAttributeValues={":prid": payment_request_id},
        )
        current["payment_request_id"] = payment_request_id
        return current

    def append_event(
        self,
        order_id: str,
        account_id: str,
        event: Dict[str, Any],
    ) -> None:
        """Atomically append an event to order.history using DDB list_append."""
        self.table.update_item(
            Key={"id": order_id},
            UpdateExpression="SET history = list_append(if_not_exists(history, :empty), :evt)",
            ConditionExpression=Attr("account_id").eq(account_id),
            ExpressionAttributeValues={":empty": [], ":evt": [event]},
        )

    def delete(self, order_id: str, account_id: str) -> bool:
        """Delete order, validating it belongs to the account"""
        current = self.get_by_id(order_id, account_id)
        if not current:
            return False

        self.table.delete_item(Key={"id": order_id})
        return True
