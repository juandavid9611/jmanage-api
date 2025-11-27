import boto3
import os
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

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
        
        # Generate a simple order number (in a real app this might be more complex)
        order_number = f"#{int(datetime.now().timestamp())}"

        # Convert items prices to Decimal
        items = []
        for i in payload.get("items", []):
            item_data = i.model_dump() if hasattr(i, "model_dump") else i
            if "price" in item_data:
                item_data["price"] = to_decimal(item_data["price"])
            items.append(item_data)

        item = {
            "id": order_id,
            "account_id": account_id,
            "orderNumber": order_number,
            "createdAt": now,
            "taxes": to_decimal(payload.get("taxes", 0)),
            "items": items,
            "history": {
                "orderTime": now,
                "timeline": [
                    {"title": "Order placed", "time": now}
                ]
            },
            "subtotal": to_decimal(payload.get("subtotal", 0)),
            "shipping": to_decimal(payload.get("shipping", 0)),
            "discount": to_decimal(payload.get("discount", 0)),
            "customer": payload.get("customer", {}).model_dump() if hasattr(payload.get("customer"), "model_dump") else payload.get("customer", {}),
            "delivery": payload.get("delivery", {}).model_dump() if hasattr(payload.get("delivery"), "model_dump") else payload.get("delivery", {}),
            "totalAmount": to_decimal(payload.get("totalAmount", 0)),
            "totalQuantity": payload.get("totalQuantity", 0),
            "shippingAddress": payload.get("shippingAddress", {}).model_dump() if hasattr(payload.get("shippingAddress"), "model_dump") else payload.get("shippingAddress", {}),
            "payment": payload.get("payment", {}).model_dump() if hasattr(payload.get("payment"), "model_dump") else payload.get("payment", {}),
            "status": "pending"
        }
        
        self.table.put_item(Item=item)
        return item

    def list_all(self, account_id: str) -> List[Dict[str, Any]]:
        """List all orders for the specified account"""
        try:
            # Use GSI to query by account_id
            response = self.table.query(
                IndexName=self._account_gsi,
                KeyConditionExpression=Key("account_id").eq(account_id)
            )
            return response.get("Items", [])
        except Exception:
            # Fallback to scan with filter if GSI doesn't exist yet
            response = self.table.scan(
                FilterExpression=Key("account_id").eq(account_id)
            )
            return response.get("Items", [])

    def get_by_id(self, order_id: str, account_id: str) -> Optional[Dict[str, Any]]:
        """Get order by ID, validating it belongs to the account"""
        response = self.table.get_item(Key={"id": order_id})
        item = response.get("Item")
        
        # Validate account ownership
        if item and item.get("account_id") != account_id:
            return None
            
        return item

    def update(self, order_id: str, account_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update order, validating it belongs to the account"""
        current = self.get_by_id(order_id, account_id)
        if not current:
            return None
            
        # Merge payload into current
        # Note: This is a simplified update. For production, use UpdateExpression
        updated_item = {**current, **payload}
        
        # Ensure account_id cannot be changed
        updated_item["account_id"] = account_id
        
        # Ensure numeric fields are Decimal in the updated item
        numeric_fields = ["taxes", "subtotal", "shipping", "discount", "totalAmount"]
        for field in numeric_fields:
            if field in updated_item:
                updated_item[field] = to_decimal(updated_item[field])
                
        self.table.put_item(Item=updated_item)
        return updated_item

    def delete(self, order_id: str, account_id: str) -> bool:
        """Delete order, validating it belongs to the account"""
        # Verify ownership before deleting
        current = self.get_by_id(order_id, account_id)
        if not current:
            return False
            
        self.table.delete_item(Key={"id": order_id})
        return True
