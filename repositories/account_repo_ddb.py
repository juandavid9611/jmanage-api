from .ddb_session import account_table
from typing import Any


class AccountRepo:
    """DynamoDB-backed repository for account table"""
    def __init__(self):
        self._table = account_table()
    
    def get(self, account_id: str) -> dict[str, Any] | None:
        """Get account by ID"""
        resp = self._table.get_item(Key={"id": account_id})
        return resp.get("Item")
    
    def put(self, item: dict[str, Any]) -> None:
        """Create account"""
        self._table.put_item(Item=item)
    
    def update(self, account_id: str, updates: dict[str, Any]) -> None:
        """Update account"""
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
            Key={"id": account_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )
