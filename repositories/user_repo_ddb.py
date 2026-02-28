import os
from .ddb_session import user_table
from boto3.dynamodb.conditions import Attr
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

class UserRepo:
    """DynamoDB-backed repository for user table. No business rules here."""
    def __init__(self):
        self._table = user_table()
        self._group_gsi = os.getenv("USER_GROUP_GSI", "group_index")

    def get(self, user_id: str, account_id: str) -> dict[str, Any] | None:
        """Get user by ID
        
        Note: account_id parameter is kept for API compatibility but not used.
        Access control is enforced at the service/API layer via memberships.
        """
        resp = self._table.get_item(Key={"id": user_id})
        return resp.get("Item")

    def list_all(self, account_id: str) -> Iterable[dict[str, Any]]:
        """List all users
        
        Note: account_id parameter is kept for API compatibility but not used.
        Returns all users. Account filtering should be done via memberships at service layer.
        """
        return _scan_all(self._table)

    def list_by_group(self, group: str, account_id: str) -> Iterable[dict[str, Any]]:
        """List users by group
        
        Note: account_id parameter is kept for API compatibility but not used.
        Returns all users in the group. Account filtering should be done via memberships at service layer.
        """
        return _scan_all(
            self._table,
            FilterExpression=Attr("user_group").eq(group)
        )

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def update(self, user_id: str, account_id: str, updates: dict[str, Any]) -> None:
        """Update user
        
        Note: account_id parameter is kept for API compatibility but not used.
        Access control is enforced at the service/API layer via memberships.
        """
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
            return  # Nothing to update
        update_expression = "SET " + ", ".join(update_expr_parts)
        self._table.update_item(
            Key={"id": user_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )

    def delete(self, user_id: str, account_id: str) -> None:
        """Delete user
        
        Note: account_id parameter is kept for API compatibility but not used.
        Access control is enforced at the service/API layer via memberships.
        """
        self._table.delete_item(Key={"id": user_id})