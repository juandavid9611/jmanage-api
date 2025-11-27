import os
from .ddb_session import workspace_table
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

class WorkspaceRepo:
    """DynamoDB-backed repository for workspace table. No business rules here."""
    def __init__(self):
        self._table = workspace_table()
        self._user_gsi = os.getenv("WORKSPACE_USER_GSI", "user_index")
        self._account_gsi = os.getenv("WORKSPACE_ACCOUNT_GSI", "account_id_index")

    def get(self, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        """Get workspace by ID, validating it belongs to the account"""
        resp = self._table.get_item(Key={"id": workspace_id})
        item = resp.get("Item")
        
        # Validate account ownership
        if item and item.get("account_id") != account_id:
            return None
            
        return item

    def list_all(self, account_id: str) -> Iterable[dict[str, Any]]:
        """List all workspaces for the specified account"""
        try:
            from boto3.dynamodb.conditions import Key
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

    def list_by_group(self, group: str, account_id: str) -> Iterable[dict[str, Any]]:
        """List workspaces by group within the specified account"""
        return _scan_all(
            self._table,
            FilterExpression=Attr("user_group").eq(group) & Attr("account_id").eq(account_id)
        )

    def list_by_type(self, workspace_type: str, account_id: str) -> Iterable[dict[str, Any]]:
        """List workspaces by type within the specified account"""
        return _scan_all(
            self._table,
            FilterExpression=Attr("workspace_type").eq(workspace_type) & Attr("account_id").eq(account_id)
        )

    def put(self, item: dict[str, Any]) -> None:
        """Put workspace item (account_id must be in item)"""
        if "account_id" not in item:
            raise ValueError("account_id is required")
        self._table.put_item(Item=item)

    def update(self, workspace_id: str, account_id: str, updates: dict[str, Any]) -> None:
        """Update workspace, validating it belongs to the account"""
        # Verify ownership
        current = self.get(workspace_id, account_id)
        if not current:
            raise ValueError(f"Workspace {workspace_id} not found in account {account_id}")
        
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
            return  # Nothing to update
        update_expression = "SET " + ", ".join(update_expr_parts)
        resp = self._table.update_item(
            Key={"id": workspace_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")

    def delete(self, workspace_id: str, account_id: str) -> None:
        """Delete workspace, validating it belongs to the account"""
        # Verify ownership before deleting
        current = self.get(workspace_id, account_id)
        if not current:
            raise ValueError(f"Workspace {workspace_id} not found in account {account_id}")
        self._table.delete_item(Key={"id": workspace_id})