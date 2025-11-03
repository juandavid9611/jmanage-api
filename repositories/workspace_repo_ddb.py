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

    def get(self, workspace_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": workspace_id})
        return resp.get("Item")

    def list_all(self) -> Iterable[dict[str, Any]]:
        return _scan_all(self._table)

    def list_by_group(self, group: str) -> Iterable[dict[str, Any]]:
        # TODO optimize with GSI if needed
        return _scan_all(
            self._table,
            FilterExpression=Attr("user_group").eq(group)
        )

    def list_by_type(self, workspace_type: str) -> Iterable[dict[str, Any]]:
        # TODO optimize with GSI if needed
        return _scan_all(
            self._table,
            FilterExpression=Attr("workspace_type").eq(workspace_type)
        )

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def update(self, workspace_id: str, updates: dict[str, Any]) -> None:
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

    def delete(self, workspace_id: str) -> None:
        self._table.delete_item(Key={"id": workspace_id})