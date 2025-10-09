import os
from .ddb_session import calendar_table
from boto3.dynamodb.conditions import Attr
from typing import Optional, Iterable, Dict, Any, List

def _scan_all(table, **kwargs) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
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

class CalendarRepo:
    """DynamoDB-backed repository for calendar table. No business rules here."""
    def __init__(self):
        self._table = calendar_table()
        self._user_gsi = os.getenv("CALENDAR_USER_GSI", "user_index")

    # ------------- Reads -------------
    def get(self, calendar_event_id: str) -> Optional[Dict[str, Any]]:
        resp = self._table.get_item(Key={"id": calendar_event_id})
        return resp.get("Item")
    
    def list_all(self) -> Iterable[Dict[str, Any]]:
        return _scan_all(self._table)

    def list_by_group(self, group: str) -> Iterable[Dict[str, Any]]:
        # TODO optimize with GSI if needed
        return _scan_all(
            self._table,
            FilterExpression=Attr("user_group").eq(group)
        )

    # ------------- Writes -------------
    def put(self, item: Dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def update(self, calendar_event_id: str, updates: Dict[str, Any]) -> None:
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
            Key={"id": calendar_event_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )

    def delete(self, calendar_event_id: str) -> None:
        self._table.delete_item(Key={"id": calendar_event_id})