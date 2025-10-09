import os
from .ddb_session import tour_table
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

class TourRepo:
    """DynamoDB-backed repository for tour table. No business rules here."""
    def __init__(self):
        self._table = tour_table()
        self._user_gsi = os.getenv("TOUR_USER_GSI", "user_index")

    # ------------- Reads -------------
    def get(self, tour_id: str) -> Optional[Dict[str, Any]]:
        resp = self._table.get_item(Key={"id": tour_id})
        return resp.get("Item")

    def list_all(self) -> Iterable[Dict[str, Any]]:
        return _scan_all(self._table)

    def list_by_group(self, group: str) -> Iterable[Dict[str, Any]]:
        # TODO optimize with GSI if needed
        return _scan_all(
            self._table,
            FilterExpression=Attr("user_group").eq(group)
        )
    
    def list_by_type(self, tour_type: str) -> Iterable[Dict[str, Any]]:
        # TODO optimize with GSI if needed
        return _scan_all(
            self._table,
            FilterExpression=Attr("tour_type").eq(tour_type)
        )

    # ------------- Writes -------------
    def put(self, item: Dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def update(self, tour_id: str, updates: Dict[str, Any]) -> None:
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
            Key={"id": tour_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")

    def delete(self, tour_id: str) -> None:
        self._table.delete_item(Key={"id": tour_id})