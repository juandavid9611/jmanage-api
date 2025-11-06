from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, Iterable

from boto3.dynamodb.conditions import Attr

from .ddb_session import product_table


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


def _convert_for_dynamodb(value: Any) -> Any:
    """Recursively convert floats to Decimal for DynamoDB compatibility."""

    if isinstance(value, float):
        # DynamoDB does not accept float types; convert to Decimal preserving precision.
        return Decimal(str(value))

    if isinstance(value, Mapping):
        return {k: _convert_for_dynamodb(v) for k, v in value.items()}

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_convert_for_dynamodb(v) for v in value]

    return value


class ProductRepo:
    """DynamoDB-backed repository for product table."""

    def __init__(self):
        self._table = product_table()

    def get(self, product_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": product_id})
        return resp.get("Item")

    def list_all(self) -> Iterable[dict[str, Any]]:
        return _scan_all(self._table)

    def list_by_category(self, category: str) -> Iterable[dict[str, Any]]:
        return _scan_all(
            self._table,
            FilterExpression=Attr("category").eq(category),
        )

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=_convert_for_dynamodb(item))

    def update(self, product_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        if not updates:
            return self.get(product_id)

        update_expr_parts = []
        expr_attr_values: dict[str, Any] = {}
        expr_attr_names: dict[str, str] = {}

        for field, value in updates.items():
            placeholder = f":{field}"
            name_placeholder = f"#{field}"
            update_expr_parts.append(f"{name_placeholder} = {placeholder}")
            expr_attr_values[placeholder] = _convert_for_dynamodb(value)
            expr_attr_names[name_placeholder] = field

        update_expression = "SET " + ", ".join(update_expr_parts)
        resp = self._table.update_item(
            Key={"id": product_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")

    def delete(self, product_id: str) -> None:
        self._table.delete_item(Key={"id": product_id})
