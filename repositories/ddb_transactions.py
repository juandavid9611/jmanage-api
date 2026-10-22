"""Thin helpers around DynamoDB `transact_write_items` for the finance flows.

Money moves in transactions (a payment row + the counter it changes + the adjustment it creates)
so a crash or a double click can never leave half of it written. The resource's client accepts
native Python types (Decimal, str, list), same as `Table.put_item`.
"""
from typing import Any

from .ddb_session import dynamodb


class TransactionConflict(Exception):
    """A transaction was cancelled; `reasons` has one entry per item ('ConditionalCheckFailed', 'None', ...)."""

    def __init__(self, reasons: list[str], message: str = "transaction cancelled"):
        super().__init__(message)
        self.reasons = reasons

    @property
    def conditional_failed(self) -> list[int]:
        """Indexes of the items whose condition failed."""
        return [i for i, reason in enumerate(self.reasons) if reason == "ConditionalCheckFailed"]


def tx_put(table, item: dict[str, Any], *, condition: str | None = None,
           names: dict[str, str] | None = None, values: dict[str, Any] | None = None) -> dict[str, Any]:
    op: dict[str, Any] = {"TableName": table.name, "Item": item}
    if condition:
        op["ConditionExpression"] = condition
    if names:
        op["ExpressionAttributeNames"] = names
    if values:
        op["ExpressionAttributeValues"] = values
    return {"Put": op}


def tx_update(table, key: dict[str, Any], update: str, *, condition: str | None = None,
              names: dict[str, str] | None = None, values: dict[str, Any] | None = None) -> dict[str, Any]:
    op: dict[str, Any] = {"TableName": table.name, "Key": key, "UpdateExpression": update}
    if condition:
        op["ConditionExpression"] = condition
    if names:
        op["ExpressionAttributeNames"] = names
    if values:
        op["ExpressionAttributeValues"] = values
    return {"Update": op}


def transact_write(items: list[dict[str, Any]]) -> None:
    """All-or-nothing write. Raises TransactionConflict when any condition fails."""
    client = dynamodb.meta.client
    try:
        client.transact_write_items(TransactItems=items)
    except client.exceptions.TransactionCanceledException as exc:
        reasons = [r.get("Code", "None") for r in exc.response.get("CancellationReasons", [])]
        raise TransactionConflict(reasons) from exc


def query_all(table, **kwargs) -> list[dict[str, Any]]:
    """Every item of a query, following LastEvaluatedKey (one page is at most 1 MB)."""
    items: list[dict[str, Any]] = []
    start_key = None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            return items
