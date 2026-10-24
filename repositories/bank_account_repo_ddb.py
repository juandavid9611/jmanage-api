import os
from typing import Any

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from .ddb_session import bank_account_table
from .ddb_transactions import query_all


class ConditionFailed(Exception):
    """A conditional write did not match (missing item, other tenant/workspace, or a state guard)."""


def conditional_update(
    table,
    key: dict[str, Any],
    *,
    sets: dict[str, Any],
    removes: tuple[str, ...] = (),
    condition: str,
    cond_names: dict[str, str] | None = None,
    cond_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """UpdateItem with SET/REMOVE built from plain dicts and a mandatory condition. Returns the new item."""
    names: dict[str, str] = dict(cond_names or {})
    values: dict[str, Any] = dict(cond_values or {})
    set_parts = []
    for i, (attr, value) in enumerate(sets.items()):
        names[f"#s{i}"] = attr
        values[f":s{i}"] = value
        set_parts.append(f"#s{i} = :s{i}")
    expr = "SET " + ", ".join(set_parts)
    if removes:
        remove_parts = []
        for i, attr in enumerate(removes):
            names[f"#r{i}"] = attr
            remove_parts.append(f"#r{i}")
        expr += " REMOVE " + ", ".join(remove_parts)
    try:
        resp = table.update_item(
            Key=key,
            UpdateExpression=expr,
            ConditionExpression=condition,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise ConditionFailed() from exc
        raise
    return resp["Attributes"]


class BankAccountRepo:
    """Bank / cash accounts of the club. Tenant column `account_id`; reads return None on a mismatch."""

    def __init__(self):
        self._table = bank_account_table()
        self._account_gsi = os.getenv("BANK_ACCOUNT_ACCOUNT_GSI", "account_id_index")
        self.page_limit: int | None = None  # tests force small pages to exercise pagination

    @property
    def table(self):
        return self._table

    def get(self, bank_account_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._table.get_item(Key={"id": bank_account_id}).get("Item")
        return item if item and item.get("account_id") == account_id else None

    def list_by_account(self, account_id: str) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "IndexName": self._account_gsi,
            "KeyConditionExpression": Key("account_id").eq(account_id),
        }
        if self.page_limit:
            kwargs["Limit"] = self.page_limit
        return query_all(self._table, **kwargs)

    def create(self, item: dict[str, Any]) -> dict[str, Any]:
        self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(id)")
        return item

    def update(self, bank_account_id: str, account_id: str, workspace_id: str, sets: dict[str, Any]) -> dict[str, Any]:
        return conditional_update(
            self._table,
            {"id": bank_account_id},
            sets=sets,
            condition="attribute_exists(id) AND account_id = :tenant AND workspace_id = :ws",
            cond_values={":tenant": account_id, ":ws": workspace_id},
        )
