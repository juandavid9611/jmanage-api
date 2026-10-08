from typing import Any

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from .ddb_session import training_session_table


class StatusConflict(Exception):
    """The session is no longer in the status the caller expected."""


def _query_all(table, **kwargs) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    start_key = None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return items


class TrainingSessionRepo:
    """DynamoDB-backed repository for the TrainingSession table."""

    def __init__(self):
        self._table = training_session_table()
        self._account_gsi = "account_id_index"

    def get(self, session_id: str, account_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": session_id})
        item = resp.get("Item")
        if item and item.get("account_id") != account_id:
            return None
        return item

    def list_by_workspace(self, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._account_gsi,
            KeyConditionExpression=Key("account_id").eq(account_id),
            FilterExpression=Attr("workspace_id").eq(workspace_id),
        )

    def put(self, item: dict[str, Any]) -> None:
        if "account_id" not in item:
            raise ValueError("account_id is required")
        self._table.put_item(Item=item)

    def update(
        self,
        session_id: str,
        account_id: str,
        updates: dict[str, Any],
        expected_statuses: list[str] | None = None,
    ) -> None:
        """Set attributes; fails with StatusConflict if the session left `expected_statuses` meanwhile."""
        if not updates:
            return
        names = {f"#{k}": k for k in updates}
        values = {f":{k}": v for k, v in updates.items()}
        condition = Attr("account_id").eq(account_id)
        if expected_statuses:
            condition = condition & Attr("status").is_in(expected_statuses)
        try:
            self._table.update_item(
                Key={"id": session_id},
                UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in updates),
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
                ConditionExpression=condition,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise StatusConflict(session_id)
            raise

    def delete(self, session_id: str, account_id: str) -> None:
        self._table.delete_item(
            Key={"id": session_id},
            ConditionExpression=Attr("account_id").eq(account_id),
        )
