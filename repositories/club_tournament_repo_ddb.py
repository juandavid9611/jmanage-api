from typing import Any

from boto3.dynamodb.conditions import Attr, Key

from .ddb_session import club_tournament_table


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


class ClubTournamentRepo:
    """DynamoDB-backed repository for the ClubTournament table."""

    def __init__(self):
        self._table = club_tournament_table()
        self._account_gsi = "account_id_index"

    def get(self, tournament_id: str, account_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": tournament_id})
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

    def update(self, tournament_id: str, account_id: str, updates: dict[str, Any]) -> None:
        if not updates:
            return
        self._table.update_item(
            Key={"id": tournament_id},
            UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in updates),
            ExpressionAttributeNames={f"#{k}": k for k in updates},
            ExpressionAttributeValues={f":{k}": v for k, v in updates.items()},
            ConditionExpression=Attr("account_id").eq(account_id),
        )

    def delete(self, tournament_id: str, account_id: str) -> None:
        self._table.delete_item(
            Key={"id": tournament_id},
            ConditionExpression=Attr("account_id").eq(account_id),
        )
