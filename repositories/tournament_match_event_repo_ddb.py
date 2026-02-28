import os
from .ddb_session import tournament_match_event_table
from boto3.dynamodb.conditions import Key
from typing import Any


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


class TournamentMatchEventRepo:
    """DynamoDB-backed repository for the MatchEvent table."""

    def __init__(self):
        self._table = tournament_match_event_table()
        self._match_gsi = os.getenv("EVENT_MATCH_GSI", "match_index")

    def get(self, event_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": event_id})
        return resp.get("Item")

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def delete(self, event_id: str) -> None:
        self._table.delete_item(Key={"id": event_id})

    def list_by_match(self, match_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._match_gsi,
            KeyConditionExpression=Key("match_id").eq(match_id),
        )

    def update(self, event_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        if not updates:
            return self.get(event_id)

        parts, eav, ean = [], {}, {}
        for i, (field, value) in enumerate(updates.items(), start=1):
            nk = f"#n{i}"
            vk = f":v{i}"
            ean[nk] = field
            eav[vk] = value
            parts.append(f"{nk} = {vk}")

        resp = self._table.update_item(
            Key={"id": event_id},
            UpdateExpression="SET " + ", ".join(parts),
            ExpressionAttributeValues=eav,
            ExpressionAttributeNames=ean,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")
