import os
from .ddb_session import tournament_player_table
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


class TournamentPlayerRepo:
    """DynamoDB-backed repository for the TournamentPlayer table."""

    def __init__(self):
        self._table = tournament_player_table()
        self._tournament_gsi = os.getenv("PLAYER_TOURNAMENT_GSI", "tournament_index")
        self._team_gsi = os.getenv("PLAYER_TEAM_GSI", "team_index")

    def get(self, player_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": player_id})
        return resp.get("Item")

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def delete(self, player_id: str) -> None:
        self._table.delete_item(Key={"id": player_id})

    def list_by_tournament(self, tournament_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._tournament_gsi,
            KeyConditionExpression=Key("tournament_id").eq(tournament_id),
        )

    def list_by_team(self, team_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._team_gsi,
            KeyConditionExpression=Key("team_id").eq(team_id),
        )

    def update(self, player_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        if not updates:
            return self.get(player_id)

        parts, eav, ean = [], {}, {}
        for i, (field, value) in enumerate(updates.items(), start=1):
            nk = f"#n{i}"
            vk = f":v{i}"
            ean[nk] = field
            eav[vk] = value
            parts.append(f"{nk} = {vk}")

        resp = self._table.update_item(
            Key={"id": player_id},
            UpdateExpression="SET " + ", ".join(parts),
            ExpressionAttributeValues=eav,
            ExpressionAttributeNames=ean,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")
