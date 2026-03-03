import os
from .ddb_session import tournament_team_table
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


class TournamentTeamRepo:
    """DynamoDB-backed repository for the TournamentTeam table."""

    def __init__(self):
        self._table = tournament_team_table()
        self._tournament_gsi = os.getenv("TEAM_TOURNAMENT_GSI", "tournament_index")
        self._group_gsi = os.getenv("TEAM_GROUP_GSI", "group_index")

    def get(self, team_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": team_id})
        return resp.get("Item")

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def delete(self, team_id: str) -> None:
        self._table.delete_item(Key={"id": team_id})

    def list_by_tournament(self, tournament_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._tournament_gsi,
            KeyConditionExpression=Key("tournament_id").eq(tournament_id),
        )

    def list_by_group(self, group_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._group_gsi,
            KeyConditionExpression=Key("group_id").eq(group_id),
        )

    def clear_group(self, team_id: str) -> None:
        """Remove the group_id attribute so the group_index GSI entry is deleted."""
        self._table.update_item(
            Key={"id": team_id},
            UpdateExpression="REMOVE #g",
            ExpressionAttributeNames={"#g": "group_id"},
        )

    def update(self, team_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        if not updates:
            return self.get(team_id)

        parts, eav, ean = [], {}, {}
        for i, (field, value) in enumerate(updates.items(), start=1):
            nk = f"#n{i}"
            vk = f":v{i}"
            ean[nk] = field
            eav[vk] = value
            parts.append(f"{nk} = {vk}")

        resp = self._table.update_item(
            Key={"id": team_id},
            UpdateExpression="SET " + ", ".join(parts),
            ExpressionAttributeValues=eav,
            ExpressionAttributeNames=ean,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")
