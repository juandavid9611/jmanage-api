import os
from .ddb_session import tournament_match_table
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


class TournamentMatchRepo:
    """DynamoDB-backed repository for the Match table."""

    def __init__(self):
        self._table = tournament_match_table()
        self._tournament_gsi = os.getenv("MATCH_TOURNAMENT_GSI", "tournament_index")

    def get(self, match_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": match_id})
        return resp.get("Item")

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def put_batch(self, items: list[dict[str, Any]]) -> None:
        with self._table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

    def delete(self, match_id: str) -> None:
        self._table.delete_item(Key={"id": match_id})

    def list_by_tournament(
        self,
        tournament_id: str,
        *,
        matchweek: int | None = None,
        status: str | None = None,
        team_id: str | None = None,
        round_name: str | None = None,
        group_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        """Query all matches for a tournament, with optional client-side filters."""
        kce = Key("tournament_id").eq(tournament_id)
        # Date range filtering via sort key when possible
        if date_from and date_to:
            kce = kce & Key("date").between(date_from, date_to)
        elif date_from:
            kce = kce & Key("date").gte(date_from)
        elif date_to:
            kce = kce & Key("date").lte(date_to)

        items = _query_all(
            self._table,
            IndexName=self._tournament_gsi,
            KeyConditionExpression=kce,
        )

        # Client-side filters
        if matchweek is not None:
            items = [i for i in items if i.get("matchweek") == matchweek]
        if status:
            items = [i for i in items if i.get("status") == status]
        if team_id:
            items = [i for i in items if i.get("home_team_id") == team_id or i.get("away_team_id") == team_id]
        if round_name:
            items = [i for i in items if i.get("round") == round_name]
        if group_id:
            items = [i for i in items if i.get("group_id") == group_id]

        return items

    def update(self, match_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        if not updates:
            return self.get(match_id)

        parts, eav, ean = [], {}, {}
        for i, (field, value) in enumerate(updates.items(), start=1):
            nk = f"#n{i}"
            vk = f":v{i}"
            ean[nk] = field
            eav[vk] = value
            parts.append(f"{nk} = {vk}")

        resp = self._table.update_item(
            Key={"id": match_id},
            UpdateExpression="SET " + ", ".join(parts),
            ExpressionAttributeValues=eav,
            ExpressionAttributeNames=ean,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")
