import os
from .ddb_session import tournament_table
from boto3.dynamodb.conditions import Key, Attr
from typing import Any


def _query_all(table, **kwargs) -> list[dict[str, Any]]:
    """Paginate through all results of a query."""
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


class TournamentRepo:
    """DynamoDB-backed repository for the Tournament table.

    Groups and bracket data are stored as embedded JSON attributes
    on the tournament item.
    """

    def __init__(self):
        self._table = tournament_table()
        self._account_gsi = os.getenv("TOURNAMENT_ACCOUNT_GSI", "account_id_index")

    # ── Single-item operations ───────────────────────────────────────

    def get(self, tournament_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": tournament_id})
        return resp.get("Item")

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def delete(self, tournament_id: str) -> None:
        self._table.delete_item(Key={"id": tournament_id})

    # ── List / query ─────────────────────────────────────────────────

    def list_by_account(self, account_id: str, status: str | None = None) -> list[dict[str, Any]]:
        items = _query_all(
            self._table,
            IndexName=self._account_gsi,
            KeyConditionExpression=Key("account_id").eq(account_id),
        )
        if status:
            items = [i for i in items if i.get("status") == status]
        return items

    # ── Partial update ───────────────────────────────────────────────

    def update(self, tournament_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        if not updates:
            return self.get(tournament_id)

        parts, eav, ean = [], {}, {}
        for i, (field, value) in enumerate(updates.items(), start=1):
            nk = f"#n{i}"
            vk = f":v{i}"
            ean[nk] = field
            eav[vk] = value
            parts.append(f"{nk} = {vk}")

        resp = self._table.update_item(
            Key={"id": tournament_id},
            UpdateExpression="SET " + ", ".join(parts),
            ExpressionAttributeValues=eav,
            ExpressionAttributeNames=ean,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes")
