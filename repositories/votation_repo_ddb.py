import os
from typing import Any

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from .ddb_session import votation_table


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


class VotationRepo:
    """DynamoDB-backed repository for the Votation table."""

    def __init__(self):
        self._table = votation_table()
        self._account_gsi = os.getenv("VOTATION_ACCOUNT_GSI", "account_id_index")

    def get(self, votation_id: str, account_id: str) -> dict[str, Any] | None:
        resp = self._table.get_item(Key={"id": votation_id})
        item = resp.get("Item")
        if item and item.get("account_id") != account_id:
            return None
        return item

    def list_by_workspace(self, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        items = _query_all(
            self._table,
            IndexName=self._account_gsi,
            KeyConditionExpression=Key("account_id").eq(account_id),
            FilterExpression=Attr("workspace_id").eq(workspace_id),
        )
        return items

    def list_by_account(self, account_id: str) -> list[dict[str, Any]]:
        return _query_all(
            self._table,
            IndexName=self._account_gsi,
            KeyConditionExpression=Key("account_id").eq(account_id),
        )

    def put(self, item: dict[str, Any]) -> None:
        self._table.put_item(Item=item)

    def update_status(self, votation_id: str, account_id: str, status: str) -> None:
        self._table.update_item(
            Key={"id": votation_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": status},
            ConditionExpression=Attr("account_id").eq(account_id),
        )

    def update_candidates(self, votation_id: str, account_id: str, candidates: list[dict]) -> None:
        self._table.update_item(
            Key={"id": votation_id},
            UpdateExpression="SET candidates = :c",
            ExpressionAttributeValues={":c": candidates},
            ConditionExpression=Attr("account_id").eq(account_id),
        )

    def cast_vote(self, votation_id: str, account_id: str, voter_id: str, candidate_id: str) -> bool:
        """
        Atomically record or update a vote. Returns True if accepted,
        False if the votation is closed or account mismatch.
        """
        try:
            self._table.update_item(
                Key={"id": votation_id},
                UpdateExpression="SET votes.#voter = :cid",
                ExpressionAttributeNames={"#voter": voter_id},
                ExpressionAttributeValues={":cid": candidate_id},
                ConditionExpression=(
                    Attr("account_id").eq(account_id)
                    & Attr("status").eq("open")
                ),
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def delete(self, votation_id: str, account_id: str) -> None:
        item = self.get(votation_id, account_id)
        if not item:
            raise ValueError(f"Votation {votation_id} not found")
        self._table.delete_item(Key={"id": votation_id})

    def set_winner(self, votation_id: str, account_id: str, winner_id: str) -> None:
        self._table.update_item(
            Key={"id": votation_id},
            UpdateExpression="SET winner_id = :w, #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":w": winner_id, ":s": "closed"},
            ConditionExpression=Attr("account_id").eq(account_id),
        )

    def set_tied(self, votation_id: str, account_id: str, tied_candidate_ids: list) -> None:
        self._table.update_item(
            Key={"id": votation_id},
            UpdateExpression="SET #s = :s, tied_candidate_ids = :t",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "tied", ":t": tied_candidate_ids},
            ConditionExpression=Attr("account_id").eq(account_id),
        )

    def set_tiebreaker_id(self, votation_id: str, account_id: str, tiebreaker_votation_id: str) -> None:
        self._table.update_item(
            Key={"id": votation_id},
            UpdateExpression="SET tiebreaker_votation_id = :t",
            ExpressionAttributeValues={":t": tiebreaker_votation_id},
            ConditionExpression=Attr("account_id").eq(account_id),
        )
