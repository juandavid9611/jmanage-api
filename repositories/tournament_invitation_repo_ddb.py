import os
from typing import Any

from boto3.dynamodb.conditions import Key, Attr

from .ddb_session import tournament_invitation_table


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


def _scan_all(table, **kwargs) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    start_key = None
    while True:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break
    return items


class TournamentInvitationRepo:
    """DynamoDB-backed repository for the TournamentInvitation table."""

    def __init__(self):
        self._table = tournament_invitation_table()
        self._token_gsi = os.getenv("INVITATION_TOKEN_GSI", "token_index")
        self._account_gsi = os.getenv("INVITATION_ACCOUNT_GSI", "account_id_index")
        self._tournament_gsi = os.getenv("INVITATION_TOURNAMENT_GSI", "tournament_index")

    def create(self, invitation: dict) -> dict:
        """Persist a new invitation item and return it."""
        self._table.put_item(Item=invitation)
        return invitation

    def get_by_id(self, invitation_id: str) -> dict | None:
        resp = self._table.get_item(Key={"id": invitation_id})
        return resp.get("Item")

    def get_by_token(self, token: str) -> dict | None:
        """Lookup via token_index GSI; returns the single item or None."""
        items = _query_all(
            self._table,
            IndexName=self._token_gsi,
            KeyConditionExpression=Key("token").eq(token),
        )
        return items[0] if items else None

    def list_by_account(self, account_id: str) -> list[dict]:
        return _query_all(
            self._table,
            IndexName=self._account_gsi,
            KeyConditionExpression=Key("account_id").eq(account_id),
        )

    def list_by_tournament(self, tournament_id: str) -> list[dict]:
        return _query_all(
            self._table,
            IndexName=self._tournament_gsi,
            KeyConditionExpression=Key("tournament_id").eq(tournament_id),
        )

    def list_pending_for_team_email(self, tournament_team_id: str, email: str) -> list[dict]:
        """Scan-and-filter for idempotency check when creating invitations.
        Acceptable: row count per team is low."""
        return _scan_all(
            self._table,
            FilterExpression=(
                Attr("tournament_team_id").eq(tournament_team_id)
                & Attr("email").eq(email)
                & Attr("status").eq("pending")
            ),
        )

    def list_by_team_email(self, tournament_team_id: str, email: str) -> list[dict]:
        """Scan-and-filter for ALL invitation rows matching (tournament_team_id, email),
        regardless of status. Used to prevent re-invitation after acceptance."""
        return _scan_all(
            self._table,
            FilterExpression=(
                Attr("tournament_team_id").eq(tournament_team_id)
                & Attr("email").eq(email)
            ),
        )

    def update_status(self, invitation_id: str, status: str, **extra_fields) -> dict:
        """Update status + updated_at + any extra fields (e.g. accepted_at)."""
        updates = {"status": status, **extra_fields}

        parts, eav, ean = [], {}, {}
        for i, (field, value) in enumerate(updates.items(), start=1):
            nk = f"#n{i}"
            vk = f":v{i}"
            ean[nk] = field
            eav[vk] = value
            parts.append(f"{nk} = {vk}")

        resp = self._table.update_item(
            Key={"id": invitation_id},
            UpdateExpression="SET " + ", ".join(parts),
            ExpressionAttributeValues=eav,
            ExpressionAttributeNames=ean,
            ReturnValues="ALL_NEW",
        )
        return resp.get("Attributes", {})
