import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from api.schemas.training_sessions import ReviewIn, SessionIn
from repositories.training_session_repo_ddb import TrainingSessionRepo

DRAFT, SENT, APPROVED, REJECTED = "draft", "sent", "approved", "rejected"
EDITABLE_STATUSES = (DRAFT, REJECTED)
SENDABLE_STATUSES = (DRAFT, REJECTED)

# DynamoDB items are capped at 400KB; keep headroom for the other attributes.
MAX_EXERCISES_BYTES = 300_000


class InvalidTransition(Exception):
    """The requested action is not allowed from the session's current status."""


def ensure_editable(status: str) -> None:
    if status not in EDITABLE_STATUSES:
        raise InvalidTransition(f"A session in status '{status}' cannot be edited")


def ensure_sendable(status: str) -> None:
    if status not in SENDABLE_STATUSES:
        raise InvalidTransition(f"A session in status '{status}' cannot be sent")


def resolve_review(status: str, approved: bool, comment: str) -> tuple[str, str]:
    """Return (new_status, clean_comment) for a review, or raise."""
    if status != SENT:
        raise InvalidTransition(f"Only sent sessions can be reviewed (current status '{status}')")
    comment = (comment or "").strip()
    if not approved and not comment:
        raise ValueError("A comment is required to reject a session")
    return (APPROVED if approved else REJECTED), comment


def _to_dynamo(value: Any) -> Any:
    """Round-trip through JSON so floats become Decimal (boto3 rejects float)."""
    return json.loads(json.dumps(value), parse_float=Decimal)


def normalize_exercises(exercises: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = [{**e, "id": e.get("id") or uuid4().hex} for e in exercises]
    if len(json.dumps(out).encode("utf-8")) > MAX_EXERCISES_BYTES:
        raise ValueError("Exercises are too large to store; simplify the diagrams or remove exercises")
    return _to_dynamo(out)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TrainingSessionService:
    def __init__(self, repo: TrainingSessionRepo):
        self.repo = repo

    def list_sessions(self, workspace_id: str, account_id: str) -> list[dict[str, Any]]:
        items = self.repo.list_by_workspace(workspace_id, account_id)
        items.sort(key=lambda i: i.get("created_at", ""), reverse=True)
        return [self._map(i) for i in items]

    def get_session(self, session_id: str, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._get_raw(session_id, workspace_id, account_id)
        return self._map(item) if item else None

    def create_session(self, workspace_id: str, body: SessionIn, user_id: str, account_id: str) -> dict[str, Any]:
        now = _now()
        item = {
            "id": uuid4().hex,
            "account_id": account_id,
            "workspace_id": workspace_id,
            "title": body.title.strip(),
            "date": body.date,
            "location": body.location,
            "team_group": body.team_group,
            "objective": body.objective,
            "status": DRAFT,
            "exercises": normalize_exercises([e.model_dump() for e in body.exercises]),
            "review_comment": None,
            "reviewed_by": None,
            "reviewed_at": None,
            "created_by": user_id,
            "created_at": now,
            "updated_at": now,
        }
        self.repo.put(item)
        return self._map(item)

    def update_session(
        self, session_id: str, workspace_id: str, account_id: str, body: SessionIn
    ) -> dict[str, Any] | None:
        item = self._get_raw(session_id, workspace_id, account_id)
        if not item:
            return None
        ensure_editable(item["status"])
        self.repo.update(
            session_id,
            account_id,
            {
                "title": body.title.strip(),
                "date": body.date,
                "location": body.location,
                "team_group": body.team_group,
                "objective": body.objective,
                "exercises": normalize_exercises([e.model_dump() for e in body.exercises]),
                "updated_at": _now(),
            },
            expected_statuses=list(EDITABLE_STATUSES),
        )
        return self.get_session(session_id, workspace_id, account_id)

    def delete_session(self, session_id: str, workspace_id: str, account_id: str) -> bool:
        if not self._get_raw(session_id, workspace_id, account_id):
            return False
        self.repo.delete(session_id, account_id)
        return True

    def send_session(self, session_id: str, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self._get_raw(session_id, workspace_id, account_id)
        if not item:
            return None
        ensure_sendable(item["status"])
        # A resubmission starts a fresh review cycle.
        self.repo.update(
            session_id,
            account_id,
            {
                "status": SENT,
                "review_comment": None,
                "reviewed_by": None,
                "reviewed_at": None,
                "updated_at": _now(),
            },
            expected_statuses=list(SENDABLE_STATUSES),
        )
        return self.get_session(session_id, workspace_id, account_id)

    def review_session(
        self, session_id: str, workspace_id: str, account_id: str, body: ReviewIn, reviewer_id: str
    ) -> dict[str, Any] | None:
        item = self._get_raw(session_id, workspace_id, account_id)
        if not item:
            return None
        new_status, comment = resolve_review(item["status"], body.approved, body.comment)
        now = _now()
        self.repo.update(
            session_id,
            account_id,
            {
                "status": new_status,
                "review_comment": comment,
                "reviewed_by": reviewer_id,
                "reviewed_at": now,
                "updated_at": now,
            },
            expected_statuses=[SENT],
        )
        return self.get_session(session_id, workspace_id, account_id)

    def _get_raw(self, session_id: str, workspace_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(session_id, account_id)
        if not item or item.get("workspace_id") != workspace_id:
            return None
        return item

    @staticmethod
    def _map(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": item["id"],
            "workspaceId": item.get("workspace_id"),
            "title": item.get("title"),
            "date": item.get("date"),
            "location": item.get("location", ""),
            "teamGroup": item.get("team_group", ""),
            "objective": item.get("objective", ""),
            "status": item.get("status"),
            "exercises": [
                {
                    "id": e.get("id"),
                    "name": e.get("name"),
                    "durationMinutes": e.get("duration_minutes", 0),
                    "instructions": e.get("instructions", ""),
                    "diagram": e.get("diagram"),
                }
                for e in item.get("exercises", [])
            ],
            "reviewComment": item.get("review_comment"),
            "reviewedBy": item.get("reviewed_by"),
            "reviewedAt": item.get("reviewed_at"),
            "createdBy": item.get("created_by"),
            "createdAt": item.get("created_at"),
            "updatedAt": item.get("updated_at"),
        }
