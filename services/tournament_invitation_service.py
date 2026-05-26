import logging
import os
import secrets
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

INVITATION_TTL_DAYS = 14
TOKEN_BYTES = 32  # 32-byte url-safe random ~ 43 chars


class TournamentInvitationService:
    def __init__(
        self,
        invitation_repo,
        tournament_repo,
        tournament_team_repo,
        account_repo,
        membership_svc,
        cognito_wrapper,
        notifications,
        user_repo,
    ):
        self._invitations = invitation_repo
        self._tournaments = tournament_repo
        self._teams = tournament_team_repo
        self._accounts = account_repo
        self._memberships = membership_svc
        self._cognito = cognito_wrapper
        self._notifications = notifications
        self._users = user_repo

    def create_for_team(
        self,
        *,
        account_id: str,
        tournament_id: str,
        tournament_team_id: str,
        email: str,
    ) -> dict:
        """Idempotent: if a pending invitation already exists for this (team, email),
        returns it without resending. Otherwise creates + sends."""
        # Idempotency: skip if a pending invitation already exists for this (team, email).
        existing = self._invitations.list_pending_for_team_email(tournament_team_id, email)
        if existing:
            return existing[0]

        now = self._now()
        invitation = {
            "id": str(uuid.uuid4()),
            "account_id": account_id,
            "tournament_id": tournament_id,
            "tournament_team_id": tournament_team_id,
            "email": email,
            "token": self._new_token(),
            "status": "pending",
            "expires_at": (now + timedelta(days=INVITATION_TTL_DAYS)).isoformat(),
            "accepted_at": None,
            "accepted_by_user_id": None,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }
        self._invitations.create(invitation)
        self._send_invitation_email(invitation)
        return invitation

    def _send_invitation_email(self, invitation: dict) -> None:
        tournament = self._tournaments.get(invitation["tournament_id"])
        team = self._teams.get(invitation["tournament_team_id"])
        account = self._accounts.get(invitation["account_id"])
        base = os.environ.get("BASE_ACTION_URL", "https://jmanage.app").rstrip("/")
        invite_url = f"{base}/invite/{invitation['token']}"
        self._notifications.team_owner_invited(
            email=invitation["email"],
            organizer_name=(account or {}).get("name", "Organizador"),
            tournament_name=(tournament or {}).get("name", "Torneo"),
            team_name=(team or {}).get("name", "Equipo"),
            invite_url=invite_url,
        )

    def _assert_team_belongs_to_account(self, team: dict | None, account_id: str) -> None:
        """Validate that the team's tournament belongs to the given account.
        Team items don't store account_id directly; the tournament row does."""
        if not team:
            raise ValueError("Team not found")
        tournament = self._tournaments.get(team.get("tournament_id", ""))
        if not tournament or tournament.get("account_id") != account_id:
            raise ValueError("Team not found in account")

    def resend(self, *, account_id: str, tournament_team_id: str) -> dict:
        """Regenerate token, extend expiry, re-send. Covers the case where the org first
        added the team without an email and then added it later via PATCH."""
        team = self._teams.get(tournament_team_id)
        self._assert_team_belongs_to_account(team, account_id)
        email = team.get("contact_email")
        if not email:
            raise ValueError("Team has no contact_email; set it before resending")

        existing = self._invitations.list_pending_for_team_email(tournament_team_id, email)
        if existing:
            inv = existing[0]
            # Rotate the token and extend expiry, but keep the same row id.
            now = self._now()
            self._invitations.update_status(
                inv["id"], "pending",
                token=self._new_token(),
                expires_at=(now + timedelta(days=INVITATION_TTL_DAYS)).isoformat(),
                updated_at=now.isoformat(),
            )
            inv = self._invitations.get_by_id(inv["id"])
            self._send_invitation_email(inv)
            return inv
        # No pending row exists → make one.
        return self.create_for_team(
            account_id=account_id,
            tournament_id=team["tournament_id"],
            tournament_team_id=tournament_team_id,
            email=email,
        )

    def revoke(self, *, account_id: str, tournament_team_id: str) -> None:
        """Mark the pending invitation revoked."""
        team = self._teams.get(tournament_team_id)
        self._assert_team_belongs_to_account(team, account_id)
        email = team.get("contact_email")
        if not email:
            return
        for inv in self._invitations.list_pending_for_team_email(tournament_team_id, email):
            self._invitations.update_status(inv["id"], "revoked", updated_at=self._now().isoformat())

    def get_public_summary(self, *, token: str) -> Optional[dict]:
        """Returns InvitationSummary dict or None if invalid/expired/revoked. See Task 7."""
        raise NotImplementedError  # Task 7

    def accept(
        self,
        *,
        token: str,
        password: Optional[str],
        authenticated_user_id: Optional[str],
        authenticated_email: Optional[str],
    ) -> dict:
        """See Task 7."""
        raise NotImplementedError  # Task 7

    def list_for_tournament(self, *, account_id: str, tournament_id: str) -> list[dict]:
        invitations = self._invitations.list_by_tournament(tournament_id)
        return [i for i in invitations if i.get("account_id") == account_id]

    @staticmethod
    def _new_token() -> str:
        return secrets.token_urlsafe(TOKEN_BYTES)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
