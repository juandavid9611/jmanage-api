import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional

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
        returns it without resending. Otherwise creates + sends. See Task 5."""
        raise NotImplementedError  # Task 5

    def resend(self, *, account_id: str, tournament_team_id: str) -> dict:
        """Regenerate token, extend expiry, re-send. See Task 5."""
        raise NotImplementedError  # Task 5

    def revoke(self, *, account_id: str, tournament_team_id: str) -> None:
        """Mark the pending invitation revoked. See Task 5."""
        raise NotImplementedError  # Task 5

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
