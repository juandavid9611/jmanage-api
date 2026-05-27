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
        # Idempotency: skip if a pending OR accepted invitation already exists for this
        # (team, email). Expired/revoked rows are legitimate retries and do not block.
        existing = self._invitations.list_by_team_email(tournament_team_id, email)
        blocking = [r for r in existing if r.get("status") in ("pending", "accepted")]
        if blocking:
            # Prefer the pending row if both somehow exist; otherwise return the accepted one.
            pending = [r for r in blocking if r.get("status") == "pending"]
            return pending[0] if pending else blocking[0]

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
        """Returns InvitationSummary dict or None if invalid/expired/revoked."""
        inv = self._invitations.get_by_token(token)
        if not inv:
            return None
        if inv["status"] in ("revoked",):
            return None
        # Lazy-expire: if expired_at < now and still pending, flip to expired and return None.
        if datetime.fromisoformat(inv["expires_at"]) < self._now() and inv["status"] == "pending":
            self._invitations.update_status(inv["id"], "expired", updated_at=self._now().isoformat())
            return None
        tournament = self._tournaments.get(inv["tournament_id"]) or {}
        team = self._teams.get(inv["tournament_team_id"]) or {}
        account = self._accounts.get(inv["account_id"]) or {}
        existing_user = self._users.get_by_email(inv["email"])

        return {
            "tournament_id": inv["tournament_id"],
            "tournament_name": tournament.get("name", ""),
            "tournament_team_id": inv["tournament_team_id"],
            "team_name": team.get("name", ""),
            "organizer_account_name": account.get("name", ""),
            "email": inv["email"],
            "email_has_existing_user": bool(existing_user),
            "status": inv["status"],
            "expires_at": inv["expires_at"],
        }

    def accept(
        self,
        *,
        token: str,
        password: Optional[str],
        authenticated_user_id: Optional[str],
        authenticated_email: Optional[str],
    ) -> dict:
        """Accept a team-owner invitation.

        Two paths:
        - Authenticated: caller provides their JWT; we verify the email matches and
          link the existing user to the team/account without creating new credentials.
        - Unauthenticated: a new Cognito user is created, stored in the user table,
          and JWT tokens are returned so the frontend can sign in immediately.
        """
        # NOTE: race condition — two concurrent accepts on the same pending invitation
        # can both pass the status check below and proceed to create_membership / Cognito
        # user creation.  Acceptable for current low-concurrency usage.  Proper fix:
        # a DynamoDB conditional update (ConditionExpression: status = "pending") so
        # only one writer wins; the loser gets a ConditionalCheckFailedException.
        inv = self._invitations.get_by_token(token)
        if not inv:
            raise ValueError("Invalid invitation token")
        if inv["status"] != "pending":
            raise ValueError(f"Invitation is {inv['status']}")
        if datetime.fromisoformat(inv["expires_at"]) < self._now():
            self._invitations.update_status(inv["id"], "expired", updated_at=self._now().isoformat())
            raise ValueError("Invitation expired")

        user_id: str
        tokens: dict = {}

        if authenticated_user_id and authenticated_email:
            # Authenticated path: enforce email match.
            if authenticated_email.strip().lower() != inv["email"].strip().lower():
                raise ValueError("Authenticated user's email does not match invitation email")
            user_id = authenticated_user_id
        else:
            # Unauthenticated path: must create a new Cognito user.
            if not password:
                raise ValueError("Password required for new user")
            if self._users.get_by_email(inv["email"]):
                raise ValueError("A user with this email already exists; sign in first and retry")
            cognito_resp = self._cognito.admin_create_confirmed_user(
                user_email=inv["email"], name=inv["email"].split("@")[0], password=password,
            )
            user = cognito_resp["User"]
            user_id = next(a["Value"] for a in user["Attributes"] if a["Name"] == "sub")
            self._users.create({"id": user_id, "email": inv["email"], "name": inv["email"].split("@")[0]})
            sign_in = self._cognito.start_sign_in(user_name=inv["email"], password=password)
            auth = sign_in.get("AuthenticationResult") or {}
            tokens = {
                "access_token": auth.get("AccessToken"),
                "id_token": auth.get("IdToken"),
                "refresh_token": auth.get("RefreshToken"),
            }

        # Fetch account to resolve default workspace (required by MembershipService).
        account = self._accounts.get(inv["account_id"]) or {}
        default_workspace = (account.get("settings") or {}).get("default_workspace")
        if not default_workspace:
            raise ValueError(
                "Account has no default workspace configured; configure one before accepting invitations"
            )

        existing_memberships = self._memberships.get_user_account_memberships(
            user_id, inv["account_id"]
        )
        has_membership = any(
            m.get("workspace_id") == default_workspace for m in existing_memberships
        )
        if has_membership:
            logger.info(
                "accept: user %s already has a membership on account %s / workspace %s — "
                "skipping create_membership to preserve existing role",
                user_id,
                inv["account_id"],
                default_workspace,
            )
        else:
            self._memberships.create_membership(
                user_id=user_id,
                account_id=inv["account_id"],
                workspace_id=default_workspace,
                role="team_owner",
            )
        self._teams.update(inv["tournament_team_id"], {"owner_user_id": user_id})
        now = self._now()
        self._invitations.update_status(
            inv["id"], "accepted",
            accepted_at=now.isoformat(),
            accepted_by_user_id=user_id,
            updated_at=now.isoformat(),
        )
        return {
            "account_id": inv["account_id"],
            "tournament_id": inv["tournament_id"],
            "tournament_team_id": inv["tournament_team_id"],
            **tokens,
        }

    def list_for_tournament(self, *, account_id: str, tournament_id: str) -> list[dict]:
        invitations = self._invitations.list_by_tournament(tournament_id)
        return [i for i in invitations if i.get("account_id") == account_id]

    @staticmethod
    def _new_token() -> str:
        return secrets.token_urlsafe(TOKEN_BYTES)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
