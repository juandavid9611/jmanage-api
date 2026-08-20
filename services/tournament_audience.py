"""Resolves the notification audience for a tournament — the deduplicated
set of user emails associated with it, derived from existing team and
invitation data (there is no dedicated followers/roster table)."""

from typing import Any

from repositories.tournament_invitation_repo_ddb import TournamentInvitationRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo


def resolve_tournament_user_emails(
    tournament_id: str,
    team_repo: TournamentTeamRepo,
    invitation_repo: TournamentInvitationRepo,
) -> list[str]:
    emails: set[str] = set()

    for team in team_repo.list_by_tournament(tournament_id):
        contact_email = (team.get("contact_email") or "").strip()
        if contact_email:
            emails.add(contact_email)

    for invitation in invitation_repo.list_by_tournament(tournament_id):
        if invitation.get("status") != "accepted":
            continue
        email = (invitation.get("email") or "").strip()
        if email:
            emails.add(email)

    return sorted(emails)
