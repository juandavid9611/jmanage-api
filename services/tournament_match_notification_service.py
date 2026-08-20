"""Sends match-lifecycle push notifications to a tournament's audience.
Wraps audience resolution + the Notifications facade so the calling
services (TournamentMatchService, TournamentMatchEventService) don't
need to know how the audience is computed or handle delivery failures."""

import logging
from typing import Any, Callable

from repositories.tournament_invitation_repo_ddb import TournamentInvitationRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from services.notification_orchestator import Notifications
from services.tournament_audience import resolve_tournament_user_emails

logger = logging.getLogger(__name__)


class TournamentMatchNotificationService:
    def __init__(
        self,
        team_repo: TournamentTeamRepo,
        invitation_repo: TournamentInvitationRepo,
        notifications: Notifications,
    ) -> None:
        self._team_repo = team_repo
        self._invitation_repo = invitation_repo
        self._notifications = notifications

    def match_started(
        self,
        *,
        tournament: dict[str, Any],
        home_team_name: str,
        away_team_name: str,
    ) -> None:
        self._broadcast(
            tournament,
            lambda emails: self._notifications.match_started(
                user_emails=emails,
                tournament_name=tournament.get("name", ""),
                home_team_name=home_team_name,
                away_team_name=away_team_name,
            ),
        )

    def match_event_created(
        self,
        *,
        tournament: dict[str, Any],
        event: dict[str, Any],
        team_name: str,
        player_name: str | None,
    ) -> None:
        self._broadcast(
            tournament,
            lambda emails: self._notifications.match_event_created(
                user_emails=emails,
                tournament_name=tournament.get("name", ""),
                event_type=event.get("type", ""),
                team_name=team_name,
                player_name=player_name,
                minute=event.get("minute", 0),
            ),
        )

    def match_finished(
        self,
        *,
        tournament: dict[str, Any],
        home_team_name: str,
        away_team_name: str,
        score_home: int,
        score_away: int,
    ) -> None:
        self._broadcast(
            tournament,
            lambda emails: self._notifications.match_finished(
                user_emails=emails,
                tournament_name=tournament.get("name", ""),
                home_team_name=home_team_name,
                away_team_name=away_team_name,
                score_home=score_home,
                score_away=score_away,
            ),
        )

    def _broadcast(
        self, tournament: dict[str, Any], send: Callable[[list[str]], Any]
    ) -> None:
        tournament_id = (tournament or {}).get("id")
        if not tournament_id:
            return
        try:
            emails = resolve_tournament_user_emails(
                tournament_id, self._team_repo, self._invitation_repo
            )
            if emails:
                send(emails)
        except Exception:
            logger.exception(
                "Failed to send match notification for tournament %s", tournament_id
            )
