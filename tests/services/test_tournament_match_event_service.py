import unittest
from unittest.mock import MagicMock

from api.schemas.tournaments import CreateMatchEvent
from services.tournament_match_event_service import TournamentMatchEventService


class TestTournamentMatchEventServiceNotifications(unittest.TestCase):
    def setUp(self):
        self.repo = MagicMock()
        self.repo.list_by_match.return_value = []
        self.match_repo = MagicMock()
        self.team_repo = MagicMock()
        self.player_repo = MagicMock()
        self.tournament_repo = MagicMock()
        self.match_notifications = MagicMock()

        self.match_repo.get.return_value = {
            "id": "mtc_1",
            "tournament_id": "trn_1",
            "status": "live",
            "home_team_id": "tm_home",
            "away_team_id": "tm_away",
        }
        self.team_repo.get.return_value = {"id": "tm_home", "name": "Halcones"}
        self.player_repo.get.return_value = {"id": "ply_1", "name": "Juan Perez"}
        self.tournament_repo.get.return_value = {"id": "trn_1", "name": "Liga 2026"}

        self.service = TournamentMatchEventService(
            self.repo,
            match_repo=self.match_repo,
            team_repo=self.team_repo,
            player_repo=self.player_repo,
            tournament_repo=self.tournament_repo,
            match_notifications=self.match_notifications,
        )

    def test_create_goal_event_notifies_match_event_created(self):
        body = CreateMatchEvent(type="goal", minute=34, player_id="ply_1", team_id="tm_home")

        item = self.service.create_event("mtc_1", body)

        self.match_notifications.match_event_created.assert_called_once_with(
            tournament=self.tournament_repo.get.return_value,
            event=item,
            team_name="Halcones",
            player_name="Juan Perez",
        )

    def test_create_substitution_event_notifies_without_requiring_player_lookup_failure(self):
        body = CreateMatchEvent(
            type="substitution", minute=60, player_id="ply_1", assist_player_id="ply_2", team_id="tm_home"
        )

        self.service.create_event("mtc_1", body)

        self.match_notifications.match_event_created.assert_called_once()

    def test_no_notification_service_configured_does_not_raise(self):
        service = TournamentMatchEventService(
            self.repo,
            match_repo=self.match_repo,
            team_repo=self.team_repo,
            player_repo=self.player_repo,
            tournament_repo=self.tournament_repo,
            match_notifications=None,
        )
        body = CreateMatchEvent(type="goal", minute=10, player_id="ply_1", team_id="tm_home")

        service.create_event("mtc_1", body)  # must not raise


if __name__ == "__main__":
    unittest.main()
