import unittest
from unittest.mock import MagicMock

from services.tournament_match_notification_service import TournamentMatchNotificationService


class TestTournamentMatchNotificationService(unittest.TestCase):
    def setUp(self):
        self.team_repo = MagicMock()
        self.invitation_repo = MagicMock()
        self.notifications = MagicMock()
        self.team_repo.list_by_tournament.return_value = [
            {"id": "tm_1", "contact_email": "coach@example.com"},
        ]
        self.invitation_repo.list_by_tournament.return_value = []
        self.service = TournamentMatchNotificationService(
            team_repo=self.team_repo,
            invitation_repo=self.invitation_repo,
            notifications=self.notifications,
        )
        self.tournament = {"id": "trn_1", "name": "Liga 2026"}

    def test_match_started_resolves_audience_and_sends(self):
        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.notifications.match_started.assert_called_once_with(
            user_emails=["coach@example.com"],
            tournament_name="Liga 2026",
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

    def test_match_event_created_resolves_audience_and_sends(self):
        event = {"type": "goal", "minute": 34}

        self.service.match_event_created(
            tournament=self.tournament,
            event=event,
            team_name="Halcones",
            player_name="Juan Perez",
        )

        self.notifications.match_event_created.assert_called_once_with(
            user_emails=["coach@example.com"],
            tournament_name="Liga 2026",
            event_type="goal",
            team_name="Halcones",
            player_name="Juan Perez",
            minute=34,
        )

    def test_match_finished_resolves_audience_and_sends(self):
        self.service.match_finished(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )

        self.notifications.match_finished.assert_called_once_with(
            user_emails=["coach@example.com"],
            tournament_name="Liga 2026",
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )

    def test_no_recipients_skips_send(self):
        self.team_repo.list_by_tournament.return_value = []

        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.notifications.match_started.assert_not_called()

    def test_missing_tournament_id_skips_send_without_raising(self):
        self.service.match_started(
            tournament={},
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.notifications.match_started.assert_not_called()

    def test_swallows_exception_from_audience_resolution(self):
        self.team_repo.list_by_tournament.side_effect = Exception("ddb down")

        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )  # must not raise

        self.notifications.match_started.assert_not_called()

    def test_none_tournament_skips_send_without_raising(self):
        self.service.match_started(
            tournament=None,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )  # must not raise

        self.notifications.match_started.assert_not_called()

    def test_swallows_exception_from_notifications_send(self):
        self.notifications.match_started.side_effect = Exception("onesignal down")

        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )  # must not raise


if __name__ == "__main__":
    unittest.main()
