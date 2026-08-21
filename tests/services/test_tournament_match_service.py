import unittest
from unittest.mock import MagicMock

from api.schemas.tournaments import PatchMatch
from services.tournament_aggregator import default_team_stats, default_tournament_stats
from services.tournament_match_service import TournamentMatchService


class TestTournamentMatchServiceNotifications(unittest.TestCase):
    def setUp(self):
        self.repo = MagicMock()
        self.event_repo = MagicMock()
        self.event_repo.list_by_match.return_value = []
        self.team_repo = MagicMock()
        self.tournament_repo = MagicMock()
        self.match_notifications = MagicMock()

        self.team_repo.get.side_effect = lambda team_id: {
            "tm_home": {"id": "tm_home", "name": "Halcones", "stats": default_team_stats()},
            "tm_away": {"id": "tm_away", "name": "Tigres", "stats": default_team_stats()},
        }.get(team_id)
        self.tournament_repo.get.return_value = {
            "id": "trn_1",
            "name": "Liga 2026",
            "rules": {},
            "stats": default_tournament_stats(),
        }

        self.service = TournamentMatchService(
            self.repo,
            self.event_repo,
            team_repo=self.team_repo,
            tournament_repo=self.tournament_repo,
            match_notifications=self.match_notifications,
        )

        self.base_match = {
            "id": "mtc_1",
            "tournament_id": "trn_1",
            "home_team_id": "tm_home",
            "away_team_id": "tm_away",
            "status": "scheduled",
            "score_home": 0,
            "score_away": 0,
            "matchweek": 0,
        }

    def test_scheduled_to_live_notifies_match_started(self):
        self.repo.get.return_value = self.base_match
        updated = {**self.base_match, "status": "live"}
        self.repo.update.return_value = updated

        self.service.update_match("mtc_1", PatchMatch(status="live"))

        self.match_notifications.match_started.assert_called_once_with(
            tournament=self.tournament_repo.get.return_value,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )
        self.match_notifications.match_finished.assert_not_called()

    def test_live_to_finished_notifies_match_finished(self):
        live_match = {**self.base_match, "status": "live"}
        self.repo.get.return_value = live_match
        updated = {**live_match, "status": "finished", "score_home": 2, "score_away": 1}
        self.repo.update.return_value = updated

        self.service.update_match("mtc_1", PatchMatch(status="finished"))

        self.match_notifications.match_finished.assert_called_once_with(
            tournament=self.tournament_repo.get.return_value,
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )
        self.match_notifications.match_started.assert_not_called()

    def test_non_status_update_does_not_notify(self):
        self.repo.get.return_value = self.base_match
        self.repo.update.return_value = {**self.base_match, "venue": "New Venue"}

        self.service.update_match("mtc_1", PatchMatch(venue="New Venue"))

        self.match_notifications.match_started.assert_not_called()
        self.match_notifications.match_finished.assert_not_called()

    def test_scheduled_to_postponed_does_not_notify_started(self):
        self.repo.get.return_value = self.base_match
        self.repo.update.return_value = {**self.base_match, "status": "postponed"}

        self.service.update_match("mtc_1", PatchMatch(status="postponed"))

        self.match_notifications.match_started.assert_not_called()
        self.match_notifications.match_finished.assert_not_called()

    def test_reopen_finished_to_live_does_not_renotify_started_or_finished(self):
        finished_match = {**self.base_match, "status": "finished", "score_home": 2, "score_away": 1}
        self.repo.get.return_value = finished_match
        self.repo.update.return_value = {**finished_match, "status": "live"}

        self.service.update_match("mtc_1", PatchMatch(status="live"))

        self.match_notifications.match_started.assert_not_called()
        self.match_notifications.match_finished.assert_not_called()

    def test_notification_failure_does_not_prevent_match_update(self):
        self.repo.get.return_value = self.base_match
        updated = {**self.base_match, "status": "live"}
        self.repo.update.return_value = updated
        self.tournament_repo.get.side_effect = Exception("ddb throttled")

        result = self.service.update_match("mtc_1", PatchMatch(status="live"))

        self.assertEqual(result, updated)  # update succeeded despite notification failure


if __name__ == "__main__":
    unittest.main()
