import unittest
from unittest.mock import MagicMock

from services.notification_orchestator import Notifications


class TestNotificationsMatchMethods(unittest.TestCase):
    def setUp(self):
        self.email_sender = MagicMock()
        self.in_app_sender = MagicMock()
        self.in_app_sender.publish_bulk.return_value = "notif_1"
        self.notifications = Notifications(
            email_sender=self.email_sender,
            in_app_sender=self.in_app_sender,
        )

    def test_match_started_sends_bulk_in_app_notification(self):
        result = self.notifications.match_started(
            user_emails=["a@example.com", "b@example.com"],
            tournament_name="Liga 2026",
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.assertEqual(result, "notif_1")
        self.in_app_sender.publish_bulk.assert_called_once_with(
            user_emails=["a@example.com", "b@example.com"],
            title="¡Comenzó el partido!",
            content="Halcones vs Tigres — Liga 2026",
            category="match_started",
            action_url_path="dashboard/tournaments",
        )

    def test_match_event_created_maps_goal_copy(self):
        self.notifications.match_event_created(
            user_emails=["a@example.com"],
            tournament_name="Liga 2026",
            event_type="goal",
            team_name="Halcones",
            player_name="Juan Perez",
            minute=34,
        )

        self.in_app_sender.publish_bulk.assert_called_once_with(
            user_emails=["a@example.com"],
            title="¡Gol!",
            content="Gol de Juan Perez (Halcones) al minuto 34. — Liga 2026",
            category="match_event",
            action_url_path="dashboard/tournaments",
        )

    def test_match_event_created_maps_red_card_copy(self):
        self.notifications.match_event_created(
            user_emails=["a@example.com"],
            tournament_name="Liga 2026",
            event_type="red_card",
            team_name="Tigres",
            player_name="Mario Ruiz",
            minute=70,
        )

        self.in_app_sender.publish_bulk.assert_called_once_with(
            user_emails=["a@example.com"],
            title="Tarjeta roja",
            content="Roja para Mario Ruiz (Tigres) al minuto 70. — Liga 2026",
            category="match_event",
            action_url_path="dashboard/tournaments",
        )

    def test_match_event_created_maps_substitution_copy_without_player_name(self):
        self.notifications.match_event_created(
            user_emails=["a@example.com"],
            tournament_name="Liga 2026",
            event_type="substitution",
            team_name="Halcones",
            player_name=None,
            minute=60,
        )

        self.in_app_sender.publish_bulk.assert_called_once_with(
            user_emails=["a@example.com"],
            title="Cambio",
            content="Cambio en Halcones al minuto 60. — Liga 2026",
            category="match_event",
            action_url_path="dashboard/tournaments",
        )

    def test_match_event_created_unknown_type_falls_back(self):
        self.notifications.match_event_created(
            user_emails=["a@example.com"],
            tournament_name="Liga 2026",
            event_type="some_future_type",
            team_name="Halcones",
            player_name=None,
            minute=10,
        )

        self.in_app_sender.publish_bulk.assert_called_once_with(
            user_emails=["a@example.com"],
            title="Evento del partido",
            content="Halcones — minuto 10. — Liga 2026",
            category="match_event",
            action_url_path="dashboard/tournaments",
        )

    def test_match_finished_sends_bulk_in_app_notification(self):
        self.notifications.match_finished(
            user_emails=["a@example.com"],
            tournament_name="Liga 2026",
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )

        self.in_app_sender.publish_bulk.assert_called_once_with(
            user_emails=["a@example.com"],
            title="Final del partido",
            content="Halcones 2 - 1 Tigres — Liga 2026",
            category="match_finished",
            action_url_path="dashboard/tournaments",
        )


if __name__ == "__main__":
    unittest.main()
