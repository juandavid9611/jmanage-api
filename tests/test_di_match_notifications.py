import os
import unittest
from unittest.mock import patch, MagicMock


class TestDiMatchNotificationWiring(unittest.TestCase):
    def setUp(self):
        # Set up environment variables for DynamoDB and notification services
        env_vars = {
            "COURIER_AUTH_TOKEN": "test-token",
            "TOURNAMENT_MATCH_TABLE_NAME": "test-tournament-match",
            "TOURNAMENT_MATCH_EVENT_TABLE_NAME": "test-tournament-match-event",
            "TOURNAMENT_TEAM_TABLE_NAME": "test-tournament-team",
            "TOURNAMENT_INVITATION_TABLE_NAME": "test-tournament-invitation",
            "TOURNAMENT_TABLE_NAME": "test-tournament",
            "NOTIFICATION_TABLE_NAME": "test-notification",
            "ONESIGNAL_APP_ID": "test-app-id",
        }
        self._env_patch = patch.dict(os.environ, env_vars)
        self._env_patch.start()

        # Mock boto3 DynamoDB to avoid actual AWS calls
        self._boto3_patch = patch("repositories.ddb_session.dynamodb")
        mock_dynamodb = self._boto3_patch.start()
        mock_dynamodb.Table.return_value = MagicMock()

    def tearDown(self):
        self._env_patch.stop()
        self._boto3_patch.stop()

    def test_get_match_service_wires_match_notifications(self):
        from di import get_match_service
        from services.tournament_match_notification_service import TournamentMatchNotificationService

        service = get_match_service()

        self.assertIsInstance(service.match_notifications, TournamentMatchNotificationService)

    def test_get_match_event_service_wires_match_notifications(self):
        from di import get_match_event_service
        from services.tournament_match_notification_service import TournamentMatchNotificationService

        service = get_match_event_service()

        self.assertIsInstance(service.match_notifications, TournamentMatchNotificationService)


if __name__ == "__main__":
    unittest.main()
