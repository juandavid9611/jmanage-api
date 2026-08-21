import unittest
from unittest.mock import MagicMock

from services.tournament_audience import resolve_tournament_user_emails


class TestResolveTournamentUserEmails(unittest.TestCase):
    def setUp(self):
        self.team_repo = MagicMock()
        self.invitation_repo = MagicMock()

    def test_empty_tournament_returns_empty_list(self):
        self.team_repo.list_by_tournament.return_value = []
        self.invitation_repo.list_by_tournament.return_value = []

        result = resolve_tournament_user_emails("trn_1", self.team_repo, self.invitation_repo)

        self.assertEqual(result, [])

    def test_includes_team_contact_emails(self):
        self.team_repo.list_by_tournament.return_value = [
            {"id": "tm_1", "contact_email": "coach1@example.com"},
            {"id": "tm_2", "contact_email": "coach2@example.com"},
        ]
        self.invitation_repo.list_by_tournament.return_value = []

        result = resolve_tournament_user_emails("trn_1", self.team_repo, self.invitation_repo)

        self.assertEqual(result, ["coach1@example.com", "coach2@example.com"])

    def test_includes_only_accepted_invitations(self):
        self.team_repo.list_by_tournament.return_value = []
        self.invitation_repo.list_by_tournament.return_value = [
            {"email": "accepted@example.com", "status": "accepted"},
            {"email": "pending@example.com", "status": "pending"},
        ]

        result = resolve_tournament_user_emails("trn_1", self.team_repo, self.invitation_repo)

        self.assertEqual(result, ["accepted@example.com"])

    def test_dedupes_overlapping_emails(self):
        self.team_repo.list_by_tournament.return_value = [
            {"id": "tm_1", "contact_email": "same@example.com"},
        ]
        self.invitation_repo.list_by_tournament.return_value = [
            {"email": "same@example.com", "status": "accepted"},
        ]

        result = resolve_tournament_user_emails("trn_1", self.team_repo, self.invitation_repo)

        self.assertEqual(result, ["same@example.com"])

    def test_skips_blank_or_missing_contact_email(self):
        self.team_repo.list_by_tournament.return_value = [
            {"id": "tm_1", "contact_email": ""},
            {"id": "tm_2"},
            {"id": "tm_3", "contact_email": "  "},
            {"id": "tm_4", "contact_email": "real@example.com"},
        ]
        self.invitation_repo.list_by_tournament.return_value = []

        result = resolve_tournament_user_emails("trn_1", self.team_repo, self.invitation_repo)

        self.assertEqual(result, ["real@example.com"])


if __name__ == "__main__":
    unittest.main()
