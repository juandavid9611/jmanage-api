import unittest

from core.account_type import is_club


class TestIsClub(unittest.TestCase):
    def test_club_account(self):
        self.assertTrue(is_club({"settings": {"account_type": "club"}}))

    def test_tournament_account(self):
        self.assertFalse(is_club({"settings": {"account_type": "tournament"}}))

    def test_missing_account_type_counts_as_club(self):
        self.assertTrue(is_club({"settings": {"timezone": "America/Bogota"}}))

    def test_missing_or_empty_settings_counts_as_club(self):
        self.assertTrue(is_club({}))
        self.assertTrue(is_club({"settings": None}))

    def test_none_account(self):
        self.assertTrue(is_club(None))

    def test_unknown_type_is_not_club(self):
        self.assertFalse(is_club({"settings": {"account_type": "federation"}}))


if __name__ == "__main__":
    unittest.main()
