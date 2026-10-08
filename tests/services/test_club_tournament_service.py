import unittest

from services.club_tournament_service import (
    normalize_roster_identity,
    validate_lineup,
    validate_match_date,
)


class TestRosterIdentity(unittest.TestCase):
    def test_user_only(self):
        self.assertEqual(normalize_roster_identity("u1", None), ("u1", None))

    def test_guest_only_is_trimmed(self):
        self.assertEqual(normalize_roster_identity(None, "  Pepe "), (None, "Pepe"))

    def test_blank_strings_count_as_missing(self):
        self.assertEqual(normalize_roster_identity("", "Pepe"), (None, "Pepe"))

    def test_both_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize_roster_identity("u1", "Pepe")

    def test_neither_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize_roster_identity(None, "  ")


class TestMatchDate(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(validate_match_date("2026-10-07"), "2026-10-07")

    def test_impossible_date(self):
        with self.assertRaises(ValueError):
            validate_match_date("2026-02-31")


class TestValidateLineup(unittest.TestCase):
    ROSTER = {"r1", "r2", "r3"}

    def test_normalizes_entries(self):
        out = validate_lineup(
            [{"roster_entry_id": "r1", "called_up": True, "status": "titular", "minutes": 90},
             {"roster_entry_id": "r2", "called_up": False}],
            self.ROSTER,
        )
        self.assertEqual(out[0], {"roster_entry_id": "r1", "called_up": True, "status": "titular", "minutes": 90})
        self.assertEqual(out[1], {"roster_entry_id": "r2", "called_up": False, "status": "", "minutes": 0})

    def test_empty_lineup_is_valid(self):
        self.assertEqual(validate_lineup([], self.ROSTER), [])

    def test_unknown_roster_entry(self):
        with self.assertRaises(ValueError):
            validate_lineup([{"roster_entry_id": "zzz", "called_up": True}], self.ROSTER)

    def test_duplicate_roster_entry(self):
        with self.assertRaises(ValueError):
            validate_lineup(
                [{"roster_entry_id": "r1", "called_up": True}, {"roster_entry_id": "r1", "called_up": True}],
                self.ROSTER,
            )

    def test_invalid_status(self):
        with self.assertRaises(ValueError):
            validate_lineup([{"roster_entry_id": "r1", "called_up": True, "status": "capitan"}], self.ROSTER)


if __name__ == "__main__":
    unittest.main()
