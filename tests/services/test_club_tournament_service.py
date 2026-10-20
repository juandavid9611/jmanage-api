import unittest

from unittest.mock import MagicMock

from api.schemas.club_tournaments import LineupEntryStatPatch
from services.club_tournament_service import (
    ClubTournamentService,
    normalize_roster_identity,
    parse_lineup_stat,
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


class TestValidateLineupKeepsStats(unittest.TestCase):
    ROSTER = {"r1", "r2"}

    def test_stats_survive_a_lineup_save(self):
        previous = [{"roster_entry_id": "r1", "called_up": True, "goals": 2, "yellow_card": True}]
        out = validate_lineup(
            [{"roster_entry_id": "r1", "called_up": True, "status": "titular", "goals": 99, "assists": 99}],
            self.ROSTER,
            previous,
        )
        self.assertEqual(out[0]["goals"], 2)
        self.assertTrue(out[0]["yellow_card"])
        self.assertNotIn("assists", out[0])

    def test_new_entry_gets_no_stats(self):
        out = validate_lineup([{"roster_entry_id": "r2", "called_up": True}], self.ROSTER, [])
        self.assertEqual(set(out[0]), {"roster_entry_id", "called_up", "status", "minutes"})


class TestParseLineupStat(unittest.TestCase):
    def test_counter(self):
        self.assertEqual(parse_lineup_stat("goals", "3"), ("goals", 3))
        self.assertEqual(parse_lineup_stat("assists", "0"), ("assists", 0))

    def test_counter_rejects_negative_decimal_empty_and_huge(self):
        for bad in ("-1", "1.5", "", "abc", "100"):
            with self.assertRaises(ValueError, msg=bad):
                parse_lineup_stat("goals", bad)

    def test_flags_use_stored_snake_case_keys(self):
        self.assertEqual(parse_lineup_stat("yellowCard", "true"), ("yellow_card", True))
        self.assertEqual(parse_lineup_stat("redCard", "False"), ("red_card", False))
        self.assertEqual(parse_lineup_stat("mvp", "true"), ("mvp", True))

    def test_flag_rejects_garbage(self):
        with self.assertRaises(ValueError):
            parse_lineup_stat("mvp", "maybe")

    def test_unknown_or_non_stat_property(self):
        for name in ("approved", "minutes", "yellow_card", ""):
            with self.assertRaises(ValueError, msg=name):
                parse_lineup_stat(name, "1")


class TestPatchLineupEntryStat(unittest.TestCase):
    def setUp(self):
        self.match_repo = MagicMock()
        self.svc = ClubTournamentService(MagicMock(), MagicMock(), self.match_repo, MagicMock())
        self.match = {
            "id": "m1", "tournament_id": "t1", "workspace_id": "w1", "account_id": "a1",
            "lineup": {"saved_at": "x", "entries": [
                {"roster_entry_id": "r1", "called_up": False},
                {"roster_entry_id": "r2", "called_up": True, "status": "titular", "minutes": 0},
            ]},
        }
        self.match_repo.get.return_value = self.match
        self.match_repo.set_lineup_entry_field.return_value = True

    def patch(self, entry_id, name, value):
        return self.svc.patch_lineup_entry_stat("t1", "m1", entry_id, "w1", "a1", LineupEntryStatPatch(name=name, value=value))

    def test_writes_only_the_one_field_of_the_right_index(self):
        out = self.patch("r2", "yellowCard", "true")
        self.match_repo.set_lineup_entry_field.assert_called_once_with("m1", "a1", 1, "r2", "yellow_card", True)
        self.assertEqual(out["id"], "m1")

    def test_entry_not_called_up(self):
        with self.assertRaises(ValueError):
            self.patch("r1", "goals", "1")

    def test_entry_not_in_lineup(self):
        with self.assertRaises(ValueError):
            self.patch("zzz", "goals", "1")

    def test_no_lineup_yet(self):
        self.match["lineup"] = None
        with self.assertRaises(ValueError):
            self.patch("r2", "goals", "1")

    def test_match_of_another_workspace_is_not_found(self):
        self.match["workspace_id"] = "other"
        self.assertIsNone(self.patch("r2", "goals", "1"))
        self.match_repo.set_lineup_entry_field.assert_not_called()

    def test_invalid_value_never_reaches_the_repo(self):
        with self.assertRaises(ValueError):
            self.patch("r2", "goals", "-3")
        self.match_repo.set_lineup_entry_field.assert_not_called()

    def test_lost_race_is_reported(self):
        self.match_repo.set_lineup_entry_field.return_value = False
        with self.assertRaises(ValueError):
            self.patch("r2", "goals", "1")

    def test_map_match_defaults_missing_stats(self):
        out = ClubTournamentService._map_match(self.match)
        entry = out["lineup"]["entries"][1]
        self.assertEqual((entry["goals"], entry["assists"], entry["mvp"]), (0, 0, False))
        self.assertEqual((entry["late"], entry["yellowCard"], entry["redCard"]), (False, False, False))


if __name__ == "__main__":
    unittest.main()
