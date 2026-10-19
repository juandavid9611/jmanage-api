import unittest

from core.public_fields import PUBLIC_PLAYER_FIELDS, PUBLIC_TEAM_FIELDS, public_player, public_team

SENSITIVE_TEAM = {
    "manager_name": "Someone Real",
    "contact_email": "manager@example.com",
    "contact_phone": "3001234567",
    "documents": {"id": "key"},
    "manager_user_ids": ["u-1"],
    "rules_accepted": True,
    "created_at": "2026-01-01",
}
SENSITIVE_PLAYER = {"id_number": "1234567890", "created_at": "2026-01-01"}


class TestPublicTeam(unittest.TestCase):
    def test_keeps_public_fields_and_drops_everything_else(self):
        team = {
            "id": "ttm_1", "tournament_id": "trn_1", "name": "Kanto League", "short_name": "KAN",
            "logo_url": "https://x/logo.png", "primary_color": "#fff", "seed": 3, "group_id": "g1",
            "stats": {"points": 9}, **SENSITIVE_TEAM,
        }
        out = public_team(team)
        self.assertEqual(set(out), set(PUBLIC_TEAM_FIELDS))
        for key in SENSITIVE_TEAM:
            self.assertNotIn(key, out)

    def test_missing_fields_are_omitted_not_none(self):
        self.assertEqual(public_team({"id": "t", "name": "A", **SENSITIVE_TEAM}), {"id": "t", "name": "A"})

    def test_new_private_fields_stay_private_by_default(self):
        self.assertNotIn("bank_account", public_team({"id": "t", "bank_account": "123"}))


class TestPublicPlayer(unittest.TestCase):
    def test_drops_national_id_and_internal_fields(self):
        player = {
            "id": "p1", "tournament_id": "trn_1", "team_id": "ttm_1", "name": "Player One",
            "number": 9, "position": "FW", "avatar_url": "https://x/a.png", "stats": {"goals": 4},
            **SENSITIVE_PLAYER,
        }
        out = public_player(player)
        self.assertEqual(set(out), set(PUBLIC_PLAYER_FIELDS))
        self.assertNotIn("id_number", out)
        self.assertNotIn("created_at", out)

    def test_allowlists_contain_no_personal_fields(self):
        banned = {"id_number", "contact_email", "contact_phone", "manager_name", "documents", "manager_user_ids", "email", "phone"}
        self.assertFalse(banned & set(PUBLIC_TEAM_FIELDS))
        self.assertFalse(banned & set(PUBLIC_PLAYER_FIELDS))


if __name__ == "__main__":
    unittest.main()
