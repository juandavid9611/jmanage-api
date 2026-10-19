import os
import unittest
from unittest.mock import patch

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from services.tournament_service import TournamentService


class FakeRepo:
    def __init__(self, items):
        self.items = items

    def list_public(self):
        return [dict(i) for i in self.items]


class FakeTeamRepo:
    def __init__(self, teams):
        self.teams = teams
        self.gets = []

    def get(self, team_id):
        self.gets.append(team_id)
        return self.teams.get(team_id)


class FakeS3:
    def presign_get_from_explicit_key(self, key, content_type):
        return f"https://signed/{key}"


def final_with(winner):
    return {"final": [{"team1_id": "a", "team2_id": "b", "winner_team_id": winner}]}


def mk(items, teams, s3=None):
    repo = FakeRepo(items)
    team_repo = FakeTeamRepo(teams)
    return TournamentService(repo, team_repo=team_repo, s3=s3), team_repo


class TestPublicChampion(unittest.TestCase):
    def test_finished_with_final_winner_gets_champion(self):
        svc, _ = mk(
            [{"id": "t1", "status": "finished", "bracket": final_with("a")}],
            {"a": {"id": "a", "tournament_id": "t1", "name": "Samba FC", "short_name": "SFC", "logo_url": "k/logo.png"}},
            s3=FakeS3(),
        )
        out = svc.list_public_tournaments()
        champ = out["items"][0]["champion"]
        self.assertEqual((champ["team_id"], champ["name"], champ["short_name"]), ("a", "Samba FC", "SFC"))
        self.assertEqual(champ["logo_url"], "https://signed/k/logo.png")

    def test_no_champion_when_not_finished_or_no_winner_or_league(self):
        svc, repo = mk(
            [
                {"id": "t1", "status": "active", "bracket": final_with("a")},
                {"id": "t2", "status": "finished", "bracket": final_with(None)},
                {"id": "t3", "status": "finished"},  # league: no bracket
            ],
            {"a": {"id": "a", "tournament_id": "t1", "name": "X"}},
        )
        out = svc.list_public_tournaments()
        self.assertTrue(all("champion" not in i for i in out["items"]))
        self.assertEqual(repo.gets, [])  # no team reads for tournaments without a winner

    def test_winner_from_another_tournament_is_ignored(self):
        svc, _ = mk(
            [{"id": "t1", "status": "finished", "bracket": final_with("a")}],
            {"a": {"id": "a", "tournament_id": "OTHER", "name": "Intruder"}},
        )
        self.assertNotIn("champion", svc.list_public_tournaments()["items"][0])

    def test_missing_team_record_is_ignored(self):
        svc, _ = mk([{"id": "t1", "status": "finished", "bracket": final_with("ghost")}], {})
        self.assertNotIn("champion", svc.list_public_tournaments()["items"][0])

    def test_status_filter_and_counts_unchanged(self):
        svc, _ = mk(
            [
                {"id": "t1", "status": "finished", "bracket": final_with("a")},
                {"id": "t2", "status": "active"},
            ],
            {"a": {"id": "a", "tournament_id": "t1", "name": "A"}},
        )
        out = svc.list_public_tournaments(status="finished")
        self.assertEqual([i["id"] for i in out["items"]], ["t1"])
        self.assertEqual(out["counts_by_status"], {"finished": 1, "active": 1})


if __name__ == "__main__":
    unittest.main()
