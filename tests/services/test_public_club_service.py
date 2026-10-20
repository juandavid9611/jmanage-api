import json
import os
import unittest
from decimal import Decimal

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from services import public_club_service as pcs
from services.public_club_service import PublicClubService
from services.tournament_service import TournamentService

NOW = 1_800_000_000.0  # fixed "now": 2027-01-15T08:00:00Z
DAY = 86400


def ms(offset_days: float) -> int:
    return int((NOW + offset_days * DAY) * 1000)


class FakeAccountRepo:
    def __init__(self, accounts):
        self.accounts = accounts
        self.gets = 0

    def get(self, account_id):
        self.gets += 1
        return self.accounts.get(account_id)


class FakeMembershipRepo:
    def __init__(self, items):
        self.items = items

    def list_by_account(self, account_id):
        return self.items


class FakeWorkspaceRepo:
    def __init__(self, n):
        self.n = n

    def list_all(self, account_id):
        if isinstance(self.n, list):
            return self.n
        return [{"id": f"ws{i}", "name": f"Sub {i}", "plan": "ws-plan-secret", "logo_key": "wslogo.png"} for i in range(self.n)]


class FakeTourRepo:
    def __init__(self, tours):
        self.tours = tours
        self.calls = []

    def list_for_public_overview(self, account_id, max_items):
        self.calls.append((account_id, max_items))
        return self.tours


class FakeTournamentRepo:
    def __init__(self, items):
        self.items = items

    def list_by_account(self, account_id):
        return [dict(i) for i in self.items]


class FakeTeamRepo:
    def __init__(self, teams):
        self.teams = teams

    def get(self, team_id):
        return self.teams.get(team_id)


class FakeS3:
    def presign_get_from_explicit_key(self, key, content_type):
        return f"https://signed/{key}"

    def get_s3_public_url(self, key):
        return f"https://bucket.s3.us-west-2.amazonaws.com/{key}"


def account(enabled=True, account_type="club", logo="acc/logo.png"):
    settings = {"account_type": account_type, "default_workspace": "ws_main", "timezone": "America/Bogota"}
    if enabled is not None:
        settings["public_world_enabled"] = enabled
    return {
        "id": "vittoriacd",
        "name": "Vittoria CD",
        "settings": settings,
        "subscription": {"plan": "paid"},
        "branding": {"logo_url": logo},
        "owner_email": "owner@secret.example",
    }


def booker(name, approved=True, **extra):
    return {
        "id": f"u-{name}", "name": name, "email": f"{name}@secret.example", "phone": "+57 300 000 0000",
        "avatarUrl": f"https://cdn/avatars/{name}.png", "approved": approved, "late": False,
        "goals": 2, "assists": 1, "paid": True, **extra,
    }


def match(title, start_days, home=None, away=None, bookers=None):
    t = {
        "id": f"tour_{title}", "event_type": "match", "tour_name": title,
        "available": {"startDate": ms(start_days), "endDate": ms(start_days + 0.1)},
        "bookers": bookers or {},
    }
    if home is not None:
        t["scores"] = {"home": home, "away": away}
    return t


def training(start_days, bookers=None):
    return {
        "id": f"tour_tr{start_days}", "event_type": "training", "tour_name": "Entrenamiento",
        "available": {"startDate": ms(start_days)}, "bookers": bookers or {},
    }


def build(acc=None, tours=None, tournaments=None, teams=None, members=None, categories=3, accounts=None):
    acc = acc if acc is not None else account()
    tournament_repo = FakeTournamentRepo(tournaments or [])
    tsvc = TournamentService(tournament_repo, team_repo=FakeTeamRepo(teams or {}), s3=FakeS3())
    return PublicClubService(
        account_repo=FakeAccountRepo(accounts if accounts is not None else {"vittoriacd": acc}),
        membership_repo=FakeMembershipRepo(members or []),
        workspace_repo=FakeWorkspaceRepo(categories),
        tour_repo=FakeTourRepo(tours or []),
        tournament_repo=tournament_repo,
        tournament_service=tsvc,
        s3=FakeS3(),
        clock=lambda: NOW,
    )


def members(n_players=4):
    out = [{"user_id": f"p{i}", "role": "user", "status": "active", "workspace_id": "ws0"} for i in range(n_players)]
    out += [
        {"user_id": "p0", "role": "user", "status": "active", "workspace_id": "ws1"},  # same person, 2nd category
        {"user_id": "pi", "role": "user", "status": "inactive", "workspace_id": "ws0"},
        {"user_id": "adm", "role": "admin", "status": "active", "workspace_id": "ws0"},
        {"user_id": "coach", "role": "coach", "status": "active", "workspace_id": "ws0"},
    ]
    return out


class TestNotFound(unittest.TestCase):
    def test_missing_account(self):
        self.assertIsNone(build(accounts={}).get_overview("ghost"))

    def test_disabled_false_none_or_missing_flag(self):
        for flag in (False, None):
            self.assertIsNone(build(acc=account(enabled=flag)).get_overview("vittoriacd"))

    def test_truthy_but_not_true_is_disabled(self):
        for flag in ("true", 1, "yes"):
            acc = account()
            acc["settings"]["public_world_enabled"] = flag
            self.assertIsNone(build(acc=acc).get_overview("vittoriacd"))

    def test_tournament_account(self):
        self.assertIsNone(build(acc=account(account_type="tournament")).get_overview("vittoriacd"))

    def test_disabled_does_not_read_anything_else(self):
        svc = build(acc=account(enabled=False))
        svc.get_overview("vittoriacd")
        self.assertEqual(svc.tour_repo.calls, [])


class TestAggregates(unittest.TestCase):
    def test_wld_goals_pending_and_counts(self):
        tours = [
            match("A", -10, 3, 1),   # win
            match("B", -9, 0, 0),    # draw
            match("C", -8, 1, 2),    # loss
            match("D", -7, 2, 0),    # win
            match("E", -6),          # past, no scores => pending
            match("F", 5, 0, 0),     # future: not played even with scores
            training(-5), training(-4), training(3),
        ]
        out = build(tours=tours, members=members(), categories=3).get_overview("vittoriacd")
        s = out["stats"]
        self.assertEqual((s["matchesPlayed"], s["wins"], s["draws"], s["losses"]), (4, 2, 1, 1))
        self.assertEqual((s["goalsFor"], s["goalsAgainst"]), (6, 3))
        self.assertEqual(s["trainingsHeld"], 2)
        self.assertEqual(s["activePlayers"], 4)
        self.assertEqual(s["categories"], 3)

    def test_recent_matches_order_outcomes_and_pending(self):
        tours = [match("A", -10, 3, 1), match("E", -6), match("C", -8, 1, 2), match("B", -9, 1, 1)]
        out = build(tours=tours).get_overview("vittoriacd")
        self.assertEqual([m["title"] for m in out["recentMatches"]], ["E", "C", "B", "A"])
        self.assertEqual([m["outcome"] for m in out["recentMatches"]], ["pending", "loss", "draw", "win"])
        self.assertIsNone(out["recentMatches"][0]["homeScore"])
        self.assertEqual((out["recentMatches"][1]["homeScore"], out["recentMatches"][1]["awayScore"]), (1, 2))

    def test_attendance_pct(self):
        tours = [
            training(-3, {"a": booker("a", True), "b": booker("b", True), "c": booker("c", False), "d": booker("d", False)}),
            match("A", -2, 1, 0, {"e": booker("e", True), "f": booker("f", False)}),
            training(-1),  # no bookers: ignored
            training(4, {"z": booker("z", True)}),  # future: ignored
        ]
        s = build(tours=tours).get_overview("vittoriacd")["stats"]
        self.assertEqual(s["averageAttendancePct"], 50.0)

    def test_attendance_null_without_data(self):
        s = build(tours=[training(-1), match("A", -2, 1, 0)]).get_overview("vittoriacd")["stats"]
        self.assertIsNone(s["averageAttendancePct"])

    def test_decimal_and_string_scores(self):
        t = match("A", -2)
        t["scores"] = {"home": Decimal("2"), "away": "1"}
        bad = match("B", -3)
        bad["scores"] = {"home": "x", "away": None}
        out = build(tours=[t, bad]).get_overview("vittoriacd")
        self.assertEqual(out["stats"]["wins"], 1)
        self.assertEqual(out["stats"]["matchesPlayed"], 1)
        self.assertEqual(out["recentMatches"][1]["outcome"], "pending")

    def test_iso_and_second_timestamps_parse_and_bad_dates_skipped(self):
        iso = match("ISO", 0, 1, 0)
        iso["available"] = {"startDate": "2027-01-02T10:00:00.000Z"}
        sec = match("SEC", -1, 0, 1)
        sec["available"] = {"startDate": int(NOW - DAY)}
        junk = match("JUNK", -1, 1, 1)
        junk["available"] = {"startDate": "not a date"}
        out = build(tours=[iso, sec, junk]).get_overview("vittoriacd")
        self.assertEqual(out["stats"]["matchesPlayed"], 2)
        self.assertEqual(out["recentMatches"][0]["date"][:10], "2027-01-14")
        self.assertEqual(out["recentMatches"][1]["date"][:10], "2027-01-02")

    def test_other_event_types_ignored(self):
        t = match("X", -1, 5, 0)
        t["event_type"] = "friendly_dinner"
        self.assertEqual(build(tours=[t]).get_overview("vittoriacd")["stats"]["matchesPlayed"], 0)


class TestCaps(unittest.TestCase):
    def test_recent_8_upcoming_5(self):
        tours = [match(f"P{i}", -i - 1, 1, 0) for i in range(30)] + [match(f"U{i}", i + 1) for i in range(30)]
        out = build(tours=tours).get_overview("vittoriacd")
        self.assertEqual(len(out["recentMatches"]), 8)
        self.assertEqual(out["recentMatches"][0]["title"], "P0")
        self.assertEqual([m["title"] for m in out["upcomingMatches"]], ["U0", "U1", "U2", "U3", "U4"])
        self.assertEqual(out["stats"]["matchesPlayed"], 30)

    def test_tournaments_capped_and_title_truncated(self):
        ts = [{"id": f"t{i}", "name": "N", "status": "draft", "is_public": True, "created_at": f"2026-01-{i + 1:02d}"} for i in range(25)]
        out = build(tournaments=ts, tours=[match("x" * 500, 3)]).get_overview("vittoriacd")
        self.assertEqual(len(out["tournaments"]), pcs.MAX_TOURNAMENTS)
        self.assertEqual(len(out["upcomingMatches"][0]["title"]), pcs.MAX_TITLE_LEN)

    def test_repo_scan_is_bounded(self):
        svc = build()
        svc.get_overview("vittoriacd")
        self.assertEqual(svc.tour_repo.calls, [("vittoriacd", pcs.MAX_TOURS_SCANNED)])


class TestTournaments(unittest.TestCase):
    def test_only_public_and_champion(self):
        final = {"final": [{"winner_team_id": "tm1"}]}
        ts = [
            {"id": "t1", "name": "Copa", "season": "2026", "status": "finished", "is_public": True, "bracket": final,
             "created_at": "2026-02-01", "account_id": "vittoriacd", "payments_enabled": True, "location": "secret"},
            {"id": "t2", "name": "Privado", "status": "active", "is_public": False},
            {"id": "t3", "name": "SinFlag", "status": "active"},
            {"id": "t4", "name": "Liga", "season": 2026, "status": "active", "is_public": True, "created_at": "2026-03-01"},
        ]
        teams = {"tm1": {"id": "tm1", "tournament_id": "t1", "name": "Samba FC", "short_name": "SFC",
                         "logo_url": "k/l.png", "manager_email": "m@secret.example", "manager_name": "Boss"}}
        out = build(tournaments=ts, teams=teams).get_overview("vittoriacd")
        self.assertEqual([t["id"] for t in out["tournaments"]], ["t4", "t1"])
        self.assertIsNone(out["tournaments"][0]["champion"])
        self.assertEqual(out["tournaments"][0]["season"], "2026")
        self.assertEqual(out["tournaments"][1]["champion"],
                         {"name": "Samba FC", "shortName": "SFC", "logoUrl": "https://signed/k/l.png"})


class TestPrivacy(unittest.TestCase):
    EXPECTED = {
        "top": {"club", "stats", "categories", "recentMatches", "upcomingMatches", "tournaments"},
        "category": {"id", "name", "activePlayers"},
        "club": {"name", "logoUrl"},
        "stats": {"activePlayers", "categories", "matchesPlayed", "wins", "draws", "losses", "goalsFor",
                  "goalsAgainst", "trainingsHeld", "averageAttendancePct"},
        "recent": {"date", "title", "homeScore", "awayScore", "outcome"},
        "upcoming": {"date", "title"},
        "tournament": {"id", "name", "status", "season", "champion"},
        "champion": {"name", "shortName", "logoUrl"},
    }
    SECRETS = ("secret.example", "+57 300", "avatars/", "u-alice", "u-bob", "Boss", "owner@", "ws_main", "ws-plan-secret", "wslogo")

    def _out(self):
        final = {"final": [{"winner_team_id": "tm1"}]}
        tours = [
            match("Rival FC", -3, 2, 1, {"a": booker("alice"), "b": booker("bob", False)}),
            match("Next FC", 4, bookers={"a": booker("alice")}),
            training(-2, {"a": booker("alice"), "b": booker("bob")}),
        ]
        ts = [{"id": "t1", "name": "Copa", "season": "2026", "status": "finished", "is_public": True,
               "bracket": final, "created_at": "x", "account_id": "vittoriacd"}]
        teams = {"tm1": {"id": "tm1", "tournament_id": "t1", "name": "Samba", "short_name": "SMB",
                         "logo_url": "k.png", "manager_email": "m@secret.example", "manager_name": "Boss"}}
        return build(tours=tours, tournaments=ts, teams=teams, members=members()).get_overview("vittoriacd")

    def test_exact_allowlist_at_every_level(self):
        out = self._out()
        e = self.EXPECTED
        self.assertEqual(set(out), e["top"])
        self.assertEqual(set(out["club"]), e["club"])
        self.assertEqual(set(out["stats"]), e["stats"])
        self.assertTrue(out["categories"])
        for c in out["categories"]:
            self.assertEqual(set(c), e["category"])
        for m in out["recentMatches"]:
            self.assertEqual(set(m), e["recent"])
        for m in out["upcomingMatches"]:
            self.assertEqual(set(m), e["upcoming"])
        for t in out["tournaments"]:
            self.assertEqual(set(t), e["tournament"])
            if t["champion"]:
                self.assertEqual(set(t["champion"]), e["champion"])
        self.assertTrue(out["recentMatches"] and out["upcomingMatches"] and out["tournaments"][0]["champion"])

    def test_no_pii_like_keys_at_any_depth_and_no_pii_values(self):
        out = self._out()
        bad_keys = ("email", "phone", "avatar", "userid", "user_id", "booker", "manager", "owner", "paid", "assist")

        def walk(node):
            if isinstance(node, dict):
                for k, v in node.items():
                    for bad in bad_keys:
                        self.assertNotIn(bad, k.lower(), f"PII-looking key {k}")
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        walk(out)
        blob = json.dumps(out)
        for secret in self.SECRETS:
            self.assertNotIn(secret, blob)
        for person in ("alice", "bob"):
            self.assertNotIn(person, blob.lower())

    def test_values_are_scalars_only(self):
        out = self._out()
        for m in out["recentMatches"] + out["upcomingMatches"]:
            for v in m.values():
                self.assertIsInstance(v, (str, int, type(None)))

    def test_logo_resolution(self):
        self.assertEqual(self._out()["club"]["logoUrl"], "https://bucket.s3.us-west-2.amazonaws.com/acc/logo.png")
        self.assertIsNone(build(acc=account(logo="")).get_overview("vittoriacd")["club"]["logoUrl"])
        self.assertEqual(build(acc=account(logo="https://x/y.png")).get_overview("vittoriacd")["club"]["logoUrl"], "https://x/y.png")


def ws(i, name=None):
    return {"id": f"c{i}", "name": name if name is not None else f"Cat {i}", "plan": "x", "logo_key": "y"}


def mem(user, workspace, role="user", status="active"):
    return {"user_id": user, "workspace_id": workspace, "role": role, "status": status}


class TestCategories(unittest.TestCase):
    def _cats(self, workspaces, members_):
        out = build(categories=workspaces, members=members_).get_overview("vittoriacd")
        self.assertEqual(out["stats"]["categories"], len(out["categories"]))
        return out["categories"]

    def test_per_category_counts(self):
        cats = self._cats(
            [ws(1, "Sub 12"), ws(2, "Sub 14"), ws(3, "Empty")],
            [
                mem("a", "c1"), mem("b", "c1"), mem("a", "c2"),  # a is in two categories: counted in both
                mem("a", "c1"),  # duplicate membership row: still one person
                mem("adm", "c1", role="admin"), mem("co", "c1", role="coach"),
                mem("off", "c2", status="inactive"), mem("dis", "c2", status="disabled"),
                mem("x", "other_ws"),
            ],
        )
        self.assertEqual(cats, [
            {"id": "c3", "name": "Empty", "activePlayers": 0},
            {"id": "c1", "name": "Sub 12", "activePlayers": 2},
            {"id": "c2", "name": "Sub 14", "activePlayers": 1},
        ])

    def test_club_total_still_distinct_people(self):
        out = build(categories=[ws(1), ws(2)], members=[mem("a", "c1"), mem("a", "c2"), mem("b", "c2")]) \
            .get_overview("vittoriacd")
        self.assertEqual(out["stats"]["activePlayers"], 2)

    def test_sorted_by_name_case_insensitive(self):
        cats = self._cats([ws(1, "beta"), ws(2, "Alpha"), ws(3, "Charlie"), ws(4, "alpha2")], [])
        self.assertEqual([c["name"] for c in cats], ["Alpha", "alpha2", "beta", "Charlie"])

    def test_capped_at_30(self):
        cats = self._cats([ws(i, f"Cat {i:03d}") for i in range(45)], [])
        self.assertEqual(len(cats), pcs.MAX_CATEGORIES)
        self.assertEqual(cats[0]["name"], "Cat 000")

    def test_no_categories(self):
        self.assertEqual(self._cats([], [mem("a", "c1")]), [])


class TestCache(unittest.TestCase):
    def test_cached_for_60s_then_refreshed(self):
        t = [NOW]
        svc = build()
        svc._clock = lambda: t[0]
        svc.get_overview("vittoriacd")
        svc.get_overview("vittoriacd")
        self.assertEqual(svc.account_repo.gets, 1)
        t[0] += pcs.CACHE_TTL_SECONDS + 1
        svc.get_overview("vittoriacd")
        self.assertEqual(svc.account_repo.gets, 2)

    def test_404_cached_and_cache_bounded(self):
        svc = build(accounts={})
        for i in range(pcs.CACHE_MAX_ENTRIES + 50):
            svc.get_overview(f"ghost{i}")
        self.assertLessEqual(len(svc._cache), pcs.CACHE_MAX_ENTRIES)


if __name__ == "__main__":
    unittest.main()
