#!/usr/bin/env python3
"""Smoke test for the Tournaments API.

Exercises the full lifecycle: create tournament → groups → teams → players →
generate schedule → create match events → query standings/stats/bracket.

Usage:
    python migrations/tournament_smoke_test.py [BASE_URL]

    BASE_URL defaults to http://localhost:8085

Requirements:
    - API running locally (python app.py)
    - Valid JWT token (set TOKEN env var or paste below)
    - DynamoDB tables provisioned (local or remote)
"""

import os
import sys
import json
import requests

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8085"
TOKEN = os.getenv("TOKEN", "eyJraWQiOiJQWDhpUVwvb1JEOENwbU9pY2FwNkU0VVc4U2hsbjZ3dTRiZytSaE1WRzhZVT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiOGExNjNmMC0wMDAxLTcwOTAtMzg1NS00NjljMDQwODM0MzEiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLXdlc3QtMi5hbWF6b25hd3MuY29tXC91cy13ZXN0LTJfQ1R2TXJzeHRDIiwiY29nbml0bzp1c2VybmFtZSI6ImI4YTE2M2YwLTAwMDEtNzA5MC0zODU1LTQ2OWMwNDA4MzQzMSIsIm9yaWdpbl9qdGkiOiJjZjg2MThlZS1iYWFkLTRkOWQtOGI5My04OWU0ZGU4MjQ2MzciLCJhdWQiOiI2OTE0NjRndG1lMGNrbmRlcGJmNnU3ajFsbCIsImV2ZW50X2lkIjoiMDQzNjVhMjYtODgwMS00YTc4LTkxMTItZjA4NTEyNGYwYzAzIiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE3NjkzOTEyMTcsIm5hbWUiOiJKdWFuIFJvZHJpZ3VleiIsImV4cCI6MTc3MTk4MDU4MCwiY3VzdG9tOnJvbGUiOiJhZG1pbiIsImlhdCI6MTc3MTk3Njk4MCwianRpIjoiMTI0NjlhMzctNWY5Mi00MzE5LThlNjEtY2Q0MDQ0MjZjZjdiIiwiZW1haWwiOiJqZF9yb2RyaWd1ZXphQGphdmVyaWFuYS5lZHUuY28ifQ.AamB5Wedgql2dNoY1vIkJJRNXbfVgYPZpb4da7v85tx1FlKTQljc-mmq-ni0GrK19qy57cIyiviHFogH1QbLt9a7x5pQsSRz-kznWjV84nqLZ3vUjh4Y6Q2cnN-2jpmjjwj3tVe8SOvsKG5vYpp6sppnRfbw7eeMVuoir7yX3SLj0nmXKQ9OUfxAAuDysqjxLqyhTTtKxTvOFKANrhFYGZHP4xiYXKz9qf_FYuhCLd44FjZdHZPCJvOmI4asVpZRQnX1uLMApxJ7lO-KU6vjl5xYCG-Fw7aoEa_EaCnR4VWdfLeEmycNj0tQbjFFxhoUqsOIso6VKYh1ZnqSE8-miA")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "X-Account-Id": os.getenv("ACCOUNT_ID", "sportydev"),
}

passed = 0
failed = 0
ids = {}  # Store created IDs for chained requests


def test(method, path, expected_status, body=None, label=None):
    """Make an HTTP request and verify the status code."""
    global passed, failed
    url = f"{BASE_URL}{path}"
    label = label or f"{method} {path}"

    try:
        resp = requests.request(method, url, headers=HEADERS, json=body, timeout=30)
        if resp.status_code == expected_status:
            passed += 1
            print(f"  ✅ {label} → {resp.status_code}")
            try:
                return resp.json()
            except Exception:
                return resp.text
        else:
            failed += 1
            print(f"  ❌ {label} → {resp.status_code} (expected {expected_status})")
            print(f"     Response: {resp.text[:200]}")
            return None
    except Exception as e:
        failed += 1
        print(f"  ❌ {label} → ERROR: {e}")
        return None


def main():
    global ids

    print("\n" + "=" * 60)
    print("  TOURNAMENTS API — SMOKE TEST")
    print("=" * 60)

    # ── 1. Tournaments ────────────────────────────────────────────

    print("\n§1 Tournaments CRUD")

    data = test("POST", "/tournaments", 200, {
        "name": "Liga Interna 2026",
        "season": "2026-A",
        "type": "hybrid",
        "rules": {
            "points_per_win": 3,
            "points_per_draw": 1,
            "points_per_loss": 0,
            "total_matchweeks": 6,
            "legs": 1,
        }
    }, "Create tournament")
    if data:
        ids["tournament"] = data.get("id")

    test("GET", "/tournaments", 200, label="List tournaments")

    if ids.get("tournament"):
        test("GET", f"/tournaments/{ids['tournament']}", 200, label="Get tournament")

        test("PATCH", f"/tournaments/{ids['tournament']}", 200, {
            "name": "Liga Interna 2026 - Updated",
        }, "Update tournament")

    # ── 2. Groups ─────────────────────────────────────────────────

    print("\n§2 Groups")

    if ids.get("tournament"):
        data = test("POST", f"/tournaments/{ids['tournament']}/groups", 200, {
            "name": "Group A",
            "advancement_slots": 2,
        }, "Create group A")
        if data:
            ids["groupA"] = data.get("id")

        data = test("POST", f"/tournaments/{ids['tournament']}/groups", 200, {
            "name": "Group B",
            "advancement_slots": 2,
        }, "Create group B")
        if data:
            ids["groupB"] = data.get("id")

        test("GET", f"/tournaments/{ids['tournament']}/groups", 200, label="List groups")

        if ids.get("groupA"):
            test("PATCH", f"/tournaments/{ids['tournament']}/groups/{ids['groupA']}", 200, {
                "name": "Group A - Stars",
            }, "Update group A")

    # ── 3. Teams ──────────────────────────────────────────────────

    print("\n§3 Teams")

    team_names = [
        ("Team Alpha", "ALP"), ("Team Beta", "BET"),
        ("Team Gamma", "GAM"), ("Team Delta", "DEL"),
    ]

    if ids.get("tournament"):
        for i, (name, short) in enumerate(team_names):
            group_id = ids.get("groupA") if i < 2 else ids.get("groupB", "")
            data = test("POST", f"/tournaments/{ids['tournament']}/teams", 200, {
                "name": name,
                "short_name": short,
                "group_id": group_id,
                "seed": i + 1,
            }, f"Create {name}")
            if data:
                ids[f"team{i}"] = data.get("id")

        test("GET", f"/tournaments/{ids['tournament']}/teams", 200, label="List teams")

        if ids.get("team0"):
            test("GET", f"/tournaments/{ids['tournament']}/teams/{ids['team0']}", 200,
                 label="Get team detail")
            test("PATCH", f"/tournaments/{ids['tournament']}/teams/{ids['team0']}", 200, {
                "logo_url": "https://example.com/alpha.png",
            }, "Update team")

    # ── 4. Assign teams to groups ─────────────────────────────────

    print("\n§4 Assign teams to groups")

    if ids.get("groupA") and ids.get("team0"):
        test("POST",
             f"/tournaments/{ids['tournament']}/groups/{ids['groupA']}/teams",
             200, {"team_id": ids["team0"], "seed": 1},
             "Assign team0 to group A")

    if ids.get("groupA") and ids.get("team1"):
        test("POST",
             f"/tournaments/{ids['tournament']}/groups/{ids['groupA']}/teams",
             200, {"team_id": ids["team1"], "seed": 2},
             "Assign team1 to group A")

    # ── 5. Players ────────────────────────────────────────────────

    print("\n§5 Players")

    if ids.get("tournament") and ids.get("team0"):
        players_data = [
            ("Carlos Mendez", "Forward", 9),
            ("Luis Garcia", "Midfielder", 10),
            ("Pedro Ruiz", "Defender", 4),
        ]
        for i, (name, pos, num) in enumerate(players_data):
            data = test("POST",
                        f"/tournaments/{ids['tournament']}/teams/{ids['team0']}/players",
                        200, {
                            "name": name,
                            "position": pos,
                            "number": num,
                        }, f"Create player {name}")
            if data:
                ids[f"player{i}"] = data.get("id")

        test("GET", f"/tournaments/{ids['tournament']}/players", 200,
             label="List all players")

        test("GET", f"/tournaments/{ids['tournament']}/players?team_id={ids['team0']}", 200,
             label="List players filtered by team")

        if ids.get("player0"):
            test("GET", f"/tournaments/{ids['tournament']}/players/{ids['player0']}", 200,
                 label="Get player detail (with stats)")

            test("PATCH", f"/tournaments/{ids['tournament']}/players/{ids['player0']}", 200, {
                "number": 11,
            }, "Update player number")

    # ── 6. Matches ────────────────────────────────────────────────

    print("\n§6 Matches")

    if ids.get("tournament") and ids.get("team0") and ids.get("team1"):
        data = test("POST", f"/tournaments/{ids['tournament']}/matches", 200, {
            "home_team_id": ids["team0"],
            "away_team_id": ids["team1"],
            "date": "2026-03-01T15:00:00",
            "venue": "Stadium Central",
            "matchweek": 1,
        }, "Create match")
        if data:
            ids["match0"] = data.get("id")

        test("GET", f"/tournaments/{ids['tournament']}/matches", 200,
             label="List matches")

        test("GET", f"/tournaments/{ids['tournament']}/matches?matchweek=1", 200,
             label="List matches by matchweek")

        if ids.get("match0"):
            test("GET", f"/tournaments/{ids['tournament']}/matches/{ids['match0']}", 200,
                 label="Get match detail")

            # Start match
            test("PATCH", f"/tournaments/{ids['tournament']}/matches/{ids['match0']}", 200, {
                "status": "live",
            }, "Update match → live")

    # ── 7. Match Events ───────────────────────────────────────────

    print("\n§7 Match Events")

    if ids.get("match0") and ids.get("player0"):
        data = test("POST", f"/tournaments/matches/{ids['match0']}/events", 200, {
            "type": "goal",
            "minute": 23,
            "player_id": ids["player0"],
            "team_id": ids["team0"],
        }, "Create goal event")
        if data:
            ids["event0"] = data.get("id")

        test("POST", f"/tournaments/matches/{ids['match0']}/events", 200, {
            "type": "yellow_card",
            "minute": 45,
            "player_id": ids.get("player1", ids["player0"]),
            "team_id": ids["team0"],
        }, "Create yellow card event")

        if ids.get("event0"):
            test("PATCH", f"/tournaments/matches/{ids['match0']}/events/{ids['event0']}", 200, {
                "stoppage_time": 2,
            }, "Update event (add stoppage time)")

    # ── 8. Finish match & query standings ─────────────────────────

    print("\n§8 Finish match & Standings")

    if ids.get("match0"):
        test("PATCH", f"/tournaments/{ids['tournament']}/matches/{ids['match0']}", 200, {
            "status": "finished",
            "score_home": 2,
            "score_away": 1,
        }, "Finish match with score")

    if ids.get("tournament"):
        test("GET", f"/tournaments/{ids['tournament']}/standings", 200,
             label="Get standings")

    if ids.get("tournament") and ids.get("groupA"):
        test("GET",
             f"/tournaments/{ids['tournament']}/groups/{ids['groupA']}/standings",
             200, label="Get group standings")

    # ── 9. Stats ──────────────────────────────────────────────────

    print("\n§9 Stats")

    if ids.get("tournament"):
        data = test("GET", f"/tournaments/{ids['tournament']}/stats", 200,
                     label="Get tournament stats")
        if data:
            print(f"     Stats: {json.dumps(data, indent=2)[:300]}")

    # ── 10. Bracket ───────────────────────────────────────────────

    print("\n§10 Bracket")

    if ids.get("tournament"):
        test("GET", f"/tournaments/{ids['tournament']}/bracket", 200,
             label="Get bracket (empty)")

        team_seeds = []
        for i in range(4):
            tid = ids.get(f"team{i}")
            if tid:
                team_seeds.append({"team_id": tid, "seed": i + 1})

        if len(team_seeds) >= 2:
            test("POST", f"/tournaments/{ids['tournament']}/bracket:generate", 200, {
                "source": "seeds",
                "teams": team_seeds,
            }, "Generate bracket from seeds")

            test("GET", f"/tournaments/{ids['tournament']}/bracket", 200,
                 label="Get bracket (generated)")

    # ── 11. Fixture Generation ────────────────────────────────────

    print("\n§11 Fixture Generation")

    if ids.get("tournament"):
        test("POST", f"/tournaments/{ids['tournament']}/schedule:generate", 200, {
            "start_date": "2026-04-01T10:00:00",
            "match_interval_days": 7,
            "default_venue": "Main Stadium",
        }, "Generate round-robin schedule")

        test("POST", f"/tournaments/{ids['tournament']}/matches:bulk", 200, {
            "matches": [{
                "home_team_id": ids.get("team2", ids["team0"]),
                "away_team_id": ids.get("team3", ids["team1"]),
                "date": "2026-05-01T15:00:00",
                "venue": "Secondary Stadium",
                "matchweek": 99,
            }]
        }, "Bulk create matches")

    # ── 12. Validation errors ─────────────────────────────────────

    print("\n§12 Validation errors")

    if ids.get("tournament") and ids.get("team0"):
        test("POST", f"/tournaments/{ids['tournament']}/matches", 400, {
            "home_team_id": ids["team0"],
            "away_team_id": ids["team0"],
            "date": "2026-06-01T15:00:00",
        }, "Reject: same team home & away")

    test("GET", "/tournaments/nonexistent_id", 404, label="404 on missing tournament")

    # ── 13. Cleanup ───────────────────────────────────────────────

    print("\n§13 Cleanup")

    if ids.get("event0"):
        test("DELETE", f"/tournaments/matches/{ids['match0']}/events/{ids['event0']}", 200,
             label="Delete event")

    if ids.get("player0"):
        test("DELETE", f"/tournaments/{ids['tournament']}/players/{ids['player0']}", 200,
             label="Delete player")

    if ids.get("match0"):
        test("DELETE", f"/tournaments/{ids['tournament']}/matches/{ids['match0']}", 200,
             label="Delete match")

    if ids.get("team0"):
        test("DELETE", f"/tournaments/{ids['tournament']}/teams/{ids['team0']}", 200,
             label="Delete team")

    if ids.get("groupA") and ids.get("tournament"):
        test("DELETE", f"/tournaments/{ids['tournament']}/groups/{ids['groupA']}", 200,
             label="Delete group")

    if ids.get("tournament"):
        test("DELETE", f"/tournaments/{ids['tournament']}", 200,
             label="Delete tournament")

    # ── Summary ───────────────────────────────────────────────────

    print("\n" + "=" * 60)
    total = passed + failed
    print(f"  RESULTS: {passed}/{total} passed, {failed} failed")
    print("=" * 60 + "\n")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
