# Match Push Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send push/in-app notifications to everyone registered in a tournament when a match starts, when a match event is recorded, and when a match finishes.

**Architecture:** Reuse the existing `Notifications` facade (OneSignal push + DynamoDB in-app feed, addressed by user email) by adding three new broadcast methods. A new pure function resolves "all tournament users" from existing team/invitation data, and a new thin `TournamentMatchNotificationService` combines audience resolution + the facade calls, injected into the two existing match services at their status-transition and event-creation hook points. No new API routes, no new delivery channel, no new tables.

**Tech Stack:** Python 3.12, FastAPI, DynamoDB (boto3), existing `Notifications`/OneSignal facade. Tests use the stdlib `unittest` + `unittest.mock` — this repo has no test framework installed today (no pytest, no `tests/` directory), so this plan does not introduce a new dependency for testing.

**Spec:** `docs/superpowers/specs/2026-08-20-match-push-notifications-design.md`

## Global Constraints

- No new notification delivery channel — everything goes through the existing `Notifications` facade → `_send_bulk_in_app_notification` (OneSignal push + `NotificationRepo` in-app feed), addressed by email.
- "All users of the tournament" = deduplicated union of team `contact_email` (`TournamentTeamRepo.list_by_tournament`) and `email` from invitations with `status == "accepted"` (`TournamentInvitationRepo.list_by_tournament`).
- A notification failure must never fail a match update or event write. All notification error handling lives inside `TournamentMatchNotificationService`; callers (`TournamentMatchService`, `TournamentMatchEventService`) do not add their own try/except around it.
- All new user-facing copy is in Spanish, matching the existing facade methods' tone.
- No new API routes — hooks live inside existing `update_match` and `create_event` service methods.
- Follow the existing manual-DI pattern in `di.py` (plain `get_*_service()`/`get_*_repo()` factory functions) — do not introduce a DI framework.
- Tests use stdlib `unittest`/`unittest.mock` only; run with `python -m unittest <dotted.module.path> -v` from the repo root with the venv active (`source .venv/bin/activate`).

---

### Task 1: Tournament audience resolver

**Files:**
- Create: `services/tournament_audience.py`
- Test: `tests/services/test_tournament_audience.py`

**Interfaces:**
- Consumes: `TournamentTeamRepo.list_by_tournament(tournament_id) -> list[dict]` (each dict may have `contact_email`), `TournamentInvitationRepo.list_by_tournament(tournament_id) -> list[dict]` (each dict may have `email`, `status`).
- Produces: `resolve_tournament_user_emails(tournament_id: str, team_repo, invitation_repo) -> list[str]` — sorted, deduplicated list of emails. Used by Task 3.

- [ ] **Step 1: Write the failing tests**

Create `tests/services/test_tournament_audience.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.services.test_tournament_audience -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'services.tournament_audience'`

- [ ] **Step 3: Write the implementation**

Create `services/tournament_audience.py`:

```python
"""Resolves the notification audience for a tournament — the deduplicated
set of user emails associated with it, derived from existing team and
invitation data (there is no dedicated followers/roster table)."""

from typing import Any

from repositories.tournament_invitation_repo_ddb import TournamentInvitationRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo


def resolve_tournament_user_emails(
    tournament_id: str,
    team_repo: TournamentTeamRepo,
    invitation_repo: TournamentInvitationRepo,
) -> list[str]:
    emails: set[str] = set()

    for team in team_repo.list_by_tournament(tournament_id):
        contact_email = (team.get("contact_email") or "").strip()
        if contact_email:
            emails.add(contact_email)

    for invitation in invitation_repo.list_by_tournament(tournament_id):
        if invitation.get("status") != "accepted":
            continue
        email = (invitation.get("email") or "").strip()
        if email:
            emails.add(email)

    return sorted(emails)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.services.test_tournament_audience -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add services/tournament_audience.py tests/services/test_tournament_audience.py
git commit -m "feat: add tournament audience resolver for notifications"
```

---

### Task 2: `Notifications` facade additions

**Files:**
- Modify: `services/notification_orchestator.py`
- Test: `tests/services/test_notification_orchestator_match.py`

**Interfaces:**
- Consumes: `self._send_bulk_in_app_notification(*, user_emails, title, content, category, action_url_path) -> str` (existing, `services/notification_orchestator.py:81-97`).
- Produces:
  - `Notifications.match_started(*, user_emails, tournament_name, home_team_name, away_team_name) -> str`
  - `Notifications.match_event_created(*, user_emails, tournament_name, event_type, team_name, player_name, minute) -> str`
  - `Notifications.match_finished(*, user_emails, tournament_name, home_team_name, away_team_name, score_home, score_away) -> str`
  Used by Task 3.

- [ ] **Step 1: Write the failing tests**

Create `tests/services/test_notification_orchestator_match.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.services.test_notification_orchestator_match -v`
Expected: FAIL — `AttributeError: 'Notifications' object has no attribute 'match_started'`

- [ ] **Step 3: Write the implementation**

In `services/notification_orchestator.py`, add a class-level template map after the existing `COURIER_TEMPLATE_*` constants (after line 30):

```python
    # ── Match notification copy ─────────────────────────────────────
    _MATCH_EVENT_TEMPLATES: dict[str, tuple[str, str]] = {
        "goal": ("¡Gol!", "Gol de {player_name} ({team_name}) al minuto {minute}."),
        "penalty_scored": ("¡Gol!", "Gol de {player_name} ({team_name}) al minuto {minute}."),
        "own_goal": ("Autogol", "Autogol de {player_name} ({team_name}) al minuto {minute}."),
        "yellow_card": ("Tarjeta amarilla", "Amarilla para {player_name} ({team_name}) al minuto {minute}."),
        "second_yellow": ("Tarjeta amarilla", "Amarilla para {player_name} ({team_name}) al minuto {minute}."),
        "red_card": ("Tarjeta roja", "Roja para {player_name} ({team_name}) al minuto {minute}."),
        "penalty_missed": ("Penal fallado", "{player_name} ({team_name}) falló un penal al minuto {minute}."),
        "substitution": ("Cambio", "Cambio en {team_name} al minuto {minute}."),
    }
```

Then add these three methods at the end of the class, after `votation_opened` (after line 443, before `_get_formatted_notification_field`):

```python
    def match_started(
        self,
        *,
        user_emails: list[str],
        tournament_name: str,
        home_team_name: str,
        away_team_name: str,
    ) -> str:
        return self._send_bulk_in_app_notification(
            user_emails=user_emails,
            title="¡Comenzó el partido!",
            content=f"{home_team_name} vs {away_team_name} — {tournament_name}",
            category="match_started",
            action_url_path="dashboard/tournaments",
        )

    def match_event_created(
        self,
        *,
        user_emails: list[str],
        tournament_name: str,
        event_type: str,
        team_name: str,
        player_name: str | None,
        minute: int,
    ) -> str:
        title, body_template = self._MATCH_EVENT_TEMPLATES.get(
            event_type,
            ("Evento del partido", "{team_name} — minuto {minute}."),
        )
        body = body_template.format(
            player_name=player_name or "Jugador",
            team_name=team_name,
            minute=minute,
        )
        return self._send_bulk_in_app_notification(
            user_emails=user_emails,
            title=title,
            content=f"{body} — {tournament_name}",
            category="match_event",
            action_url_path="dashboard/tournaments",
        )

    def match_finished(
        self,
        *,
        user_emails: list[str],
        tournament_name: str,
        home_team_name: str,
        away_team_name: str,
        score_home: int,
        score_away: int,
    ) -> str:
        return self._send_bulk_in_app_notification(
            user_emails=user_emails,
            title="Final del partido",
            content=f"{home_team_name} {score_home} - {score_away} {away_team_name} — {tournament_name}",
            category="match_finished",
            action_url_path="dashboard/tournaments",
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.services.test_notification_orchestator_match -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add services/notification_orchestator.py tests/services/test_notification_orchestator_match.py
git commit -m "feat: add match_started/match_event_created/match_finished to Notifications facade"
```

---

### Task 3: `TournamentMatchNotificationService`

**Files:**
- Create: `services/tournament_match_notification_service.py`
- Test: `tests/services/test_tournament_match_notification_service.py`

**Interfaces:**
- Consumes: `resolve_tournament_user_emails(tournament_id, team_repo, invitation_repo) -> list[str]` (Task 1); `Notifications.match_started/match_event_created/match_finished` (Task 2).
- Produces: `TournamentMatchNotificationService(team_repo, invitation_repo, notifications)` with methods:
  - `match_started(*, tournament: dict, home_team_name: str, away_team_name: str) -> None`
  - `match_event_created(*, tournament: dict, event: dict, team_name: str, player_name: str | None) -> None`
  - `match_finished(*, tournament: dict, home_team_name: str, away_team_name: str, score_home: int, score_away: int) -> None`
  Never raises. Used by Task 4 and Task 5.

- [ ] **Step 1: Write the failing tests**

Create `tests/services/test_tournament_match_notification_service.py`:

```python
import unittest
from unittest.mock import MagicMock

from services.tournament_match_notification_service import TournamentMatchNotificationService


class TestTournamentMatchNotificationService(unittest.TestCase):
    def setUp(self):
        self.team_repo = MagicMock()
        self.invitation_repo = MagicMock()
        self.notifications = MagicMock()
        self.team_repo.list_by_tournament.return_value = [
            {"id": "tm_1", "contact_email": "coach@example.com"},
        ]
        self.invitation_repo.list_by_tournament.return_value = []
        self.service = TournamentMatchNotificationService(
            team_repo=self.team_repo,
            invitation_repo=self.invitation_repo,
            notifications=self.notifications,
        )
        self.tournament = {"id": "trn_1", "name": "Liga 2026"}

    def test_match_started_resolves_audience_and_sends(self):
        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.notifications.match_started.assert_called_once_with(
            user_emails=["coach@example.com"],
            tournament_name="Liga 2026",
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

    def test_match_event_created_resolves_audience_and_sends(self):
        event = {"type": "goal", "minute": 34}

        self.service.match_event_created(
            tournament=self.tournament,
            event=event,
            team_name="Halcones",
            player_name="Juan Perez",
        )

        self.notifications.match_event_created.assert_called_once_with(
            user_emails=["coach@example.com"],
            tournament_name="Liga 2026",
            event_type="goal",
            team_name="Halcones",
            player_name="Juan Perez",
            minute=34,
        )

    def test_match_finished_resolves_audience_and_sends(self):
        self.service.match_finished(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )

        self.notifications.match_finished.assert_called_once_with(
            user_emails=["coach@example.com"],
            tournament_name="Liga 2026",
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )

    def test_no_recipients_skips_send(self):
        self.team_repo.list_by_tournament.return_value = []

        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.notifications.match_started.assert_not_called()

    def test_missing_tournament_id_skips_send_without_raising(self):
        self.service.match_started(
            tournament={},
            home_team_name="Halcones",
            away_team_name="Tigres",
        )

        self.notifications.match_started.assert_not_called()

    def test_swallows_exception_from_audience_resolution(self):
        self.team_repo.list_by_tournament.side_effect = Exception("ddb down")

        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )  # must not raise

        self.notifications.match_started.assert_not_called()

    def test_swallows_exception_from_notifications_send(self):
        self.notifications.match_started.side_effect = Exception("onesignal down")

        self.service.match_started(
            tournament=self.tournament,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )  # must not raise


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.services.test_tournament_match_notification_service -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'services.tournament_match_notification_service'`

- [ ] **Step 3: Write the implementation**

Create `services/tournament_match_notification_service.py`:

```python
"""Sends match-lifecycle push notifications to a tournament's audience.
Wraps audience resolution + the Notifications facade so the calling
services (TournamentMatchService, TournamentMatchEventService) don't
need to know how the audience is computed or handle delivery failures."""

import logging
from typing import Any, Callable

from repositories.tournament_invitation_repo_ddb import TournamentInvitationRepo
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from services.notification_orchestator import Notifications
from services.tournament_audience import resolve_tournament_user_emails

logger = logging.getLogger(__name__)


class TournamentMatchNotificationService:
    def __init__(
        self,
        team_repo: TournamentTeamRepo,
        invitation_repo: TournamentInvitationRepo,
        notifications: Notifications,
    ) -> None:
        self._team_repo = team_repo
        self._invitation_repo = invitation_repo
        self._notifications = notifications

    def match_started(
        self,
        *,
        tournament: dict[str, Any],
        home_team_name: str,
        away_team_name: str,
    ) -> None:
        self._broadcast(
            tournament,
            lambda emails: self._notifications.match_started(
                user_emails=emails,
                tournament_name=tournament.get("name", ""),
                home_team_name=home_team_name,
                away_team_name=away_team_name,
            ),
        )

    def match_event_created(
        self,
        *,
        tournament: dict[str, Any],
        event: dict[str, Any],
        team_name: str,
        player_name: str | None,
    ) -> None:
        self._broadcast(
            tournament,
            lambda emails: self._notifications.match_event_created(
                user_emails=emails,
                tournament_name=tournament.get("name", ""),
                event_type=event.get("type", ""),
                team_name=team_name,
                player_name=player_name,
                minute=event.get("minute", 0),
            ),
        )

    def match_finished(
        self,
        *,
        tournament: dict[str, Any],
        home_team_name: str,
        away_team_name: str,
        score_home: int,
        score_away: int,
    ) -> None:
        self._broadcast(
            tournament,
            lambda emails: self._notifications.match_finished(
                user_emails=emails,
                tournament_name=tournament.get("name", ""),
                home_team_name=home_team_name,
                away_team_name=away_team_name,
                score_home=score_home,
                score_away=score_away,
            ),
        )

    def _broadcast(
        self, tournament: dict[str, Any], send: Callable[[list[str]], Any]
    ) -> None:
        tournament_id = tournament.get("id")
        if not tournament_id:
            return
        try:
            emails = resolve_tournament_user_emails(
                tournament_id, self._team_repo, self._invitation_repo
            )
            if emails:
                send(emails)
        except Exception:
            logger.exception(
                "Failed to send match notification for tournament %s", tournament_id
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.services.test_tournament_match_notification_service -v`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add services/tournament_match_notification_service.py tests/services/test_tournament_match_notification_service.py
git commit -m "feat: add TournamentMatchNotificationService"
```

---

### Task 4: Wire match-started/match-finished notifications into `TournamentMatchService`

**Files:**
- Modify: `services/tournament_match_service.py:31-149`
- Test: `tests/services/test_tournament_match_service.py`

**Interfaces:**
- Consumes: `TournamentMatchNotificationService.match_started(*, tournament, home_team_name, away_team_name) -> None` and `.match_finished(*, tournament, home_team_name, away_team_name, score_home, score_away) -> None` (Task 3).
- Produces: `TournamentMatchService(repo, event_repo, team_repo, tournament_repo, match_notifications=None)` — new optional constructor param `match_notifications`. Used by Task 6.

- [ ] **Step 1: Write the failing tests**

Create `tests/services/test_tournament_match_service.py`:

```python
import unittest
from unittest.mock import MagicMock

from api.schemas.tournaments import PatchMatch
from services.tournament_aggregator import default_team_stats, default_tournament_stats
from services.tournament_match_service import TournamentMatchService


class TestTournamentMatchServiceNotifications(unittest.TestCase):
    def setUp(self):
        self.repo = MagicMock()
        self.event_repo = MagicMock()
        self.event_repo.list_by_match.return_value = []
        self.team_repo = MagicMock()
        self.tournament_repo = MagicMock()
        self.match_notifications = MagicMock()

        self.team_repo.get.side_effect = lambda team_id: {
            "tm_home": {"id": "tm_home", "name": "Halcones", "stats": default_team_stats()},
            "tm_away": {"id": "tm_away", "name": "Tigres", "stats": default_team_stats()},
        }.get(team_id)
        self.tournament_repo.get.return_value = {
            "id": "trn_1",
            "name": "Liga 2026",
            "rules": {},
            "stats": default_tournament_stats(),
        }

        self.service = TournamentMatchService(
            self.repo,
            self.event_repo,
            team_repo=self.team_repo,
            tournament_repo=self.tournament_repo,
            match_notifications=self.match_notifications,
        )

        self.base_match = {
            "id": "mtc_1",
            "tournament_id": "trn_1",
            "home_team_id": "tm_home",
            "away_team_id": "tm_away",
            "status": "scheduled",
            "score_home": 0,
            "score_away": 0,
            "matchweek": 0,
        }

    def test_scheduled_to_live_notifies_match_started(self):
        self.repo.get.return_value = self.base_match
        updated = {**self.base_match, "status": "live"}
        self.repo.update.return_value = updated

        self.service.update_match("mtc_1", PatchMatch(status="live"))

        self.match_notifications.match_started.assert_called_once_with(
            tournament=self.tournament_repo.get.return_value,
            home_team_name="Halcones",
            away_team_name="Tigres",
        )
        self.match_notifications.match_finished.assert_not_called()

    def test_live_to_finished_notifies_match_finished(self):
        live_match = {**self.base_match, "status": "live"}
        self.repo.get.return_value = live_match
        updated = {**live_match, "status": "finished", "score_home": 2, "score_away": 1}
        self.repo.update.return_value = updated

        self.service.update_match("mtc_1", PatchMatch(status="finished"))

        self.match_notifications.match_finished.assert_called_once_with(
            tournament=self.tournament_repo.get.return_value,
            home_team_name="Halcones",
            away_team_name="Tigres",
            score_home=2,
            score_away=1,
        )
        self.match_notifications.match_started.assert_not_called()

    def test_non_status_update_does_not_notify(self):
        self.repo.get.return_value = self.base_match
        self.repo.update.return_value = {**self.base_match, "venue": "New Venue"}

        self.service.update_match("mtc_1", PatchMatch(venue="New Venue"))

        self.match_notifications.match_started.assert_not_called()
        self.match_notifications.match_finished.assert_not_called()

    def test_scheduled_to_postponed_does_not_notify_started(self):
        self.repo.get.return_value = self.base_match
        self.repo.update.return_value = {**self.base_match, "status": "postponed"}

        self.service.update_match("mtc_1", PatchMatch(status="postponed"))

        self.match_notifications.match_started.assert_not_called()
        self.match_notifications.match_finished.assert_not_called()

    def test_reopen_finished_to_live_does_not_renotify_started_or_finished(self):
        finished_match = {**self.base_match, "status": "finished", "score_home": 2, "score_away": 1}
        self.repo.get.return_value = finished_match
        self.repo.update.return_value = {**finished_match, "status": "live"}

        self.service.update_match("mtc_1", PatchMatch(status="live"))

        self.match_notifications.match_started.assert_not_called()
        self.match_notifications.match_finished.assert_not_called()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.services.test_tournament_match_service -v`
Expected: FAIL — `TypeError: TournamentMatchService.__init__() got an unexpected keyword argument 'match_notifications'`

- [ ] **Step 3: Write the implementation**

In `services/tournament_match_service.py`, update the constructor (lines 32-42):

```python
class TournamentMatchService:
    def __init__(
        self,
        repo: TournamentMatchRepo,
        event_repo: TournamentMatchEventRepo | None = None,
        team_repo: TournamentTeamRepo | None = None,
        tournament_repo: TournamentRepo | None = None,
        match_notifications: "TournamentMatchNotificationService | None" = None,
    ):
        self.repo = repo
        self.event_repo = event_repo
        self.team_repo = team_repo
        self.tournament_repo = tournament_repo
        self.match_notifications = match_notifications
```

Add the import near the top of the file (after the `tournament_aggregator` import, line 19):

```python
from services.tournament_match_notification_service import TournamentMatchNotificationService
```

Replace the tail of `update_match` (lines 133-149) with:

```python
        result = self.repo.update(match_id, updates)

        # Propagate to materialized stats if the match crossed the finished
        # boundary. Use the freshly-persisted row so scores are current.
        new_status = (result or existing).get("status", old_status)
        was_finished = old_status == "finished"
        is_finished = new_status == "finished"
        if was_finished != is_finished:
            self._apply_match_outcome(
                existing if was_finished else (result or existing),
                sign=-1 if was_finished else +1,
            )
            if is_finished:
                self._notify_match_finished(result or existing)

        # Only a genuine kickoff (not an admin "reopen" from finished) counts
        # as the match starting.
        if old_status in ("scheduled", "postponed") and new_status == "live":
            self._notify_match_started(result or existing)

        if "status" in updates and existing.get("matchweek"):
            self._advance_current_matchweek(existing.get("tournament_id"))

        return result
```

Add two new helper methods right after `_apply_match_outcome` (after line 213, before `_advance_current_matchweek`):

```python
    def _notify_match_started(self, match: dict[str, Any]) -> None:
        if not (self.match_notifications and self.tournament_repo and self.team_repo):
            return
        tournament = self.tournament_repo.get(match.get("tournament_id")) or {}
        home = self.team_repo.get(match.get("home_team_id")) or {}
        away = self.team_repo.get(match.get("away_team_id")) or {}
        self.match_notifications.match_started(
            tournament=tournament,
            home_team_name=home.get("name", ""),
            away_team_name=away.get("name", ""),
        )

    def _notify_match_finished(self, match: dict[str, Any]) -> None:
        if not (self.match_notifications and self.tournament_repo and self.team_repo):
            return
        tournament = self.tournament_repo.get(match.get("tournament_id")) or {}
        home = self.team_repo.get(match.get("home_team_id")) or {}
        away = self.team_repo.get(match.get("away_team_id")) or {}
        self.match_notifications.match_finished(
            tournament=tournament,
            home_team_name=home.get("name", ""),
            away_team_name=away.get("name", ""),
            score_home=match.get("score_home", 0),
            score_away=match.get("score_away", 0),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.services.test_tournament_match_service -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add services/tournament_match_service.py tests/services/test_tournament_match_service.py
git commit -m "feat: notify tournament on match start and match finish"
```

---

### Task 5: Wire match-event notifications into `TournamentMatchEventService`

**Files:**
- Modify: `services/tournament_match_event_service.py:31-74`
- Test: `tests/services/test_tournament_match_event_service.py`

**Interfaces:**
- Consumes: `TournamentMatchNotificationService.match_event_created(*, tournament, event, team_name, player_name) -> None` (Task 3).
- Produces: `TournamentMatchEventService(repo, match_repo, team_repo, player_repo, tournament_repo, match_notifications=None)` — new optional constructor param. Used by Task 6.

- [ ] **Step 1: Write the failing tests**

Create `tests/services/test_tournament_match_event_service.py`:

```python
import unittest
from unittest.mock import MagicMock

from api.schemas.tournaments import CreateMatchEvent
from services.tournament_match_event_service import TournamentMatchEventService


class TestTournamentMatchEventServiceNotifications(unittest.TestCase):
    def setUp(self):
        self.repo = MagicMock()
        self.repo.list_by_match.return_value = []
        self.match_repo = MagicMock()
        self.team_repo = MagicMock()
        self.player_repo = MagicMock()
        self.tournament_repo = MagicMock()
        self.match_notifications = MagicMock()

        self.match_repo.get.return_value = {
            "id": "mtc_1",
            "tournament_id": "trn_1",
            "status": "live",
            "home_team_id": "tm_home",
            "away_team_id": "tm_away",
        }
        self.team_repo.get.return_value = {"id": "tm_home", "name": "Halcones"}
        self.player_repo.get.return_value = {"id": "ply_1", "name": "Juan Perez"}
        self.tournament_repo.get.return_value = {"id": "trn_1", "name": "Liga 2026"}

        self.service = TournamentMatchEventService(
            self.repo,
            match_repo=self.match_repo,
            team_repo=self.team_repo,
            player_repo=self.player_repo,
            tournament_repo=self.tournament_repo,
            match_notifications=self.match_notifications,
        )

    def test_create_goal_event_notifies_match_event_created(self):
        body = CreateMatchEvent(type="goal", minute=34, player_id="ply_1", team_id="tm_home")

        item = self.service.create_event("mtc_1", body)

        self.match_notifications.match_event_created.assert_called_once_with(
            tournament=self.tournament_repo.get.return_value,
            event=item,
            team_name="Halcones",
            player_name="Juan Perez",
        )

    def test_create_substitution_event_notifies_without_requiring_player_lookup_failure(self):
        body = CreateMatchEvent(
            type="substitution", minute=60, player_id="ply_1", assist_player_id="ply_2", team_id="tm_home"
        )

        self.service.create_event("mtc_1", body)

        self.match_notifications.match_event_created.assert_called_once()

    def test_no_notification_service_configured_does_not_raise(self):
        service = TournamentMatchEventService(
            self.repo,
            match_repo=self.match_repo,
            team_repo=self.team_repo,
            player_repo=self.player_repo,
            tournament_repo=self.tournament_repo,
            match_notifications=None,
        )
        body = CreateMatchEvent(type="goal", minute=10, player_id="ply_1", team_id="tm_home")

        service.create_event("mtc_1", body)  # must not raise


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.services.test_tournament_match_event_service -v`
Expected: FAIL — `TypeError: TournamentMatchEventService.__init__() got an unexpected keyword argument 'match_notifications'`

- [ ] **Step 3: Write the implementation**

In `services/tournament_match_event_service.py`, update the constructor (lines 32-44):

```python
class TournamentMatchEventService:
    def __init__(
        self,
        repo: TournamentMatchEventRepo,
        match_repo: TournamentMatchRepo | None = None,
        team_repo: TournamentTeamRepo | None = None,
        player_repo: TournamentPlayerRepo | None = None,
        tournament_repo: TournamentRepo | None = None,
        match_notifications: "TournamentMatchNotificationService | None" = None,
    ):
        self.repo = repo
        self.match_repo = match_repo
        self.team_repo = team_repo
        self.player_repo = player_repo
        self.tournament_repo = tournament_repo
        self.match_notifications = match_notifications
```

Add the import near the top of the file (after the `tournament_aggregator` import, line 26):

```python
from services.tournament_match_notification_service import TournamentMatchNotificationService
```

Update `create_event` (lines 46-74) to notify after applying the event:

```python
    def create_event(self, match_id: str, body: CreateMatchEvent) -> dict[str, Any]:
        self._require_live_match(match_id)
        self._validate_players(body.type.value, body.player_id, body.assist_player_id)

        event_index = body.event_index
        if event_index is None:
            existing = self.repo.list_by_match(match_id)
            event_index = max((e.get("event_index", 0) for e in existing), default=0) + 1

        item = {
            "id": f"mev_{uuid4().hex}",
            "match_id": match_id,
            "type": body.type.value,
            "minute": body.minute,
            "stoppage_time": body.stoppage_time,
            "player_id": body.player_id,
            "assist_player_id": body.assist_player_id,
            "team_id": body.team_id,
            "event_index": event_index,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repo.put(item)

        self._apply_event(item, sign=+1)

        if body.type.value in _GOAL_TYPES:
            self._sync_match_score(match_id)

        self._notify_match_event(item)

        return item
```

Add a new helper method after `_apply_event` (after line 172, before the `# ── Helpers ──` section):

```python
    def _notify_match_event(self, event: dict[str, Any]) -> None:
        if not (self.match_notifications and self.match_repo and self.tournament_repo and self.team_repo):
            return
        match = self.match_repo.get(event.get("match_id"))
        if not match:
            return
        tournament = self.tournament_repo.get(match.get("tournament_id")) or {}
        team = self.team_repo.get(event.get("team_id")) or {}
        player_name = None
        if self.player_repo and event.get("player_id"):
            player = self.player_repo.get(event["player_id"]) or {}
            player_name = player.get("name")
        self.match_notifications.match_event_created(
            tournament=tournament,
            event=event,
            team_name=team.get("name", ""),
            player_name=player_name,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.services.test_tournament_match_event_service -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add services/tournament_match_event_service.py tests/services/test_tournament_match_event_service.py
git commit -m "feat: notify tournament on match event creation"
```

---

### Task 6: Wire `TournamentMatchNotificationService` in `di.py`

**Files:**
- Modify: `di.py`
- Test: `tests/test_di_match_notifications.py`

**Interfaces:**
- Consumes: `TournamentMatchNotificationService(team_repo, invitation_repo, notifications)` (Task 3), `TournamentMatchService(..., match_notifications=...)` (Task 4), `TournamentMatchEventService(..., match_notifications=...)` (Task 5).
- Produces: `get_tournament_match_notification_service() -> TournamentMatchNotificationService`, and `get_match_service()`/`get_match_event_service()` now return instances with `match_notifications` populated. Nothing downstream of this task.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_di_match_notifications.py`:

```python
import os
import unittest
from unittest.mock import patch


class TestDiMatchNotificationWiring(unittest.TestCase):
    def setUp(self):
        self._env_patch = patch.dict(os.environ, {"COURIER_AUTH_TOKEN": "test-token"})
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_di_match_notifications -v`
Expected: FAIL — `AssertionError: None is not an instance of <class 'services.tournament_match_notification_service.TournamentMatchNotificationService'>`

- [ ] **Step 3: Write the implementation**

In `di.py`, add the import after the existing `TournamentMatchEventService` import (line 39):

```python
from services.tournament_match_notification_service import TournamentMatchNotificationService
```

Add a new factory function right before `get_match_service` (before line 185):

```python
def get_tournament_match_notification_service() -> TournamentMatchNotificationService:
    return TournamentMatchNotificationService(
        team_repo=TournamentTeamRepo(),
        invitation_repo=TournamentInvitationRepo(),
        notifications=get_notification_orchestator(),
    )

```

Update `get_match_service` (lines 185-195) to inject it:

```python
def get_match_service() -> TournamentMatchService:
    repo = TournamentMatchRepo()
    event_repo = TournamentMatchEventRepo()
    team_repo = TournamentTeamRepo()
    tournament_repo = TournamentRepo()
    return TournamentMatchService(
        repo,
        event_repo,
        team_repo=team_repo,
        tournament_repo=tournament_repo,
        match_notifications=get_tournament_match_notification_service(),
    )
```

Update `get_match_event_service` (lines 197-209) to inject it:

```python
def get_match_event_service() -> TournamentMatchEventService:
    repo = TournamentMatchEventRepo()
    match_repo = TournamentMatchRepo()
    team_repo = TournamentTeamRepo()
    player_repo = TournamentPlayerRepo()
    tournament_repo = TournamentRepo()
    return TournamentMatchEventService(
        repo,
        match_repo=match_repo,
        team_repo=team_repo,
        player_repo=player_repo,
        tournament_repo=tournament_repo,
        match_notifications=get_tournament_match_notification_service(),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_di_match_notifications -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Run the full new test suite together**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: PASS (all tests from Tasks 1-6, no regressions)

- [ ] **Step 6: Manual smoke check (no automated DDB in this repo)**

Run the API locally per `CLAUDE.md` (`source .venv/bin/activate && python app.py`) with `ONESIGNAL_APP_ID`/`ONESIGNAL_REST_API_KEY`/`COURIER_AUTH_TOKEN` set, then exercise via the existing `migrations/tournament_smoke_test.py` flow: create a tournament + team (with `contact_email`) + match, `PATCH` the match to `status: "live"`, create a match event, then `PATCH` the match to `status: "finished"`. Confirm three OneSignal calls fire (check server logs / OneSignal dashboard) and three rows land in the `Notification` DynamoDB table for the team's `contact_email`.

- [ ] **Step 7: Commit**

```bash
git add di.py tests/test_di_match_notifications.py
git commit -m "feat: wire TournamentMatchNotificationService into match/event service DI"
```

---

## Plan Self-Review Notes

- **Spec coverage:** Audience resolution → Task 1. `Notifications` facade additions → Task 2. `TournamentMatchNotificationService` → Task 3. Match-start/match-finish hooks → Task 4. Match-event hook → Task 5. DI wiring → Task 6. Error handling (never block the underlying write) is enforced once, inside Task 3's `_broadcast`, and relied upon (not re-implemented) by Tasks 4 and 5.
- **Design refinement vs. spec:** The spec's illustrative method signatures included an unused `match` parameter on `match_started`/`match_finished`; this plan drops it since team names/scores are already passed explicitly and nothing else from `match` is needed — avoids an unused parameter. The spec's `match_event_created` copy examples didn't show `tournament_name` in the content string; this plan appends `" — {tournament_name}"` to all three notification types for a consistent, recognizable copy convention (the spec used "e.g." — illustrative, not literal).
- **Bug caught during planning:** A naive `old_status != "live" and new_status == "live"` guard for "match started" would also fire when an admin reopens a `finished` match back to `live` (an allowed transition in `_STATUS_TRANSITIONS`). Task 4 uses `old_status in ("scheduled", "postponed") and new_status == "live"` instead, and includes a regression test (`test_reopen_finished_to_live_does_not_renotify_started_or_finished`).
