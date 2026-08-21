# Match Push Notifications — Design

Date: 2026-08-20
Status: Approved for implementation planning

## Summary

Send push/in-app notifications to everyone registered in a tournament at
three points in a match's lifecycle:

1. The match starts (`scheduled`/`postponed` → `live`)
2. A match event is recorded (goal, card, substitution, etc. — all
   `MatchEventType` values)
3. The match closes (→ `finished`)

This reuses the existing notification infrastructure (`Notifications`
facade → OneSignal push + DynamoDB in-app feed, addressed by user
email) rather than introducing any new delivery channel. No device
tokens, FCM, APNs, or SNS are involved — OneSignal already owns
client-side device targeting via `external_user_id` = email.

## Background / current state

- `services/notification_orchestator.py` (`Notifications`) is a
  domain-friendly facade already used by `CalendarService`,
  `TournamentTeamService`, `TournamentInvitationService`,
  `PaymentRequestService`, `OrderService`, `VotationService`, etc. It
  exposes bulk-broadcast methods like `calendar_event_created` and
  `votation_opened`, both of which call
  `_send_bulk_in_app_notification(user_emails=..., title=..., content=...,
  category=..., action_url_path=...)`. That method writes one DDB feed
  item per recipient (`NotificationRepo.put`) and calls OneSignal's
  bulk push endpoint once with all recipients as `external_user_ids`.
- The sports-tournament domain lives in `api/tournaments.py` /
  `services/tournament_*.py` / `repositories/tournament_*_repo_ddb.py`
  (distinct from the unrelated `api/tours.py` travel-booking domain).
- Match lifecycle: `MatchStatus` = `scheduled | live | finished |
  postponed` (`api/schemas/tournaments.py`). Transitions are validated
  in `TournamentMatchService._STATUS_TRANSITIONS` and applied in
  `update_match`. Crossing into `finished` triggers
  `_apply_match_outcome`, which already loads the `tournament` record
  and updates team/tournament stats.
- Match events: `MatchEventType` = `goal | own_goal | penalty_scored |
  penalty_missed | yellow_card | second_yellow | red_card |
  substitution`. Created via `TournamentMatchEventService.create_event`,
  which persists the event then calls `_apply_event` — at that point
  `match` (with `tournament_id`) is already loaded to update
  materialized stats.
- There is no "tournament followers" or "tournament users" table.
  User↔tournament association today is indirect: a team's
  `contact_email` (`TournamentTeamRepo.list_by_tournament`) and
  accepted invitations (`TournamentInvitationRepo.list_by_tournament`,
  filtered to `status == "accepted"`). Players
  (`TournamentPlayerRepo`) have no linked user account.

## Audience resolution

"All users of the tournament" = the deduplicated union of:

- `contact_email` for every team returned by
  `TournamentTeamRepo.list_by_tournament(tournament_id)` (skip
  empty/missing emails)
- `email` for every invitation returned by
  `TournamentInvitationRepo.list_by_tournament(tournament_id)` where
  `status == "accepted"`

This lives in a new, single-purpose function/service so both match
hook points share one implementation and one definition of "tournament
audience":

```
services/tournament_audience.py

def resolve_tournament_user_emails(
    tournament_id: str,
    team_repo: TournamentTeamRepo,
    invitation_repo: TournamentInvitationRepo,
) -> list[str]:
    ...
```

Pure function, no HTTP, easy to unit test in isolation (empty
tournament → `[]`; overlapping team-contact and invitation emails →
deduped; missing/blank `contact_email` skipped).

## New service: `TournamentMatchNotificationService`

`services/tournament_match_notification_service.py`. Thin
orchestrator: resolves the audience, then calls the corresponding
`Notifications` method. Never raises — every call is wrapped in
try/except and logged, matching the existing pattern in
`Notifications` methods (e.g. `payment_created` catches per-channel).

```python
class TournamentMatchNotificationService:
    def __init__(
        self,
        team_repo: TournamentTeamRepo,
        invitation_repo: TournamentInvitationRepo,
        notifications: Notifications,
    ) -> None: ...

    def match_started(self, *, tournament: dict, match: dict, home_team_name: str, away_team_name: str) -> None: ...
    def match_event_created(self, *, tournament: dict, match: dict, event: dict, team_name: str, player_name: str | None) -> None: ...
    def match_finished(self, *, tournament: dict, match: dict, home_team_name: str, away_team_name: str) -> None: ...
```

`tournament` is the raw `TournamentRepo.get(...)` dict already loaded
by the calling service (no extra fetch). Team names are resolved by
the caller (it already has team ids/records at the hook point) and
passed in, keeping this service free of a `TournamentRepo`↔name
lookup dependency beyond what's needed for the audience.

## `Notifications` facade additions

Three new methods on `services/notification_orchestator.py`, following
the existing `calendar_event_created`/`votation_opened` shape
(`_send_bulk_in_app_notification`, Spanish copy, `category` set for
client-side filtering):

- `match_started(*, user_emails, tournament_name, home_team_name, away_team_name) -> str`
  Title: `"¡Comenzó el partido!"`
  Content: `f"{home_team_name} vs {away_team_name} — {tournament_name}"`
  `category="match_started"`, `action_url_path="dashboard/tournaments"`

- `match_event_created(*, user_emails, tournament_name, event_type, team_name, player_name, minute) -> str`
  Title/content vary by `event_type` (Spanish), e.g.:
  - `goal`/`penalty_scored`: `"¡Gol!"` / `f"Gol de {player_name} ({team_name}) al minuto {minute}."`
  - `own_goal`: `"Autogol"` / `f"Autogol de {player_name} ({team_name}) al minuto {minute}."`
  - `yellow_card`/`second_yellow`: `"Tarjeta amarilla"` / `f"Amarilla para {player_name} ({team_name}) al minuto {minute}."`
  - `red_card`: `"Tarjeta roja"` / `f"Roja para {player_name} ({team_name}) al minuto {minute}."`
  - `penalty_missed`: `"Penal fallado"` / `f"{player_name} ({team_name}) falló un penal al minuto {minute}."`
  - `substitution`: `"Cambio"` / `f"Cambio en {team_name} al minuto {minute}."`
  `category="match_event"`, `action_url_path="dashboard/tournaments"`

- `match_finished(*, user_emails, tournament_name, home_team_name, away_team_name, score_home, score_away) -> str`
  Title: `"Final del partido"`
  Content: `f"{home_team_name} {score_home} - {score_away} {away_team_name} — {tournament_name}"`
  `category="match_finished"`, `action_url_path="dashboard/tournaments"`

## Hook points (no new API routes)

- **`TournamentMatchService.update_match`**: after persisting the
  status change, if `old_status != "live" and new_status == "live"`,
  call `self.match_notifications.match_started(...)`. If the update
  crosses into `finished` (existing `_apply_match_outcome` branch),
  after applying the outcome call
  `self.match_notifications.match_finished(...)`.
- **`TournamentMatchEventService.create_event`**: immediately after
  `_apply_event` succeeds, call
  `self.match_notifications.match_event_created(...)` using the
  already-loaded `match`/`tournament_id` and the new event's fields.
- Both services gain a `match_notifications:
  TournamentMatchNotificationService` constructor parameter, wired in
  `di.py` the same way `get_calendar_service()` already injects
  `Notifications` — `get_match_service()` and `get_match_event_service()`
  are updated to construct and pass a
  `get_tournament_match_notification_service()` factory.

## Error handling

Notification failures (audience resolution errors, OneSignal HTTP
errors, DDB write errors) are caught inside
`TournamentMatchNotificationService` and logged; they never propagate
into `update_match`/`create_event`. A push failure must never fail a
match update or event write.

## Out of scope

- No new device-token registration flow (OneSignal handles this
  client-side already).
- No new "tournament followers/subscribers" table — audience is
  derived from existing team/invitation data as approved.
- No per-user notification preferences/opt-out for match events
  specifically (falls back to whatever global in-app/push preferences
  already exist, if any).
- No changes to `jmanage-web` OneSignal client setup — it already
  receives pushes addressed by email.

## Testing plan

- Unit tests for `resolve_tournament_user_emails`: empty tournament,
  team-only, invitation-only, overlapping/deduped, blank
  `contact_email` skipped.
- Unit tests for `TournamentMatchNotificationService`: each method
  calls the right `Notifications` method with the right args; a
  raised exception from `Notifications` or the repos is caught and
  logged, not propagated.
- Unit tests for `TournamentMatchService.update_match`: notification
  fires exactly once on `scheduled→live` and on `→finished`, does not
  fire on `→postponed` or no-op updates; a notification failure does
  not prevent the match update from succeeding.
- Unit tests for `TournamentMatchEventService.create_event`:
  notification fires for every `MatchEventType` with correctly mapped
  copy; a notification failure does not prevent event creation from
  succeeding.
