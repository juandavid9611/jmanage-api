"""Business logic for TournamentTeam management."""

import mimetypes
from uuid import uuid4
from datetime import datetime, timezone
from typing import Any

from api.schemas.tournaments import CreateTeam, PatchTeam
from repositories.s3_adapter import S3Adapter
from repositories.tournament_team_repo_ddb import TournamentTeamRepo
from repositories.tournament_repo_ddb import TournamentRepo


class TournamentTeamService:
    def __init__(
        self,
        repo: TournamentTeamRepo,
        tournament_repo: TournamentRepo | None = None,
        s3: S3Adapter | None = None,
        notifications=None,
    ):
        self.repo = repo
        self.tournament_repo = tournament_repo
        self.s3 = s3
        self.notifications = notifications

    def create_team(self, tournament_id: str, body: CreateTeam) -> dict[str, Any]:
        item = {
            "id": f"ttm_{uuid4().hex}",
            "tournament_id": tournament_id,
            "name": body.name,
            "short_name": body.short_name or "",
            "logo_url": body.logo_url or "",
            "seed": body.seed,
            "manager_name": body.manager_name or "",
            "contact_email": body.contact_email or "",
            "primary_color": body.primary_color or "",
            "rules_accepted": body.rules_accepted,
            "documents": {},
            "manager_user_ids": [],
            "created_at": datetime.utcnow().isoformat(),
        }
        # DynamoDB GSI keys cannot be empty strings — only set when present
        if body.group_id:
            item["group_id"] = body.group_id
        self.repo.put(item)
        if self.tournament_repo:
            self.tournament_repo.increment_team_count(tournament_id)
        self._notify_team_registered(item, tournament_id)
        return item

    def get_team(self, team_id: str) -> dict[str, Any] | None:
        item = self.repo.get(team_id)
        if not item:
            return None
        return self._resolve_documents(self._resolve_logo(item))

    def count_teams(self, tournament_id: str) -> int:
        return self.repo.count_by_tournament(tournament_id)

    def list_teams(self, tournament_id: str, group_id: str | None = None) -> list[dict[str, Any]]:
        if group_id:
            items = self.repo.list_by_group(group_id)
        else:
            items = self.repo.list_by_tournament(tournament_id)
        return [self._resolve_logo(item) for item in items]

    def update_team(self, team_id: str, body: PatchTeam) -> dict[str, Any] | None:
        existing = self.repo.get(team_id)
        if not existing:
            return None
        updates = body.dict(exclude_unset=True, exclude_none=True)
        if not updates:
            return self._resolve_documents(self._resolve_logo(existing))
        updated = self.repo.update(team_id, updates)
        return self._resolve_documents(self._resolve_logo(updated)) if updated else None

    def delete_team(self, team_id: str) -> bool:
        existing = self.repo.get(team_id)
        if not existing:
            return False
        self.repo.delete(team_id)
        if self.tournament_repo:
            self.tournament_repo.decrement_team_count(existing.get("tournament_id", ""))
        return True

    # ── Logo management ──────────────────────────────────────────────

    def generate_logo_upload_url(
        self, team_id: str, account_id: str, filename: str, content_type: str
    ) -> dict[str, str]:
        if not self.s3:
            raise ValueError("S3 adapter not configured")
        return self.s3.presign_team_logo_put(
            account_id=account_id,
            team_id=team_id,
            filename=filename,
            content_type=content_type,
        )

    def _resolve_logo(self, item: dict[str, Any]) -> dict[str, Any]:
        if not self.s3:
            return item
        key = item.get("logo_url", "")
        if key and not key.startswith("http"):
            try:
                item["logo_url"] = self.s3.presign_get_from_explicit_key(key=key, content_type="image/png")
            except Exception:
                pass
        return item

    # ── Document management ──────────────────────────────────────────

    def generate_document_upload_url(
        self, team_id: str, account_id: str, doc_type: str, filename: str, content_type: str
    ) -> dict[str, str]:
        if not self.s3:
            raise ValueError("S3 adapter not configured")
        return self.s3.presign_team_document_put(
            account_id=account_id,
            team_id=team_id,
            doc_type=doc_type,
            filename=filename,
            content_type=content_type,
        )

    def add_document(
        self, team_id: str, doc_type: str, name: str, key: str
    ) -> dict[str, Any] | None:
        item = self.repo.get(team_id)
        if not item:
            return None
        docs = dict(item.get("documents") or {})
        file_list = list(docs.get(doc_type, []))
        file_list.append({
            "name": name,
            "key": key,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        })
        docs[doc_type] = file_list
        updated = self.repo.update(team_id, {"documents": docs})
        return self._resolve_documents(updated) if updated else None

    def remove_document(self, team_id: str, doc_type: str, key: str) -> dict[str, Any] | None:
        item = self.repo.get(team_id)
        if not item:
            return None
        docs = dict(item.get("documents") or {})
        file_list = [f for f in docs.get(doc_type, []) if f.get("key") != key]
        docs[doc_type] = file_list
        updated = self.repo.update(team_id, {"documents": docs})
        # Delete from S3 asynchronously (best-effort)
        if self.s3:
            try:
                self.s3.delete_file(key)
            except Exception:
                pass
        return self._resolve_documents(updated) if updated else None

    # ── Helpers ──────────────────────────────────────────────────────

    def _resolve_documents(self, item: dict[str, Any]) -> dict[str, Any]:
        """Convert stored S3 keys to presigned GET URLs for all document files."""
        if not self.s3:
            return item
        docs = item.get("documents")
        if not docs:
            return item
        resolved = {}
        for doc_type, file_list in docs.items():
            resolved_files = []
            for f in (file_list or []):
                key = f.get("key", "")
                url = key
                if key and not key.startswith("http"):
                    ct = mimetypes.guess_type(f.get("name", ""))[0] or "application/octet-stream"
                    try:
                        url = self.s3.presign_get_from_explicit_key(key=key, content_type=ct)
                    except Exception:
                        url = key
                resolved_files.append({**f, "url": url})
            resolved[doc_type] = resolved_files
        item["documents"] = resolved
        return item

    def _notify_team_registered(self, team: dict[str, Any], tournament_id: str) -> None:
        if not self.notifications:
            return
        contact_email = team.get("contact_email", "")
        if not contact_email:
            return
        tournament_name = ""
        if self.tournament_repo:
            tournament = self.tournament_repo.get(tournament_id)
            tournament_name = (tournament or {}).get("name", "")
        try:
            self.notifications.team_registered(
                email=contact_email,
                club_name=team.get("name", ""),
                tournament_name=tournament_name,
            )
        except Exception:
            pass  # best-effort — don't fail team creation if notification fails

    def is_team_manager(self, team_id: str, user_id: str) -> bool:
        team = self.repo.get(team_id)
        if not team:
            return False
        return user_id in team.get("manager_user_ids", [])

    def belongs_to_tournament(self, team_id: str, tournament_id: str) -> bool:
        team = self.repo.get(team_id)
        if not team:
            return False
        return team.get("tournament_id") == tournament_id
