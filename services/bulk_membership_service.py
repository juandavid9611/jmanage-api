from typing import Any

from repositories.membership_repo_ddb import MembershipRepo


class WorkspaceNotFound(Exception):
    def __init__(self, workspace_id: str):
        super().__init__(f"Workspace {workspace_id} not found in this account")
        self.workspace_id = workspace_id


class BulkMembershipService:
    """Bulk add / move / remove of workspace (category) memberships within one account."""

    def __init__(self, repo: MembershipRepo, workspace_svc):
        self.repo = repo
        self.workspace_svc = workspace_svc

    def apply(
        self, account_id: str, user_ids: list[str], workspace_id: str, mode: str,
        role: str = "user", from_workspace_id: str | None = None,
    ) -> dict[str, Any]:
        if mode == "move" and not from_workspace_id:
            raise ValueError("from_workspace_id is required for move")
        for ws in (workspace_id, from_workspace_id if mode == "move" else None):
            if ws and not self.workspace_svc.get(ws, account_id):
                raise WorkspaceNotFound(ws)

        # One account-scoped GSI query: tenancy check + per-user membership map.
        by_user: dict[str, dict[str, dict[str, Any]]] = {}
        for m in self.repo.list_by_account(account_id, include_workspaceless=True):
            if m.get("account_id") not in (None, account_id):
                continue
            # Users whose only row is a legacy workspace-less one are "unassigned":
            # they belong to the account but to no category yet.
            by_user.setdefault(m["user_id"], {})
            if m.get("workspace_id"):
                by_user[m["user_id"]][m["workspace_id"]] = m

        results = []
        for uid in dict.fromkeys(user_ids):
            if uid not in by_user:
                results.append({"userId": uid, "ok": False, "error": "user_not_found"})
                continue
            try:
                results.append(self._apply_one(
                    account_id, uid, by_user[uid], workspace_id, mode, role, from_workspace_id))
            except Exception as e:  # per-user failure must not abort the batch
                print(f"Bulk membership error for {uid}: {e}")
                results.append({"userId": uid, "ok": False, "error": "internal_error"})

        def count(status):
            return sum(1 for r in results if r["ok"] and r["status"] == status)

        return {
            "results": results,
            "created": count("created"),
            "skipped": count("skipped"),
            "moved": count("moved"),
            "removed": count("removed"),
            "failed": sum(1 for r in results if not r["ok"]),
        }

    def _apply_one(self, account_id, uid, current, workspace_id, mode, role, from_workspace_id):
        def ok(status):
            return {"userId": uid, "ok": True, "status": status}

        if mode == "add":
            if workspace_id in current:
                return ok("skipped")
            self.repo.create(uid, account_id, workspace_id, role, "active")
            return ok("created")

        if mode == "move":
            created = False
            if workspace_id not in current:
                self.repo.create(uid, account_id, workspace_id, role, "active")  # add first
                created = True
            if from_workspace_id in current:
                self.repo.delete(uid, account_id, from_workspace_id)
                return ok("moved")
            return ok("created" if created else "skipped")

        # remove
        if workspace_id not in current:
            return ok("skipped")
        if len(current) <= 1:
            return {"userId": uid, "ok": False, "error": "last_membership"}
        self.repo.delete(uid, account_id, workspace_id)
        return ok("removed")
