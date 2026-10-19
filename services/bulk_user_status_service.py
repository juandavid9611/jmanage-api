import time
from typing import Any, Callable

from services.user_service import UserNotFound

THROTTLE_CODES = {"TooManyRequestsException", "ThrottlingException"}


def _error_code(exc: Exception) -> str | None:
    return (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")


class BulkUserStatusService:
    """Bulk enable/disable of users within ONE account.

    Per-user logic lives in UserService.enable/disable (account-scoped: Cognito and
    the global user_status are only touched when no other account still needs the user).
    This layer adds tenancy, the self / last-admin guards, dedupe and Cognito throttling retry."""

    def __init__(self, user_svc, membership_svc, *, max_attempts: int = 4,
                 base_delay: float = 0.5, sleep: Callable[[float], None] = time.sleep):
        self.user_svc = user_svc
        self.membership_svc = membership_svc
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.sleep = sleep

    def apply(self, account_id: str, caller_id: str, user_ids: list[str], disabled: bool) -> dict[str, Any]:
        # One account-scoped query: tenancy ("has a membership row here") + admin census.
        by_user: dict[str, list[dict[str, Any]]] = {}
        for m in self.membership_svc.list_account_memberships(account_id, include_workspaceless=True):
            if m.get("user_id"):
                by_user.setdefault(m["user_id"], []).append(m)

        def is_active_admin(rows):
            return any(r.get("role") == "admin" and r.get("workspace_id")
                       and r.get("status", "active") == "active" for r in rows)

        def has_admin_role(rows):
            return any(r.get("role") == "admin" and r.get("workspace_id") for r in rows)

        active_admins = {uid for uid, rows in by_user.items() if is_active_admin(rows)}

        results = []
        for uid in dict.fromkeys(user_ids):
            if uid == caller_id:
                results.append(self._fail(uid, "self"))
                continue
            if uid not in by_user:
                results.append(self._fail(uid, "user_not_found"))
                continue
            if disabled and uid in active_admins and len(active_admins) <= 1:
                results.append(self._fail(uid, "last_admin"))
                continue
            try:
                op = self.user_svc.disable if disabled else self.user_svc.enable
                out = self._with_retry(lambda: op(uid, account_id))
            except UserNotFound:
                results.append(self._fail(uid, "user_not_found"))
                continue
            except Exception as e:  # per-user failure must not abort the batch
                print(f"Bulk user status error for {uid}: {e}")
                code = "throttled" if _error_code(e) in THROTTLE_CODES else "internal_error"
                results.append(self._fail(uid, code))
                continue
            if out["status"] != "skipped":
                if disabled:
                    active_admins.discard(uid)
                elif has_admin_role(by_user[uid]):
                    active_admins.add(uid)
            results.append({"userId": uid, "ok": True, "status": out["status"], "cognito": out["cognito"]})

        def count(status):
            return sum(1 for r in results if r["ok"] and r["status"] == status)

        return {
            "results": results,
            "disabled": count("disabled"),
            "enabled": count("enabled"),
            "skipped": count("skipped"),
            "failed": sum(1 for r in results if not r["ok"]),
        }

    @staticmethod
    def _fail(uid: str, error: str) -> dict[str, Any]:
        return {"userId": uid, "ok": False, "cognito": "n/a", "error": error}

    def _with_retry(self, fn):
        """Retry on Cognito throttling with exponential backoff. The per-user operations
        are idempotent/reconciling, so re-running after a partial failure is safe."""
        for attempt in range(self.max_attempts):
            try:
                return fn()
            except Exception as e:
                if _error_code(e) not in THROTTLE_CODES or attempt == self.max_attempts - 1:
                    raise
                self.sleep(self.base_delay * (2 ** attempt))
