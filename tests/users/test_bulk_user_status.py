import os
import unittest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from pydantic import ValidationError

from api.schemas.users import BulkUserStatusRequest
from services.bulk_user_status_service import BulkUserStatusService
from services.user_service import UserService, UserNotFound


class FakeMembershipSvc:
    """In-memory membership rows: dicts with user_id, account_id, workspace_id, role, status."""
    def __init__(self, rows):
        self.rows = rows

    def list_account_memberships(self, account_id, include_workspaceless=False):
        return [dict(r) for r in self.rows if r["account_id"] == account_id
                and (include_workspaceless or r["workspace_id"])]

    def get_user_account_memberships(self, user_id, account_id, include_workspaceless=False):
        return [dict(r) for r in self.rows if r["user_id"] == user_id and r["account_id"] == account_id
                and (include_workspaceless or r["workspace_id"])]

    def get_user_memberships(self, user_id):
        return [dict(r) for r in self.rows if r["user_id"] == user_id
                and r["workspace_id"] and r["status"] == "active"]

    def _set(self, uid, acc, ws, status):
        for r in self.rows:
            if (r["user_id"], r["account_id"], r["workspace_id"]) == (uid, acc, ws):
                r["status"] = status

    def enable_membership(self, uid, acc, ws):
        self._set(uid, acc, ws, "active")

    def disable_membership(self, uid, acc, ws):
        self._set(uid, acc, ws, "disabled")


class FakeUserRepo:
    def __init__(self, users):
        self.users = users
        self.updates = []

    def get(self, uid, account_id):
        u = self.users.get(uid)
        return dict(u) if u else None

    def update(self, uid, account_id, updates):
        self.updates.append((uid, updates))
        self.users[uid].update(updates)

    def batch_get(self, ids):
        return [dict(self.users[i]) for i in ids if i in self.users]


class ThrottleError(Exception):
    def __init__(self, code="TooManyRequestsException"):
        self.response = {"Error": {"Code": code}}


class FakeCog:
    def __init__(self):
        self.log = []
        self.fail = {}  # email -> list of exceptions to raise (popped per call)

    def _maybe_fail(self, email):
        q = self.fail.get(email)
        if q:
            raise q.pop(0)

    def disable_user(self, email):
        self._maybe_fail(email)
        self.log.append(("disable", email))

    def enable_user(self, email):
        self._maybe_fail(email)
        self.log.append(("enable", email))

    def list_users(self):
        return []


def row(uid, acc, ws="w1", role="user", status="active"):
    return {"user_id": uid, "account_id": acc, "workspace_id": ws, "role": role, "status": status}


def user(uid, status="active"):
    return {"id": uid, "user_name": uid.upper(), "email": f"{uid}@x", "user_status": status}


def build(rows, users, **kw):
    ms, repo, cog = FakeMembershipSvc(rows), FakeUserRepo(users), FakeCog()
    usvc = UserService(repo, None, None, cog, None, ms, None)
    sleeps = []
    bulk = BulkUserStatusService(usvc, ms, sleep=sleeps.append, **kw)
    return usvc, bulk, ms, repo, cog, sleeps


class TestSingleUserRule(unittest.TestCase):
    def test_multi_account_user_keeps_cognito(self):
        rows = [row("u1", "A", "w1"), row("u1", "A", "w2"), row("u1", "B", "w9")]
        usvc, _, ms, repo, cog, _ = build(rows, {"u1": user("u1")})
        out = usvc.disable("u1", "A")
        self.assertEqual(out, {"status": "disabled", "cognito": "kept"})
        self.assertEqual(cog.log, [])
        self.assertEqual(repo.users["u1"]["user_status"], "active")
        self.assertEqual({(r["account_id"], r["status"]) for r in ms.rows},
                         {("A", "disabled"), ("B", "active")})

    def test_other_account_disabled_does_not_count(self):
        rows = [row("u1", "A"), row("u1", "B", status="disabled")]
        usvc, _, _, repo, cog, _ = build(rows, {"u1": user("u1")})
        self.assertEqual(usvc.disable("u1", "A")["cognito"], "changed")
        self.assertEqual(cog.log, [("disable", "u1@x")])
        self.assertEqual(repo.users["u1"]["user_status"], "disabled")

    def test_single_account_user_cognito_disabled(self):
        usvc, _, ms, repo, cog, _ = build([row("u1", "A")], {"u1": user("u1")})
        self.assertEqual(usvc.disable("u1", "A"), {"status": "disabled", "cognito": "changed"})
        self.assertEqual(cog.log, [("disable", "u1@x")])
        self.assertEqual(repo.users["u1"]["user_status"], "disabled")
        self.assertEqual(ms.rows[0]["status"], "disabled")

    def test_enable_restores_everything(self):
        rows = [row("u1", "A", "w1", status="disabled"), row("u1", "A", "w2", status="disabled")]
        usvc, _, ms, repo, cog, _ = build(rows, {"u1": user("u1", "disabled")})
        self.assertEqual(usvc.enable("u1", "A"), {"status": "enabled", "cognito": "changed"})
        self.assertEqual(cog.log, [("enable", "u1@x")])
        self.assertEqual(repo.users["u1"]["user_status"], "active")
        self.assertTrue(all(r["status"] == "active" for r in ms.rows))

    def test_enable_in_account_when_globally_active_does_not_touch_cognito(self):
        rows = [row("u1", "A", status="disabled"), row("u1", "B")]
        usvc, _, ms, _, cog, _ = build(rows, {"u1": user("u1")})
        self.assertEqual(usvc.enable("u1", "A"), {"status": "enabled", "cognito": "n/a"})
        self.assertEqual(cog.log, [])

    def test_non_member_cannot_be_touched(self):
        usvc, _, _, _, cog, _ = build([row("u1", "B")], {"u1": user("u1")})
        with self.assertRaises(UserNotFound):
            usvc.disable("u1", "A")
        with self.assertRaises(UserNotFound):
            usvc.enable("u1", "A")
        self.assertEqual(cog.log, [])

    def test_cognito_failure_is_reconciled_on_retry(self):
        usvc, _, ms, repo, cog, _ = build([row("u1", "A")], {"u1": user("u1")})
        cog.fail["u1@x"] = [RuntimeError("boom")]
        with self.assertRaises(RuntimeError):
            usvc.disable("u1", "A")
        self.assertEqual(ms.rows[0]["status"], "disabled")
        self.assertEqual(usvc.disable("u1", "A")["cognito"], "changed")  # not 'skipped'
        self.assertEqual(repo.users["u1"]["user_status"], "disabled")


class TestListUsersPerAccount(unittest.TestCase):
    def mk(self):
        rows = [
            row("u1", "A", "w1"), row("u1", "A", "w2", status="disabled"),   # partially active in A
            row("u2", "A", "w1", status="disabled"), row("u2", "B", "w1"),    # disabled in A, active in B
            row("u3", "A", "w1", status="disabled"),                          # single-account disabled
            row("u4", "A", "w1"),                                             # plain active
        ]
        users = {"u1": user("u1"), "u2": user("u2", "active"), "u3": user("u3", "disabled"), "u4": user("u4")}
        usvc, *_ = build(rows, users)
        return usvc

    def test_hidden_by_default(self):
        ids = sorted(u["id"] for u in self.mk().list_users("A"))
        self.assertEqual(ids, ["u1", "u4"])

    def test_include_disabled_derives_status_from_account(self):
        users = {u["id"]: u for u in self.mk().list_users("A", include_disabled=True)}
        self.assertEqual({k: v["status"] for k, v in users.items()},
                         {"u1": "active", "u2": "disabled", "u3": "disabled", "u4": "active"})
        self.assertEqual(users["u2"]["memberships"], [])

    def test_other_account_view_is_independent(self):
        rows = [row("u2", "A", status="disabled"), row("u2", "B")]
        usvc, *_ = build(rows, {"u2": user("u2")})
        self.assertEqual([u["id"] for u in usvc.list_users("B")], ["u2"])
        self.assertEqual(usvc.list_users("A"), [])

    def test_group_filter_with_disabled_membership(self):
        users = {u["id"]: u for u in self.mk().list_users("A", group="w1", include_disabled=True)}
        self.assertEqual(users["u3"]["status"], "disabled")
        self.assertEqual(users["u1"]["group"], "w1")

    def test_workspaceless_only_keeps_global_status(self):
        usvc, *_ = build([row("u9", "A", None)], {"u9": user("u9", "disabled")})
        self.assertEqual(usvc.list_users("A"), [])
        self.assertEqual(usvc.list_users("A", include_disabled=True)[0]["status"], "disabled")


class TestBulk(unittest.TestCase):
    def test_mixed_batch_and_counts(self):
        rows = [row("admin", "A", role="admin"), row("u1", "A"), row("u2", "A"), row("u2", "B"),
                row("u3", "A", status="disabled")]
        users = {k: user(k) for k in ("admin", "u1", "u2")}
        users["u3"] = user("u3", "disabled")
        _, bulk, _, repo, cog, _ = build(rows, users)
        out = bulk.apply("A", "admin", ["u1", "u2", "u3", "u1", "nobody", "admin"], True)
        by = {r["userId"]: r for r in out["results"]}
        self.assertEqual(len(out["results"]), 5)  # duplicate u1 processed once
        self.assertEqual((by["u1"]["status"], by["u1"]["cognito"]), ("disabled", "changed"))
        self.assertEqual((by["u2"]["status"], by["u2"]["cognito"]), ("disabled", "kept"))
        self.assertEqual((by["u3"]["status"], by["u3"]["cognito"]), ("skipped", "n/a"))
        self.assertEqual(by["nobody"]["error"], "user_not_found")
        self.assertEqual(by["admin"]["error"], "self")
        self.assertEqual((out["disabled"], out["enabled"], out["skipped"], out["failed"]), (2, 0, 1, 2))
        self.assertEqual(cog.log, [("disable", "u1@x")])  # u2 kept (other account), admin untouched

    def test_idempotent_second_run(self):
        rows = [row("u1", "A")]
        _, bulk, _, _, cog, _ = build(rows, {"u1": user("u1")})
        bulk.apply("A", "c", ["u1"], True)
        out = bulk.apply("A", "c", ["u1"], True)
        self.assertEqual((out["skipped"], out["disabled"]), (1, 0))
        self.assertEqual(len(cog.log), 1)
        out = bulk.apply("A", "c", ["u1"], False)
        self.assertEqual(out["enabled"], 1)
        out = bulk.apply("A", "c", ["u1"], False)
        self.assertEqual(out["skipped"], 1)

    def test_self_guard_on_enable_too(self):
        _, bulk, *_ = build([row("me", "A", status="disabled")], {"me": user("me")})
        out = bulk.apply("A", "me", ["me"], False)
        self.assertEqual(out["results"][0]["error"], "self")

    def test_last_admin_guard_cumulative(self):
        # caller is not an admin row here (e.g. platform operator), so selecting all admins
        # must be stopped before the last one.
        rows = [row("a1", "A", role="admin"), row("a2", "A", role="admin"), row("a3", "A", role="admin"),
                row("u1", "A")]
        users = {k: user(k) for k in ("a1", "a2", "a3", "u1")}
        _, bulk, ms, *_ = build(rows, users)
        out = bulk.apply("A", "caller", ["a1", "a2", "a3", "u1"], True)
        by = {r["userId"]: r for r in out["results"]}
        self.assertTrue(by["a1"]["ok"] and by["a2"]["ok"])
        self.assertEqual(by["a3"]["error"], "last_admin")
        self.assertTrue(by["u1"]["ok"])  # non-admin still processed
        active = [r for r in ms.rows if r["role"] == "admin" and r["status"] == "active"]
        self.assertEqual([r["user_id"] for r in active], ["a3"])

    def test_enabling_admin_restores_headroom(self):
        rows = [row("a1", "A", role="admin", status="disabled"), row("a2", "A", role="admin")]
        _, bulk, *_ = build(rows, {"a1": user("a1", "disabled"), "a2": user("a2")})
        out = bulk.apply("A", "caller", ["a1", "a2"], False)
        self.assertEqual(out["enabled"], 1)
        out = bulk.apply("A", "caller", ["a2", "a1"], True)  # a2 first, a1 second -> a1 is last
        by = {r["userId"]: r for r in out["results"]}
        self.assertTrue(by["a2"]["ok"])
        self.assertEqual(by["a1"]["error"], "last_admin")

    def test_admin_disabled_in_other_account_only_is_not_counted_here(self):
        rows = [row("a1", "A", role="admin"), row("a1", "B", role="admin"), row("a2", "B", role="admin")]
        _, bulk, *_ = build(rows, {"a1": user("a1"), "a2": user("a2")})
        out = bulk.apply("A", "caller", ["a1"], True)
        self.assertEqual(out["results"][0]["error"], "last_admin")

    def test_tenancy(self):
        rows = [row("u1", "A"), row("x", "B")]
        _, bulk, ms, _, cog, _ = build(rows, {"u1": user("u1"), "x": user("x")})
        out = bulk.apply("A", "c", ["x"], True)
        self.assertEqual(out["results"][0]["error"], "user_not_found")
        self.assertEqual(cog.log, [])
        self.assertEqual(ms.rows[1]["status"], "active")

    def test_workspaceless_member_is_in_account(self):
        _, bulk, _, repo, cog, _ = build([row("u9", "A", None)], {"u9": user("u9")})
        out = bulk.apply("A", "c", ["u9"], True)
        self.assertEqual(out["results"][0]["status"], "disabled")
        self.assertEqual(repo.users["u9"]["user_status"], "disabled")

    def test_throttling_retried_with_backoff(self):
        _, bulk, _, repo, cog, sleeps = build([row("u1", "A")], {"u1": user("u1")})
        cog.fail["u1@x"] = [ThrottleError(), ThrottleError("ThrottlingException")]
        out = bulk.apply("A", "c", ["u1"], True)
        self.assertEqual(out["results"][0]["status"], "disabled")
        self.assertEqual(sleeps, [0.5, 1.0])
        self.assertEqual(repo.users["u1"]["user_status"], "disabled")

    def test_throttling_exhausted_is_per_user_failure(self):
        rows = [row("u1", "A"), row("u2", "A")]
        _, bulk, _, _, cog, sleeps = build(rows, {"u1": user("u1"), "u2": user("u2")})
        cog.fail["u1@x"] = [ThrottleError() for _ in range(10)]
        out = bulk.apply("A", "c", ["u1", "u2"], True)
        by = {r["userId"]: r for r in out["results"]}
        self.assertEqual(by["u1"]["error"], "throttled")
        self.assertTrue(by["u2"]["ok"])
        self.assertEqual(len(sleeps), 3)  # 4 attempts

    def test_other_errors_not_retried_and_isolated(self):
        rows = [row("u1", "A"), row("u2", "A")]
        _, bulk, _, _, cog, sleeps = build(rows, {"u1": user("u1"), "u2": user("u2")})
        cog.fail["u1@x"] = [RuntimeError("x")]
        out = bulk.apply("A", "c", ["u1", "u2"], True)
        self.assertEqual(out["results"][0]["error"], "internal_error")
        self.assertEqual(sleeps, [])
        self.assertEqual((out["disabled"], out["failed"]), (1, 1))


class TestSchema(unittest.TestCase):
    def test_camel_case_and_limits(self):
        b = BulkUserStatusRequest.model_validate({"userIds": ["a"], "disabled": True})
        self.assertEqual((b.user_ids, b.disabled), (["a"], True))
        with self.assertRaises(ValidationError):
            BulkUserStatusRequest.model_validate({"userIds": [], "disabled": True})
        with self.assertRaises(ValidationError):
            BulkUserStatusRequest.model_validate({"userIds": ["a"] * 201, "disabled": True})
        with self.assertRaises(ValidationError):
            BulkUserStatusRequest.model_validate({"userIds": ["a"]})


if __name__ == "__main__":
    unittest.main()
