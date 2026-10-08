import os
import unittest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from pydantic import ValidationError

from api.schemas.memberships import BulkMembershipRequest
from repositories.ddb_batch import batch_get_items
from services.bulk_membership_service import BulkMembershipService, WorkspaceNotFound
from services.user_service import UserService


class FakeBatch:
    def __init__(self, table, store, unprocessed_first=0):
        self.table, self.store, self.calls = table, store, []
        self.unprocessed_first = unprocessed_first

    def __call__(self, req):
        keys = req[self.table]["Keys"]
        self.calls.append(list(keys))
        assert len(keys) <= 100
        if self.unprocessed_first > 0:
            self.unprocessed_first -= 1
            return {"Responses": {self.table: []}, "UnprocessedKeys": {self.table: {"Keys": keys}}}
        return {"Responses": {self.table: [self.store[k["id"]] for k in keys if k["id"] in self.store]},
                "UnprocessedKeys": {}}


class TestBatchGet(unittest.TestCase):
    def test_chunks_of_100_and_dedupe(self):
        store = {f"u{i}": {"id": f"u{i}"} for i in range(250)}
        fake = FakeBatch("T", store)
        keys = [{"id": f"u{i}"} for i in range(250)] + [{"id": "u0"}]
        items = batch_get_items(fake, "T", keys, sleep=lambda s: None)
        self.assertEqual(len(items), 250)
        self.assertEqual([len(c) for c in fake.calls], [100, 100, 50])

    def test_unprocessed_retried(self):
        fake = FakeBatch("T", {"a": {"id": "a"}}, unprocessed_first=2)
        sleeps = []
        items = batch_get_items(fake, "T", [{"id": "a"}], sleep=sleeps.append)
        self.assertEqual(items, [{"id": "a"}])
        self.assertEqual(len(fake.calls), 3)
        self.assertEqual(len(sleeps), 2)

    def test_gives_up(self):
        fake = FakeBatch("T", {}, unprocessed_first=99)
        with self.assertRaises(RuntimeError):
            batch_get_items(fake, "T", [{"id": "a"}], max_retries=2, sleep=lambda s: None)

    def test_empty(self):
        fake = FakeBatch("T", {})
        self.assertEqual(batch_get_items(fake, "T", []), [])
        self.assertEqual(fake.calls, [])


class FakeMembershipRepo:
    def __init__(self, rows):
        # rows: (user, account, ws, role)
        self.rows = {(u, a, w): {"user_id": u, "account_id": a, "workspace_id": w, "role": r, "status": "active"}
                     for u, a, w, r in rows}
        self.log = []

    def list_by_account(self, account_id):
        return [dict(v) for k, v in self.rows.items() if k[1] == account_id]

    def create(self, u, a, w, role="user", status="active"):
        self.log.append(("create", u, w))
        self.rows[(u, a, w)] = {"user_id": u, "account_id": a, "workspace_id": w, "role": role, "status": status}

    def delete(self, u, a, w):
        self.log.append(("delete", u, w))
        self.rows.pop((u, a, w), None)


class FakeWorkspaceSvc:
    def __init__(self, ws):
        self.ws = ws  # {ws_id: account}

    def get(self, ws, account_id):
        return {"id": ws} if self.ws.get(ws) == account_id else None


def make(rows=None):
    repo = FakeMembershipRepo(rows if rows is not None else [
        ("u1", "A", "w1", "user"), ("u2", "A", "w1", "user"), ("u2", "A", "w2", "user"),
        ("x1", "B", "wb", "user"),
    ])
    svc = BulkMembershipService(repo, FakeWorkspaceSvc({"w1": "A", "w2": "A", "w3": "A", "wb": "B"}))
    return svc, repo


class TestBulk(unittest.TestCase):
    def test_add_creates_and_skips_idempotent(self):
        svc, repo = make()
        r = svc.apply("A", ["u1", "u2"], "w2", "add", role="coach")
        self.assertEqual((r["created"], r["skipped"], r["failed"]), (1, 1, 0))
        self.assertEqual(repo.rows[("u1", "A", "w2")]["role"], "coach")
        self.assertEqual(repo.rows[("u2", "A", "w2")]["role"], "user")  # untouched
        r2 = svc.apply("A", ["u1", "u2"], "w2", "add")
        self.assertEqual((r2["created"], r2["skipped"]), (0, 2))

    def test_move_adds_before_removing(self):
        svc, repo = make()
        r = svc.apply("A", ["u1"], "w3", "move", from_workspace_id="w1")
        self.assertEqual(r["results"][0]["status"], "moved")
        self.assertEqual(repo.log, [("create", "u1", "w3"), ("delete", "u1", "w1")])
        r2 = svc.apply("A", ["u1"], "w3", "move", from_workspace_id="w1")
        self.assertEqual(r2["results"][0]["status"], "skipped")

    def test_move_target_exists_still_removes_source(self):
        svc, repo = make()
        r = svc.apply("A", ["u2"], "w2", "move", from_workspace_id="w1")
        self.assertEqual(r["results"][0]["status"], "moved")
        self.assertNotIn(("u2", "A", "w1"), repo.rows)

    def test_move_failure_on_add_keeps_source(self):
        svc, repo = make()
        orig = repo.create
        repo.create = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        r = svc.apply("A", ["u1"], "w3", "move", from_workspace_id="w1")
        self.assertFalse(r["results"][0]["ok"])
        self.assertIn(("u1", "A", "w1"), repo.rows)
        self.assertEqual(r["failed"], 1)
        repo.create = orig

    def test_remove_and_last_membership_guard(self):
        svc, repo = make()
        r = svc.apply("A", ["u1", "u2"], "w1", "remove")
        by = {x["userId"]: x for x in r["results"]}
        self.assertEqual(by["u1"]["error"], "last_membership")
        self.assertFalse(by["u1"]["ok"])
        self.assertEqual(by["u2"]["status"], "removed")
        self.assertEqual((r["removed"], r["failed"]), (1, 1))
        self.assertIn(("u1", "A", "w1"), repo.rows)
        # idempotent
        r2 = svc.apply("A", ["u2"], "w1", "remove")
        self.assertEqual(r2["results"][0]["status"], "skipped")

    def test_tenancy(self):
        svc, repo = make()
        r = svc.apply("A", ["x1", "ghost"], "w1", "add")
        self.assertEqual([x["error"] for x in r["results"]], ["user_not_found"] * 2)
        self.assertEqual(r["failed"], 2)
        self.assertNotIn(("x1", "A", "w1"), repo.rows)
        with self.assertRaises(WorkspaceNotFound):
            svc.apply("A", ["u1"], "wb", "add")
        with self.assertRaises(WorkspaceNotFound):
            svc.apply("A", ["u1"], "w2", "move", from_workspace_id="wb")

    def test_duplicate_ids_processed_once(self):
        svc, _ = make()
        r = svc.apply("A", ["u1", "u1"], "w2", "add")
        self.assertEqual(len(r["results"]), 1)


class TestSchema(unittest.TestCase):
    def test_camel_case_and_defaults(self):
        b = BulkMembershipRequest.model_validate({"userIds": ["a"], "workspaceId": "w", "mode": "add"})
        self.assertEqual((b.user_ids, b.workspace_id, b.role), (["a"], "w", "user"))

    def test_validation(self):
        base = {"userIds": ["a"], "workspaceId": "w", "mode": "move"}
        for bad in [
            base,  # move without from
            {**base, "fromWorkspaceId": "w"},
            {**base, "mode": "add", "userIds": []},
            {**base, "mode": "add", "userIds": [str(i) for i in range(201)]},
            {**base, "mode": "add", "role": "owner"},
            {**base, "mode": "nope"},
        ]:
            with self.assertRaises(ValidationError):
                BulkMembershipRequest.model_validate(bad)


class FakeMembershipSvc:
    def __init__(self, ms):
        self.ms = ms

    def list_account_memberships(self, account_id):
        return self.ms


class FakeUserRepo:
    def __init__(self, users):
        self.users, self.get_calls, self.batch_calls = users, 0, []

    def get(self, *a):
        self.get_calls += 1

    def batch_get(self, ids):
        ids = list(ids)
        self.batch_calls.append(ids)
        return [dict(self.users[i]) for i in ids if i in self.users]


class FakeCog:
    def list_users(self):
        return [{"Username": "u1", "UserStatus": "CONFIRMED"}]


def mk_user(i, status="active"):
    return {"id": i, "user_name": i.upper(), "email": f"{i}@x", "user_status": status}


class TestListUsers(unittest.TestCase):
    def setUp(self):
        ms = [
            {"user_id": "u1", "workspace_id": "w2", "role": "coach", "status": "active"},
            {"user_id": "u1", "workspace_id": "w1", "role": "user", "status": "active"},
            {"user_id": "u2", "workspace_id": "w1", "role": "admin", "status": "active"},
            {"user_id": "u3", "workspace_id": "w1", "role": "user", "status": "disabled"},
            {"user_id": "ghost", "workspace_id": "w1", "role": "user", "status": "active"},
        ]
        self.repo = FakeUserRepo({"u1": mk_user("u1"), "u2": mk_user("u2"), "u3": mk_user("u3", "disabled")})
        self.svc = UserService(self.repo, None, None, FakeCog(), None, FakeMembershipSvc(ms), None)

    def test_dedup_without_workspace(self):
        users = self.svc.list_users("A")
        self.assertEqual(sorted(u["id"] for u in users), ["u1", "u2"])
        u1 = next(u for u in users if u["id"] == "u1")
        self.assertEqual(u1["memberships"], [{"workspace_id": "w1", "role": "user"}, {"workspace_id": "w2", "role": "coach"}])
        self.assertEqual(u1["group"], "w1")
        self.assertEqual(self.repo.get_calls, 0)
        self.assertEqual(len(self.repo.batch_calls), 1)
        self.assertEqual(u1["confirmationStatus"].value, "confirmed")

    def test_workspace_filter_keeps_all_memberships_and_semantics(self):
        users = self.svc.list_users("A", group="w2")
        self.assertEqual([u["id"] for u in users], ["u1"])
        self.assertEqual(users[0]["group"], "w2")
        self.assertEqual(users[0]["role"], "coach")
        self.assertEqual(len(users[0]["memberships"]), 2)

    def test_include_disabled(self):
        users = self.svc.list_users("A", include_disabled=True)
        u3 = next(u for u in users if u["id"] == "u3")
        self.assertEqual(u3["memberships"], [])


if __name__ == "__main__":
    unittest.main()
