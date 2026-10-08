import os
import unittest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from api.schemas.workspaces import CreateWorkspace
from core.account_type import is_club
from pydantic import ValidationError
from services.workspace_service import (
    WorkspaceCreationError,
    WorkspaceNameConflict,
    WorkspaceService,
    name_key,
    normalize_workspace_name,
)


class FakeRepo:
    def __init__(self, items=()):
        self.items = {i["id"]: dict(i) for i in items}
        self.fail_delete = False

    def list_all(self, account_id):
        return [i for i in self.items.values() if i["account_id"] == account_id]

    def put(self, item):
        self.items[item["id"]] = dict(item)

    def delete(self, workspace_id, account_id):
        if self.fail_delete:
            raise RuntimeError("delete failed")
        self.items.pop(workspace_id)


class FakeMemberships:
    def __init__(self, fail=False):
        self.fail = fail
        self.created = []

    def create_membership(self, user_id, account_id, workspace_id, role="user", status="active"):
        if self.fail:
            raise RuntimeError("ddb down")
        self.created.append((user_id, account_id, workspace_id, role, status))


def make(items=(), fail=False):
    repo, mem = FakeRepo(items), FakeMemberships(fail)
    return WorkspaceService(repo, mem), repo, mem


class TestNames(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(normalize_workspace_name("  Master "), "Master")
        for bad in ("", "   ", None, "x" * 81):
            with self.assertRaises(ValueError):
                normalize_workspace_name(bad)
        self.assertEqual(len(normalize_workspace_name("x" * 80)), 80)

    def test_key_is_case_insensitive_trimmed(self):
        self.assertEqual(name_key("  MASTER "), name_key("master"))

    def test_schema_trims_and_validates(self):
        self.assertEqual(CreateWorkspace.model_validate({"name": "  Academy "}).name, "Academy")
        self.assertEqual(CreateWorkspace.model_validate({"name": "A", "logo": "u"}).logo, "u")
        for bad in ({"name": "  "}, {"name": "x" * 81}, {}):
            with self.assertRaises(ValidationError):
                CreateWorkspace.model_validate(bad)


class TestCreateWithAdmin(unittest.TestCase):
    def test_creates_workspace_and_admin_membership(self):
        svc, repo, mem = make()
        out = svc.create_with_admin("  Master ", "logo.png", "acc1", "u1")
        self.assertEqual(out["name"], "Master")
        self.assertEqual(out["account_id"], "acc1")
        self.assertEqual(out["logo"], "logo.png")
        self.assertEqual(out["role"], "admin")
        self.assertEqual(len(out["id"]), 32)
        self.assertIn(out["id"], repo.items)
        self.assertNotIn("role", repo.items[out["id"]])
        self.assertEqual(mem.created, [("u1", "acc1", out["id"], "admin", "active")])

    def test_duplicate_name_same_account_rejected(self):
        svc, repo, mem = make([{"id": "w1", "account_id": "acc1", "name": "Master"}])
        with self.assertRaises(WorkspaceNameConflict):
            svc.create_with_admin(" mASTER ", None, "acc1", "u1")
        self.assertEqual(len(repo.items), 1)
        self.assertEqual(mem.created, [])

    def test_same_name_other_account_allowed(self):
        svc, repo, _ = make([{"id": "w1", "account_id": "acc2", "name": "Master"}])
        svc.create_with_admin("Master", None, "acc1", "u1")
        self.assertEqual(len(repo.items), 2)

    def test_invalid_name(self):
        svc, repo, _ = make()
        with self.assertRaises(ValueError):
            svc.create_with_admin("   ", None, "acc1", "u1")
        self.assertEqual(repo.items, {})

    def test_membership_failure_rolls_back(self):
        svc, repo, _ = make(fail=True)
        with self.assertRaises(WorkspaceCreationError):
            svc.create_with_admin("Master", None, "acc1", "u1")
        self.assertEqual(repo.items, {})

    def test_rollback_failure_still_raises_creation_error(self):
        svc, repo, _ = make(fail=True)
        repo.fail_delete = True
        with self.assertRaises(WorkspaceCreationError):
            svc.create_with_admin("Master", None, "acc1", "u1")


class TestClubOnly(unittest.TestCase):
    def test_tournament_account_rejected_by_predicate(self):
        self.assertFalse(is_club({"settings": {"account_type": "tournament"}}))
        self.assertTrue(is_club({"settings": {"account_type": "club"}}))


if __name__ == "__main__":
    unittest.main()
