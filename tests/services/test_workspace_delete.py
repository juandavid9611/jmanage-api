import os
import unittest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

from services.workspace_service import (
    WorkspaceDeleteBlocked,
    WorkspaceDeleteError,
    WorkspaceNotFound,
    WorkspaceService,
)

ACC, WS, ME = "acc1", "ws1", "me"


class Log(list):
    pass


class FakeRepo:
    def __init__(self, log, items=None):
        self.log = log
        self.items = items if items is not None else {WS: {"id": WS, "account_id": ACC}}
        self.fail_delete = False

    def get(self, wid, account_id):
        it = self.items.get(wid)
        return it if it and it["account_id"] == account_id else None

    def delete(self, wid, account_id):
        if wid not in self.items:
            raise ValueError("gone")
        if self.fail_delete:
            raise RuntimeError("boom")
        self.log.append("workspace")
        self.items.pop(wid)


class FakeMemberships:
    def __init__(self, log, members=(ME,)):
        self.log = log
        self.members = list(members)

    def list_workspace_memberships(self, wid):
        return [{"user_id": u, "workspace_id": wid} for u in self.members]

    def delete_membership(self, user_id, account_id, wid):
        self.log.append("membership")
        if user_id in self.members:
            self.members.remove(user_id)


class FakeAccounts:
    def __init__(self, default="other"):
        self.default = default

    def get(self, account_id):
        return {"settings": {"default_workspace": self.default}}


class FakeEvents:
    def __init__(self, items=()):
        self.items = list(items)

    def list_by_group(self, group, account_id):
        return self.items

    def list_filtered(self, account_id, group=None, tour_type=None):
        return self.items


def make(members=(ME,), default="other", events=(), tours=(), items=None):
    log = Log()
    repo = FakeRepo(log, items)
    mem = FakeMemberships(log, members)
    svc = WorkspaceService(repo, mem, account_svc=FakeAccounts(default),
                           calendar_repo=FakeEvents(events), tour_repo=FakeEvents(tours))
    return svc, repo, mem, log


class TestDeleteSafely(unittest.TestCase):
    def blocked(self, svc):
        with self.assertRaises(WorkspaceDeleteBlocked) as cm:
            svc.delete_safely(WS, ACC, ME)
        return cm.exception

    def test_default_workspace(self):
        svc, repo, mem, log = make(default=WS)
        self.assertEqual(self.blocked(svc).code, "default_workspace")
        self.assertEqual(log, [])

    def test_has_members_counts_others(self):
        svc, repo, mem, log = make(members=(ME, "a", "b"))
        e = self.blocked(svc)
        self.assertEqual(e.code, "has_members")
        self.assertIn("2", e.message)
        self.assertEqual(log, [])

    def test_has_events(self):
        svc, *_ = make(events=[{"id": "e"}])
        self.assertEqual(self.blocked(svc).code, "has_events")

    def test_has_tours(self):
        svc, *_ = make(tours=[{"id": "t"}])
        self.assertEqual(self.blocked(svc).code, "has_events")

    def test_success_only_caller_membership_order(self):
        svc, repo, mem, log = make()
        svc.delete_safely(WS, ACC, ME)
        self.assertEqual(log, ["membership", "workspace"])
        self.assertNotIn(WS, repo.items)

    def test_success_caller_without_membership(self):
        svc, repo, mem, log = make(members=())
        svc.delete_safely(WS, ACC, ME)
        self.assertNotIn(WS, repo.items)

    def test_tenancy_404(self):
        svc, repo, mem, log = make(items={WS: {"id": WS, "account_id": "other-acc"}})
        with self.assertRaises(WorkspaceNotFound):
            svc.delete_safely(WS, ACC, ME)
        self.assertEqual(log, [])
        self.assertIn(WS, repo.items)

    def test_already_gone_404(self):
        svc, *_ = make(items={})
        with self.assertRaises(WorkspaceNotFound):
            svc.delete_safely(WS, ACC, ME)

    def test_retry_after_partial_failure(self):
        svc, repo, mem, log = make()
        repo.fail_delete = True
        with self.assertRaises(WorkspaceDeleteError):
            svc.delete_safely(WS, ACC, ME)
        self.assertEqual(mem.members, [])  # membership stays removed, nothing re-created
        repo.fail_delete = False
        svc.delete_safely(WS, ACC, ME)
        self.assertNotIn(WS, repo.items)

    def test_operator_mode_counts_every_member(self):
        svc, *_ = make(members=(ME,))
        with self.assertRaises(WorkspaceDeleteBlocked) as cm:
            svc.check_deletable(WS, ACC, None)
        self.assertEqual(cm.exception.code, "has_members")


if __name__ == "__main__":
    unittest.main()


class TestOperatorScript(unittest.TestCase):
    def test_dry_run_confirm_and_refusal(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "delete_workspace_script", os.path.join(os.path.dirname(__file__), "..", "..", "migrations", "delete_workspace.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        svc, repo, mem, log = make(members=())
        self.assertEqual(mod.run(svc, ACC, WS, False), 0)
        self.assertIn(WS, repo.items)
        self.assertEqual(mod.run(svc, ACC, WS, True), 0)
        self.assertNotIn(WS, repo.items)
        self.assertEqual(mod.run(svc, ACC, WS, True), 1)  # gone
        svc, repo, *_ = make(members=(ME,))
        self.assertEqual(mod.run(svc, ACC, WS, True), 1)  # has_members, no caller exemption
        self.assertIn(WS, repo.items)
