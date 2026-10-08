import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from api.schemas.training_sessions import ExerciseIn, ReviewIn, SessionIn
from services.training_session_service import (
    InvalidTransition,
    TrainingSessionService,
    ensure_editable,
    ensure_sendable,
    normalize_exercises,
    resolve_review,
)


class TestStateMachine(unittest.TestCase):
    def test_only_draft_and_rejected_are_editable(self):
        ensure_editable("draft")
        ensure_editable("rejected")
        for status in ("sent", "approved"):
            with self.assertRaises(InvalidTransition):
                ensure_editable(status)

    def test_only_draft_and_rejected_can_be_sent(self):
        ensure_sendable("draft")
        ensure_sendable("rejected")
        for status in ("sent", "approved"):
            with self.assertRaises(InvalidTransition):
                ensure_sendable(status)

    def test_approve_from_sent(self):
        self.assertEqual(resolve_review("sent", True, ""), ("approved", ""))

    def test_reject_from_sent_keeps_trimmed_comment(self):
        self.assertEqual(resolve_review("sent", False, "  more intensity "), ("rejected", "more intensity"))

    def test_reject_requires_comment(self):
        with self.assertRaises(ValueError):
            resolve_review("sent", False, "   ")

    def test_review_only_from_sent(self):
        for status in ("draft", "approved", "rejected"):
            with self.assertRaises(InvalidTransition):
                resolve_review(status, True, "ok")


class TestNormalizeExercises(unittest.TestCase):
    def test_assigns_ids_and_converts_floats_to_decimal(self):
        out = normalize_exercises([{"name": "Rondo", "diagram": {"zones": [{"x": 0.5, "y": 2}]}}])
        self.assertTrue(out[0]["id"])
        self.assertEqual(out[0]["diagram"]["zones"][0]["x"], Decimal("0.5"))

    def test_keeps_existing_id(self):
        self.assertEqual(normalize_exercises([{"id": "e1", "name": "A"}])[0]["id"], "e1")

    def test_rejects_oversized_payload(self):
        with self.assertRaises(ValueError):
            normalize_exercises([{"name": "A", "instructions": "x" * 400_000}])


class TestServiceFlow(unittest.TestCase):
    def setUp(self):
        self.repo = MagicMock()
        self.svc = TrainingSessionService(self.repo)

    def _item(self, status, workspace="ws1"):
        return {"id": "s1", "account_id": "a1", "workspace_id": workspace, "status": status, "exercises": []}

    def test_session_of_another_workspace_is_not_found(self):
        self.repo.get.return_value = self._item("draft", workspace="other")
        self.assertIsNone(self.svc.get_session("s1", "ws1", "a1"))

    def test_update_of_sent_session_is_rejected(self):
        self.repo.get.return_value = self._item("sent")
        body = SessionIn(title="T", date="2026-10-07", exercises=[ExerciseIn(name="A")])
        with self.assertRaises(InvalidTransition):
            self.svc.update_session("s1", "ws1", "a1", body)
        self.repo.update.assert_not_called()

    def test_review_stores_reviewer_and_status(self):
        self.repo.get.return_value = self._item("sent")
        self.svc.review_session("s1", "ws1", "a1", ReviewIn(approved=False, comment="redo"), "admin1")
        updates = self.repo.update.call_args.args[2]
        self.assertEqual(updates["status"], "rejected")
        self.assertEqual(updates["reviewed_by"], "admin1")
        self.assertEqual(updates["review_comment"], "redo")
        self.assertEqual(self.repo.update.call_args.kwargs["expected_statuses"], ["sent"])

    def test_send_clears_previous_review(self):
        self.repo.get.return_value = self._item("rejected")
        self.svc.send_session("s1", "ws1", "a1")
        updates = self.repo.update.call_args.args[2]
        self.assertEqual(updates["status"], "sent")
        self.assertIsNone(updates["review_comment"])


if __name__ == "__main__":
    unittest.main()
