import logging
from decimal import Decimal
from time import time
from uuid import uuid4
from typing import Any
from datetime import datetime, timezone
from fastapi import HTTPException
from api.schemas.files import FileSpec
from core.account_type import is_club
from core.finance_money import MoneyError, new_id, parse_money, plain_number, valid_file_name
from repositories.bank_adjustment_repo_ddb import payment_request_adjustment_id
from repositories.ddb_transactions import TransactionConflict, transact_write
from services.finance_access import require_account_workspace_access
from repositories.s3_adapter import S3Adapter
from repositories.order_repo_ddb import OrderRepo
from services.notification_orchestator import Notifications
from repositories.payment_requests_repo_ddb import PaymentRequestsRepo
from api.schemas.payments import PaymentRequestStatus, BulkPutPaymentRequest
from services.order_rules import can_transition

logger = logging.getLogger(__name__)


def _plain_number(value):
    """Decimal -> int when integral, else float (JSON-friendly for notifications)."""
    if value is None:
        return None
    return int(value) if value == value.to_integral_value() else float(value)


MAX_FILES_PER_SUBMISSION = 10
MAX_INSTALLMENTS_LIMIT = 100
OPEN_STATUSES = ("pending", "overdue", "partial")


def _dec(value) -> Decimal:
    """Stored numbers (Decimal, or int/float in old rows and tests) -> Decimal."""
    if value is None:
        return Decimal("0")
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _status_of(item: dict[str, Any], key: str = "payment_status") -> str | None:
    value = item.get(key)
    return getattr(value, "value", value)


def _paid_of(item: dict[str, Any]) -> Decimal:
    """paid_amount of a stored row; rows that predate installments: paid => total, anything else => 0."""
    if item.get("paid_amount") is not None:
        return _dec(item["paid_amount"])
    return _dec(item.get("user_price")) if _status_of(item) == "paid" else Decimal("0")


def _remaining_of(item: dict[str, Any]) -> Decimal:
    return max(Decimal("0"), _dec(item.get("user_price")) - _paid_of(item))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http(status: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status, detail=detail)


PR_STATUS_TO_ORDER_EVENT = {
    PaymentRequestStatus.PAID.value: ("payment_paid", "Pago confirmado"),
    PaymentRequestStatus.APPROVAL_PENDING.value: ("payment_approval_pending", "Aprobando pago"),
    PaymentRequestStatus.OVERDUE.value: ("payment_overdue", "Pago vencido"),
    PaymentRequestStatus.CANCELED.value: ("payment_canceled", "Pago cancelado"),
    PaymentRequestStatus.PENDING.value: ("payment_pending", "Pago pendiente"),
}


class PaymentRequestService:
    def __init__(
        self,
        repo: PaymentRequestsRepo,
        s3: S3Adapter,
        notifier: Notifications,
        order_repo: OrderRepo,
        *,
        submissions=None,
        adjustments=None,
        bank_accounts=None,
        membership=None,
        accounts=None,
    ):
        self.submissions = submissions  # PaymentSubmissionRepo
        self.adjustments = adjustments  # BankAdjustmentRepo
        self.bank_accounts = bank_accounts  # .get(bank_account_id, account_id) -> dict | None
        self.membership = membership  # MembershipService
        self.accounts = accounts  # AccountService (account type)
        self.repo = repo
        self.notifier = notifier
        self.s3 = s3
        self.order_repo = order_repo
        #TODO Important! Fix mapping userPrice, user_price, totalAmount
        #TODO Switch to excluded and custom mapping like in TourService
        self._updateable_fields = {
            "createDate", "dueDate", "concept", "description", "category",
            "userGroup", "userPrice", "overduePrice", "status"
        }  # (allowInstallments / maxInstallments / money rules are handled explicitly in update())
        self._relevant_notification_fields = {
            "dueDate", "totalAmount", "concept", "status"
        }
        self._payments_username = "Vittoria CD Pagos"

    def get(self, payment_request_id: str, account_id: str) -> dict[str, Any] | None:
        item = self.repo.get(payment_request_id, account_id)
        if item:
            return self._map_payment_request(item)
        return None

    def _get_light(self, payment_request_id: str, account_id: str) -> dict[str, Any] | None:
        """Mapped payment request without presigning its images (notifications / order events)."""
        item = self.repo.get(payment_request_id, account_id)
        return self._map_payment_request(item, get_presigned_url=False) if item else None

    def get_detail(
        self, payment_request_id: str, account_id: str, *, viewer_id: str | None = None, viewer_is_admin: bool = False
    ) -> dict[str, Any] | None:
        """The payment request plus its `submissions` (admins: all of them; the payee: their own)."""
        raw = self.repo.get(payment_request_id, account_id)
        if not raw:
            return None
        stored = self._stored_submissions(payment_request_id, account_id)
        legacy = self._legacy_submission(raw, stored)
        all_subs = stored + ([legacy] if legacy else [])
        is_admin = viewer_is_admin or self._is_workspace_admin(viewer_id, account_id, raw.get("user_group"))
        is_payee = bool(viewer_id) and viewer_id == raw.get("user_id")
        item = self._map_payment_request(raw)
        item["submissions"] = [self._map_submission(s) for s in all_subs] if (is_admin or is_payee) else []
        return item

    def list_payment_requests(self, account_id: str, *, user_id: str | None = None, group: str | None = None) -> list[dict[str, Any]]:
        """List payment requests with optional filtering by user_id and/or group (workspace_id)"""
        if user_id or group:
            items = self.repo.list_filtered(account_id, user_id=user_id, group=group)
        else:
            items = self.repo.list_all(account_id)
        return [self._map_payment_request(i, get_presigned_url=False) for i in items]

    def bulk_create(self, bulk_item: BulkPutPaymentRequest, account_id: str) -> list[dict[str, Any]]:
        if bulk_item is None or not hasattr(bulk_item, "paymentRequestTo") or not bulk_item.paymentRequestTo:
            raise ValueError("No users provided for payment request creation.")
        allow, max_installments = self._validate_installment_settings(
            bulk_item.allowInstallments, bulk_item.maxInstallments, current_allow=False, current_max=None
        )
        price = self._money(bulk_item.userPrice, "userPrice", allow_zero=True)
        overdue_price = self._money(bulk_item.overduePrice, "overduePrice", allow_zero=True) if bulk_item.overduePrice is not None else None

        new_payment_requests = []
        created_time = int(time())
        for user in bulk_item.paymentRequestTo:
            #TODO User URL is create with GET presigned url, should be populated later with a GET user with other attributes
            new_payment_request = self._get_new_payment_request(bulk_item, user, created_time, account_id)
            new_payment_request.update({
                "user_price": price,
                "overdue_price": overdue_price,
                "allow_installments": allow,
                "max_installments": max_installments,
                "paid_amount": Decimal("0"),
                "original_amount": price,
            })
            self.repo.put(new_payment_request)
            try:
                self.notifier.payment_created(
                    email=user["email"],
                    user_name=user["name"],
                    concept=bulk_item.concept,
                    amount=_plain_number(bulk_item.userPrice),
                    due_date=bulk_item.dueDate
                )
            except Exception:
                logger.exception("payment_created notification failed for payment request %s", new_payment_request["id"])
            new_payment_requests.append(self._map_payment_request(new_payment_request, get_presigned_url=False))
        return new_payment_requests

    # ------------------------------------------------------------------ admin edit (PUT)
    def update(
        self, payment_request_id: str, account_id: str, item: BulkPutPaymentRequest, *, updated_by: str | None = None
    ) -> dict[str, Any] | None:
        raw = self.repo.get(payment_request_id, account_id)
        if not raw:
            return None
        existing = self._map_payment_request(dict(raw), get_presigned_url=False)
        cur_status = _status_of(raw)
        old_total = _dec(raw.get("user_price"))
        paid = _paid_of(raw)

        updates = self._get_needed_updates(item)
        new_status = updates.pop("payment_status", None)
        new_status = getattr(new_status, "value", new_status)
        if new_status == cur_status:
            new_status = None  # the edit form sends the current status back untouched

        if "user_price" in updates:
            updates["user_price"] = self._money(updates["user_price"], "userPrice", allow_zero=True)
        if "overdue_price" in updates:
            updates["overdue_price"] = self._money(updates["overdue_price"], "overduePrice", allow_zero=True)
        new_total = updates.get("user_price", old_total)
        if new_total < paid:
            raise _http(409, f"totalAmount cannot be lower than the amount already paid ({plain_number(paid)}).")
        if "user_price" in updates and updates["user_price"] != old_total:
            original = raw.get("original_amount")
            if original is None or _dec(original) == old_total:
                updates["original_amount"] = updates["user_price"]  # no overdue replacement happened yet

        # installment settings
        if item.allowInstallments is not None or item.maxInstallments is not None:
            allow, max_installments = self._validate_installment_settings(
                item.allowInstallments, item.maxInstallments,
                current_allow=bool(raw.get("allow_installments")), current_max=raw.get("max_installments"),
            )
            updates["allow_installments"] = allow
            updates["max_installments"] = max_installments

        remove: list[str] = []
        extra_items: list[dict[str, Any]] = []
        if new_status is not None:
            if new_status in ("partial", "approval_pending"):
                raise _http(422, f"status '{new_status}' cannot be set by hand.")
            if cur_status == "approval_pending" and self._has_pending_review(payment_request_id, account_id):
                raise _http(409, "A submission is under review: approve or reject it first.")
            if cur_status in ("paid", "partial") and new_status not in ("paid", "partial"):
                if self._has_approved_submission(payment_request_id, account_id):
                    raise _http(409, "This request has approved payments: it cannot leave the paid/partial states.")
                updates["paid_amount"] = Decimal("0")  # legacy paid row reverted as before
            if new_status == "paid" and cur_status != "paid":
                remaining = max(Decimal("0"), new_total - paid)
                updates["paid_amount"] = new_total
                if remaining > 0 and self.submissions is not None:
                    extra_items.append(self.submissions.tx_create(self._manual_submission(raw, remaining, updated_by)))
                remove.append("status_before_review")
            updates["payment_status"] = new_status
        elif cur_status in ("pending", "overdue", "partial") and paid > 0 and new_total <= paid:
            updates["payment_status"] = "paid"  # the lowered total is already covered by approved payments
            remove.append("status_before_review")

        if not updates and not remove:
            return existing

        paid_cond, paid_names, paid_values = self._paid_condition(raw)
        pr_update = self.repo.tx_update(
            payment_request_id,
            updates,
            remove=remove,
            condition=f"#c_status = :c_status AND #c_total = :c_total AND {paid_cond}",
            names={"#c_status": "payment_status", "#c_total": "user_price", **paid_names},
            values={":c_status": raw["payment_status"], ":c_total": raw["user_price"], **paid_values},
        )
        try:
            transact_write(extra_items + [pr_update])
        except TransactionConflict:
            raise _http(409, "The payment request changed meanwhile; reload it and try again.")

        new_item = self._get_light(payment_request_id, account_id)
        if not new_item:
            raise ValueError(f"Payment Request {payment_request_id} not found after update.")
        self._notify_changes(raw, existing, new_item)
        if existing.get("status") != new_item.get("status"):
            self._append_order_event_for_status(new_item, account_id)
        return new_item

    def delete(self, payment_request_id: str, account_id: str) -> None:
        raw = self.repo.get(payment_request_id, account_id)
        if raw and (self._has_approved_submission(payment_request_id, account_id) or _status_of(raw) == "partial"
                    or raw.get("paid_amount") is not None and _dec(raw["paid_amount"]) > 0):
            raise _http(409, "This payment request has approved payments (and account adjustments): it cannot be deleted.")
        self.repo.delete(payment_request_id, account_id)

    def generate_put_presigned_urls(self, payment_request_id: str, account_id: str, files: list[FileSpec]) -> dict[str, dict[str, str]]:
        presigned_urls = {}
        payment_request = self.get(payment_request_id, account_id)
        if not payment_request:
            raise ValueError(f"Payment Request {payment_request_id} not found")
        user_id = payment_request["userId"]
        locked = self._locked_keys(payment_request_id, account_id)
        for file in files:
            if not isinstance(file, FileSpec):
                raise TypeError("Each file must be an instance of FileSpec.")

            file_name = file.file_name
            file_content_type = file.content_type
            if not file_name or not file_content_type:
                raise ValueError("File 'file_name' and 'content_type' cannot be empty.")
            if locked and self.s3._kb.invoice_file(account_id, user_id, payment_request_id, file_name) in locked:
                raise _http(409, f"'{file_name}' is already a receipt of a payment in review or approved; use another file name.")

            result = self.s3.presign_invoice_put(
                account_id=account_id,
                user_id=user_id,
                payment_request_id=payment_request_id,
                filename=file_name,
                content_type=file_content_type
            )
            presigned_urls[file_name] = result["url"]
        return presigned_urls

    # ------------------------------------------------------------------ installments: player side
    def request_payment_request_approval(
        self, payment_request_id: str, account_id: str, file_names: list[str], user_id: str | None = None
    ) -> str:
        """Deprecated alias of submit_payment for the full remaining balance."""
        self.submit_payment(payment_request_id, account_id, user_id=user_id, amount=None, file_names=file_names)
        return payment_request_id

    def submit_payment(
        self,
        payment_request_id: str,
        account_id: str,
        *,
        user_id: str | None,
        amount: Decimal | None,
        file_names: list[str],
        bank_account_id: str | None = None,
    ) -> dict[str, Any]:
        raw = self.repo.get(payment_request_id, account_id)
        if not raw:
            raise _http(404, f"Payment Request {payment_request_id} not found")
        if not user_id or raw.get("user_id") != user_id:
            raise _http(403, "Only the person charged can submit a payment.")
        status = _status_of(raw)
        if status == "approval_pending":
            raise _http(409, "A submission is already under review.")
        if status not in OPEN_STATUSES:
            raise _http(409, f"Payment Request is {status}; it does not accept payments.")
        total, paid = _dec(raw.get("user_price")), _paid_of(raw)
        remaining = total - paid
        if remaining <= 0:
            raise _http(409, "Nothing left to pay.")

        subs = self._stored_submissions(payment_request_id, account_id)
        active = [s for s in subs if s.get("status") != "rejected"]
        allow, max_installments = bool(raw.get("allow_installments")), raw.get("max_installments")
        if not allow or amount is None:
            amount = remaining
        else:
            amount = self._money(amount, "amount")
            if amount > remaining:
                raise _http(422, f"amount exceeds the remaining balance ({plain_number(remaining)}).")
            if max_installments:
                n = len(active)
                if n >= int(max_installments):
                    raise _http(409, f"The maximum number of installments ({int(max_installments)}) was reached.")
                if n + 1 == int(max_installments) and amount != remaining:
                    raise _http(422, f"This is the last allowed installment: it must cover the remaining balance ({plain_number(remaining)}).")

        names = self._validate_file_names(file_names)
        locked = {key for s in active for key in (s.get("images") or [])}
        keys = [self.s3._kb.invoice_file(account_id, raw["user_id"], payment_request_id, n) for n in names]
        if any(k in locked for k in keys):
            raise _http(422, "A file name was already used by another payment of this request; rename the file.")
        self._require_uploaded(keys)
        bank = self._load_bank_account(account_id, bank_account_id) if bank_account_id else None

        submission = {
            "id": new_id("sub"),
            "account_id": account_id,
            "payment_request_id": payment_request_id,
            "user_id": raw["user_id"],
            "amount": amount,
            "file_names": names,
            "images": keys,
            "bank_account_id": bank["id"] if bank else None,
            "status": "pending_review",
            "source": "player",
            "submitted_at": _now_iso(),
        }
        existing_images = raw.get("images") or []
        new_keys = [k for k in keys if k not in existing_images]
        raw_sets, extra_names, extra_values = [], {}, {}
        if new_keys:
            raw_sets = ["#images = list_append(if_not_exists(#images, :empty), :newimgs)"]
            extra_names = {"#images": "images"}
            extra_values = {":empty": [], ":newimgs": new_keys}
        paid_cond, paid_names, paid_values = self._paid_condition(raw)
        pr_update = self.repo.tx_update(
            payment_request_id,
            {"payment_status": "approval_pending", "status_before_review": status},
            raw_sets=raw_sets,
            condition=f"#c_status = :c_status AND #c_total = :c_total AND {paid_cond}",
            names={"#c_status": "payment_status", "#c_total": "user_price", **paid_names, **extra_names},
            values={":c_status": raw["payment_status"], ":c_total": raw["user_price"], **paid_values, **extra_values},
        )
        existing = self._map_payment_request(dict(raw), get_presigned_url=False)
        try:
            transact_write([self.submissions.tx_create(submission), pr_update])
        except TransactionConflict:
            raise _http(409, "The payment request changed meanwhile (a submission may already be under review); reload it.")

        new_item = self._get_light(payment_request_id, account_id)
        self._notify_changes(raw, existing, new_item, notify_admins=True)
        self._append_order_event_for_status(new_item, account_id)
        return {
            "submissionId": submission["id"],
            "paymentRequestId": payment_request_id,
            "status": "approval_pending",
            "amount": plain_number(amount),
            "paidAmount": plain_number(paid),
            "remaining": plain_number(remaining),
        }

    # ------------------------------------------------------------------ installments: admin review
    def review_payment(
        self,
        payment_request_id: str,
        account_id: str,
        *,
        reviewer_id: str,
        submission_id: str,
        approve: bool,
        bank_account_id: str | None = None,
    ) -> dict[str, Any]:
        raw = self.repo.get(payment_request_id, account_id)
        if not raw:
            raise _http(404, f"Payment Request {payment_request_id} not found")
        sub = self._resolve_submission(raw, account_id, submission_id)
        decision = "approved" if approve else "rejected"

        if sub["status"] != "pending_review":
            if not self._same_decision(sub, decision):
                raise _http(409, f"This submission was already {sub['status']}.")
            return self._review_result(raw, account_id, sub, replayed=True)

        if _status_of(raw) != "approval_pending":
            raise _http(409, "The payment request is not under review.")
        total, paid, amount = _dec(raw.get("user_price")), _paid_of(raw), _dec(sub["amount"])
        status_before = raw.get("status_before_review")

        updates: dict[str, Any]
        bank = None
        if approve:
            new_paid = paid + amount
            if new_paid > total:
                raise _http(409, "Approving this payment would exceed the total amount; reject it instead.")
            if new_paid >= total:
                new_status = "paid"
            else:
                new_status = "overdue" if self._was_overdue(raw, status_before) else "partial"
            updates = {"payment_status": new_status, "paid_amount": new_paid}
            target = bank_account_id or sub.get("bank_account_id")
            if target:
                bank = self._load_bank_account(account_id, target)  # exists / tenant / active / club account
                require_account_workspace_access(
                    self.membership, user_id=reviewer_id, account_id=account_id,
                    request_workspace_id=raw.get("user_group"), bank_account=bank,
                )
        else:
            updates = {"payment_status": self._restore_status(raw, status_before, paid)}

        now = _now_iso()
        paid_cond, paid_names, paid_values = self._paid_condition(raw)
        items = [
            self.submissions.tx_review(
                sub["id"], status=decision, reviewed_by=reviewer_id, reviewed_at=now,
                bank_account_id=bank["id"] if bank else None,
            ),
            self.repo.tx_update(
                payment_request_id,
                updates,
                remove=["status_before_review"],
                condition=f"#c_status = :c_status AND #c_total = :c_total AND {paid_cond}",
                names={"#c_status": "payment_status", "#c_total": "user_price", **paid_names},
                values={":c_status": "approval_pending", ":c_total": raw["user_price"], **paid_values},
            ),
        ]
        if bank is not None:
            items.append(self.adjustments.tx_create(self._adjustment_item(raw, sub, bank, amount, reviewer_id, now)))

        existing = self._map_payment_request(dict(raw), get_presigned_url=False)
        try:
            transact_write(items)
        except TransactionConflict:
            fresh = self.submissions.get(sub["id"], account_id)
            if fresh and fresh["status"] != "pending_review":
                if self._same_decision(fresh, decision):
                    return self._review_result(raw, account_id, fresh, replayed=True)
                raise _http(409, f"This submission was already {fresh['status']}.")
            raise _http(409, "The payment request changed meanwhile; reload it and try again.")

        new_item = self._get_light(payment_request_id, account_id)
        self._notify_changes(raw, existing, new_item)
        if existing.get("status") != new_item.get("status"):
            self._append_order_event_for_status(new_item, account_id)
        return self._review_result(raw, account_id, self.submissions.get(sub["id"], account_id), replayed=False)

    def _review_result(self, raw: dict[str, Any], account_id: str, sub: dict[str, Any], *, replayed: bool) -> dict[str, Any]:
        detail = self.get_detail(raw["id"], account_id, viewer_is_admin=True)
        return {"submissionId": sub["id"], "decision": sub["status"], "replayed": replayed, "paymentRequest": detail}

    @staticmethod
    def _same_decision(sub: dict[str, Any], decision: str) -> bool:
        return sub["status"] == decision

    def _adjustment_item(self, raw, sub, bank, amount: Decimal, reviewer_id: str, now: str) -> dict[str, Any]:
        concept = raw.get("concept") or ""
        return {
            "id": payment_request_adjustment_id(raw["id"], sub["id"]),
            "account_id": raw["account_id"],
            "workspace_id": bank["workspace_id"],
            "bank_account_id": bank["id"],
            "amount": amount,
            "currency": bank.get("currency"),
            "date": now[:10],
            "note": f"Abono — {concept}"[:280],
            "source": "payment_request",
            "payment_request_id": raw["id"],
            "submission_id": sub["id"],
            "created_by": reviewer_id,
            "created_at": now,
        }

    def _resolve_submission(self, raw: dict[str, Any], account_id: str, submission_id: str) -> dict[str, Any]:
        pr_id = raw["id"]
        if submission_id == "legacy":
            sub_id = f"sub_legacy_{pr_id}"
            existing = self.submissions.get(sub_id, account_id)
            if existing:
                return existing
            legacy = self._legacy_submission(raw, self._stored_submissions(pr_id, account_id))
            if not legacy:
                raise _http(404, "No legacy submission to review for this payment request.")
            legacy = {**legacy, "id": sub_id}
            try:
                transact_write([self.submissions.tx_create(legacy)])
            except TransactionConflict:
                pass  # a concurrent review persisted it first: read it back
            persisted = self.submissions.get(sub_id, account_id)
            if not persisted:
                raise _http(409, "Could not persist the legacy submission; retry.")
            return persisted
        sub = self.submissions.get(submission_id, account_id)
        if not sub or sub.get("payment_request_id") != pr_id:
            raise _http(404, f"Submission {submission_id} not found")
        return sub

    def _legacy_submission(self, raw: dict[str, Any], stored: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Receipts uploaded by the old request_approval flow have no submission row: show them as one."""
        if _status_of(raw) != "approval_pending" or not raw.get("images"):
            return None
        if any(s.get("status") == "pending_review" for s in stored):
            return None
        images = list(raw["images"])
        return {
            "id": "legacy",
            "account_id": raw["account_id"],
            "payment_request_id": raw["id"],
            "user_id": raw.get("user_id"),
            "amount": _remaining_of(raw),
            "file_names": [k.rsplit("/", 1)[-1] for k in images],
            "images": images,
            "bank_account_id": None,
            "status": "pending_review",
            "source": "legacy",
            "submitted_at": None,
        }

    def _manual_submission(self, raw: dict[str, Any], amount: Decimal, updated_by: str | None) -> dict[str, Any]:
        now = _now_iso()
        return {
            "id": new_id("sub"),
            "account_id": raw["account_id"],
            "payment_request_id": raw["id"],
            "user_id": raw.get("user_id"),
            "amount": amount,
            "file_names": [],
            "images": [],
            "bank_account_id": None,
            "status": "approved",
            "source": "manual",
            "submitted_at": now,
            "reviewed_by": updated_by,
            "reviewed_at": now,
        }

    def _map_submission(self, sub: dict[str, Any]) -> dict[str, Any]:
        images = list(sub.get("images") or [])
        return {
            "id": sub["id"],
            "paymentRequestId": sub.get("payment_request_id"),
            "amount": plain_number(_dec(sub.get("amount"))),
            "accountId": sub.get("bank_account_id"),
            "fileNames": list(sub.get("file_names") or []),
            "images": [self.s3.presign_get_from_explicit_key(key=k, content_type="image/png") for k in images] if self.s3 else images,
            "status": sub.get("status"),
            "source": sub.get("source", "player"),
            "submittedAt": sub.get("submitted_at"),
            "reviewedBy": sub.get("reviewed_by"),
            "reviewedAt": sub.get("reviewed_at"),
        }

    # ------------------------------------------------------------------ small rules
    def _stored_submissions(self, payment_request_id: str, account_id: str) -> list[dict[str, Any]]:
        if self.submissions is None:
            return []
        return self.submissions.list_by_payment_request(payment_request_id, account_id)

    def _has_pending_review(self, payment_request_id: str, account_id: str) -> bool:
        return any(s.get("status") == "pending_review" for s in self._stored_submissions(payment_request_id, account_id))

    def _has_approved_submission(self, payment_request_id: str, account_id: str) -> bool:
        return any(s.get("status") == "approved" for s in self._stored_submissions(payment_request_id, account_id))

    def _locked_keys(self, payment_request_id: str, account_id: str) -> set[str]:
        """Receipts of payments in review or approved: they must never be overwritten."""
        return {
            key
            for s in self._stored_submissions(payment_request_id, account_id)
            if s.get("status") != "rejected"
            for key in (s.get("images") or [])
        }

    def _is_workspace_admin(self, user_id: str | None, account_id: str, workspace_id: str | None) -> bool:
        if not (self.membership and user_id and workspace_id):
            return False
        try:
            return self.membership.get_user_role_in_workspace(user_id, account_id, workspace_id) == "admin"
        except Exception:
            logger.exception("could not resolve the workspace role of %s", user_id)
            return False

    @staticmethod
    def _was_overdue(raw: dict[str, Any], status_before: str | None) -> bool:
        if status_before:
            return status_before == "overdue"
        return PaymentRequestService._past_due(raw)  # legacy approval_pending rows carry no status_before_review

    @staticmethod
    def _past_due(raw: dict[str, Any]) -> bool:
        try:
            return datetime.fromisoformat(raw["due_date"]).replace(tzinfo=None) < datetime.now()
        except Exception:
            return False

    def _restore_status(self, raw: dict[str, Any], status_before: str | None, paid: Decimal) -> str:
        if status_before in OPEN_STATUSES:
            return status_before
        if paid > 0:
            return "overdue" if self._past_due(raw) else "partial"
        return "overdue" if self._past_due(raw) else "pending"

    @staticmethod
    def _paid_condition(raw: dict[str, Any]) -> tuple[str, dict[str, str], dict[str, Any]]:
        """Optimistic guard on paid_amount; rows that predate installments have no attribute yet."""
        if raw.get("paid_amount") is not None:
            return "#c_paid = :c_paid", {"#c_paid": "paid_amount"}, {":c_paid": raw["paid_amount"]}
        return "attribute_not_exists(#c_paid)", {"#c_paid": "paid_amount"}, {}

    @staticmethod
    def _money(value: Any, field: str, *, allow_zero: bool = False) -> Decimal:
        try:
            return parse_money(value, allow_zero=allow_zero)
        except MoneyError as exc:
            raise _http(422, f"{field}: {exc}")

    @staticmethod
    def _validate_installment_settings(
        allow: bool | None, max_installments: int | None, *, current_allow: bool, current_max: Any
    ) -> tuple[bool, int | None]:
        allow = current_allow if allow is None else allow
        max_installments = (int(current_max) if current_max is not None else None) if max_installments is None else max_installments
        if not allow:
            return False, None
        if max_installments is None or max_installments < 2:
            raise _http(422, "allowInstallments requires maxInstallments >= 2.")
        if max_installments > MAX_INSTALLMENTS_LIMIT:
            raise _http(422, f"maxInstallments cannot exceed {MAX_INSTALLMENTS_LIMIT}.")
        return True, max_installments

    @staticmethod
    def _validate_file_names(file_names: list[str] | None) -> list[str]:
        if not file_names:
            raise _http(422, "fileNames must contain at least one file.")
        if len(file_names) > MAX_FILES_PER_SUBMISSION:
            raise _http(422, f"At most {MAX_FILES_PER_SUBMISSION} files per payment.")
        if any(not valid_file_name(n) for n in file_names):
            raise _http(422, "Invalid file name (no slashes or '..', at most 120 characters).")
        if len(set(file_names)) != len(file_names):
            raise _http(422, "Duplicated file names.")
        return list(file_names)

    def _require_uploaded(self, keys: list[str]) -> None:
        exists = getattr(self.s3, "object_exists", None)
        if exists is None:
            return
        missing = [k.rsplit("/", 1)[-1] for k in keys if exists(k) is False]
        if missing:
            raise _http(422, f"Files were not uploaded: {', '.join(missing)}.")

    def _load_bank_account(self, account_id: str, bank_account_id: str) -> dict[str, Any]:
        """The bank account must exist in this tenant, be active and belong to a club account."""
        if self.accounts is not None and not is_club(self.accounts.get(account_id)):
            raise _http(422, "Tournament accounts do not use bank accounts.")
        bank = self.bank_accounts.get(bank_account_id, account_id) if self.bank_accounts else None
        if not bank:
            raise _http(422, f"Bank account {bank_account_id} not found.")
        if not bank.get("is_active", False):
            raise _http(422, f"Bank account {bank_account_id} is not active.")
        return bank

    def _notify_changes(self, raw: dict[str, Any], old_mapped: dict[str, Any], new_mapped: dict[str, Any] | None,
                        *, notify_admins: bool = False) -> None:
        """Tell the payee what changed. Never blocks the money path."""
        if not new_mapped:
            return
        try:
            changes = self._get_notification_fields_changed(old_mapped, new_mapped)
            user = raw.get("payment_request_to") or {}
            if changes and user.get("email"):
                self.notifier.payment_updated(
                    email=user["email"],
                    user_name=user.get("name"),
                    concept=new_mapped["concept"],
                    changes=changes,
                    notify_admins=notify_admins,
                )
        except Exception:
            logger.exception("payment_updated notification failed for payment request %s", raw.get("id"))

    def cancel_for_order(self, payment_request_id: str, account_id: str) -> bool:
        """Cancel the payment request linked to a cancelled order (only if still open)."""
        item = self.repo.get(payment_request_id, account_id)
        if not item:
            return False
        open_states = {
            PaymentRequestStatus.PENDING.value,
            PaymentRequestStatus.OVERDUE.value,
            PaymentRequestStatus.APPROVAL_PENDING.value,
        }
        current = item.get("payment_status")
        current = current.value if hasattr(current, "value") else current
        if current not in open_states:
            return False
        if _paid_of(item) > 0:
            return False  # approved payments are tracked money (adjustments): never cancel it automatically
        self.repo.update(payment_request_id, account_id, {"payment_status": PaymentRequestStatus.CANCELED})
        return True

    def process_overdue_payments(self) -> list[dict[str, Any]]:
        """Move every past-due `pending` or `partial` request (of every account) to overdue.

        The overdue price replaces the price (keeping `original_amount`), paid_amount is kept, and a
        request already covered by what was paid becomes `paid`. Idempotent: a second run finds nothing.
        """
        open_by_account: dict[str, list[dict[str, Any]]] = {}
        for status in (PaymentRequestStatus.PENDING.value, PaymentRequestStatus.PARTIAL.value):
            for item in self.repo.list_by_status_all_accounts(status):
                if item.get("account_id"):
                    open_by_account.setdefault(item["account_id"], []).append(item)

        overdue_payments = []
        for account_id, open_items in open_by_account.items():
            datetime_now = datetime.now()
            account_overdue_payments = []
            for item in open_items:
                try:
                    due_datetime = datetime.fromisoformat(item["due_date"])
                    due_datetime = due_datetime.replace(tzinfo=None)
                    if due_datetime >= datetime_now:
                        continue
                    entry = self._mark_overdue(item, account_id)
                except Exception:
                    logger.exception("could not process overdue payment request %s", item.get("id"))
                    continue
                if entry:
                    account_overdue_payments.append(entry)
            overdue_payments.extend(account_overdue_payments)
            self.notifier.overdue_payments_processed(
                account_id=account_id,
                user_name=self._payments_username,
                pending_count=len(open_items),
                overdue_payments=account_overdue_payments,
            )
        return overdue_payments

    def _mark_overdue(self, item: dict[str, Any], account_id: str) -> dict[str, Any] | None:
        current_status = _status_of(item)
        old_total = _dec(item.get("user_price"))
        overdue_price = item.get("overdue_price")
        new_total = _dec(overdue_price) if overdue_price else old_total
        paid = _paid_of(item)
        updates: dict[str, Any] = {}
        if item.get("original_amount") is None:
            updates["original_amount"] = old_total  # remember what was charged before the overdue price
        if new_total != old_total:
            updates["user_price"] = new_total
        fully_paid = paid > 0 and paid >= new_total
        new_status = PaymentRequestStatus.PAID if fully_paid else PaymentRequestStatus.OVERDUE
        updates["payment_status"] = new_status
        if not self.repo.update(item["id"], account_id, updates, expected_status=current_status):
            return None  # it changed meanwhile (e.g. a payment went under review): leave it alone
        order_id = item.get("order_id")
        if order_id:
            self._append_order_event(order_id, account_id, new_status.value)
        if fully_paid:
            return None
        return {
            "id": item["id"],
            "concept": item["concept"],
            "user_price": plain_number(new_total),
            "to_name": item["payment_request_to"]["name"],
            "to_email": item["payment_request_to"]["email"],
        }

    def _append_order_event_for_status(self, pr_item: dict[str, Any], account_id: str) -> None:
        order_id = pr_item.get("orderId")
        if not order_id:
            return
        self._append_order_event(order_id, account_id, pr_item.get("status"))

    def _append_order_event(self, order_id: str, account_id: str, pr_status: str) -> None:
        status = pr_status.value if hasattr(pr_status, "value") else pr_status
        mapping = PR_STATUS_TO_ORDER_EVENT.get(status)
        if not mapping:
            return
        event_type, title = mapping
        event = {
            "type": event_type,
            "title": title,
            "time": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.order_repo.append_event(order_id, account_id, event)
        except Exception:
            logger.exception("failed to append order event %s to order %s", event_type, order_id)
        if status == PaymentRequestStatus.PAID.value:
            self._mark_order_paid(order_id, account_id)

    def _mark_order_paid(self, order_id: str, account_id: str) -> None:
        """A paid payment request moves its order to `paid` (when the transition is allowed)."""
        try:
            order = self.order_repo.get_by_id(order_id, account_id)
            if not order or order.get("status") == "paid":
                return
            if not can_transition(order.get("status"), "paid"):
                logger.warning("Payment paid but order %s is %s; not moved to paid", order_id, order.get("status"))
                return
            event = {
                "type": "order_status_changed",
                "title": "Orden: paid",
                "time": datetime.now(timezone.utc).isoformat(),
                "meta": {"from": order.get("status"), "to": "paid", "reason": "payment_paid"},
            }
            self.order_repo.transition_status(order_id, account_id, order["status"], "paid", event)
        except Exception:
            logger.exception("failed to mark order %s as paid", order_id)

    def _map_payment_request(self, item: dict[str, Any], get_presigned_url=True) -> dict[str, Any]:
        item["createDate"] = item.pop("create_date", None)
        item["dueDate"] = item.pop("due_date", None)
        item["status"] = item.pop("payment_status", None)
        item["paymentRequestTo"] = item.pop("payment_request_to", None)
        item["totalAmount"] = item.pop("user_price", None)
        item["overduePrice"] = item.pop("overdue_price", None)
        item["group"] = item.pop("user_group", None)
        item["userId"] = item.pop("user_id", None)
        item["createdTime"] = item.pop("created_time", None)
        item["orderId"] = item.pop("order_id", None)
        # Installment fields: rows created before installments existed carry none of them.
        total = item["totalAmount"]
        item["allowInstallments"] = bool(item.pop("allow_installments", False))
        max_installments = item.pop("max_installments", None)
        item["maxInstallments"] = int(max_installments) if max_installments is not None else None
        paid = item.pop("paid_amount", None)
        if paid is None:
            paid = total if _status_of(item, "status") == "paid" else 0
        item["paidAmount"] = plain_number(paid)
        original = item.pop("original_amount", None)
        item["originalAmount"] = plain_number(total if original is None else original)
        item.pop("status_before_review", None)
        if get_presigned_url:
            item["images"] = [self.s3.presign_get_from_explicit_key(key=image, content_type='image/png') for image in item.get("images", [])]
        return item

    def _get_needed_updates(self, item: BulkPutPaymentRequest) -> dict[str, Any]:
        updates = {}
        for field in self._updateable_fields:
            value = getattr(item, field, None)
            if value is not None:
                updates[self._mapped_field_name(field)] = value
        return updates

    def _mapped_field_name(self, field: str) -> str:
        mapping = {
            "createDate": "create_date",
            "dueDate": "due_date",
            "concept": "concept",
            "description": "description",
            "category": "category",
            "userGroup": "user_group",
            "userPrice": "user_price",
            "overduePrice": "overdue_price",
            "status": "payment_status"
        }
        return mapping.get(field, field)

    def _get_notification_fields_changed(self, old_payment_request, new_payment_request):
        fields_changed = []
        for field in self._relevant_notification_fields:
            if old_payment_request[field] != new_payment_request[field]:
                fields_changed.append({"name": field, "old_value": old_payment_request[field], "new_value": new_payment_request[field]})
        return fields_changed

    def _get_new_payment_request(self, bulk_item: BulkPutPaymentRequest, user: dict[str, Any], created_time: int, account_id: str) -> dict[str, Any]:
        # TODO Clean user dict from unneeded attributes
        user.pop("user_metrics", None)
        new_item = {
            "id": f"{uuid4().hex}",
            "account_id": account_id,
            "create_date": bulk_item.createDate,
            "due_date": bulk_item.dueDate,
            "concept": bulk_item.concept,
            "description": bulk_item.description,
            "category": bulk_item.category,
            "payment_request_to": user,
            "user_id": user["id"],
            "user_group": bulk_item.group,
            "user_price": bulk_item.userPrice,
            "overdue_price": bulk_item.overduePrice,
            "payment_status": PaymentRequestStatus.PENDING.value,
            "created_time": created_time,
        }
        if bulk_item.orderId:
            new_item["order_id"] = bulk_item.orderId
        if bulk_item.reference:
            new_item["reference"] = bulk_item.reference
        return new_item
