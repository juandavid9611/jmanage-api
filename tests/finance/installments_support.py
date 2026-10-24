"""Helpers for the installments tests: real repos on moto + small fakes for the collaborators."""
from decimal import Decimal

import boto3

from repositories.bank_adjustment_repo_ddb import BankAdjustmentRepo
from repositories.ddb_session import payment_request_table
from repositories.payment_requests_repo_ddb import PaymentRequestsRepo
from repositories.payment_submission_repo_ddb import PaymentSubmissionRepo
from repositories.s3_adapter import S3Adapter
from services.payment_request_service import PaymentRequestService

ACC = "acc1"
WS = "ws1"
PAYEE = "user_payee"
ADMIN = "user_admin"


class FakeNotifier:
    def __init__(self):
        self.updated, self.created, self.overdue = [], [], []

    def payment_updated(self, **kw):
        self.updated.append(kw)

    def payment_created(self, **kw):
        self.created.append(kw)

    def overdue_payments_processed(self, **kw):
        self.overdue.append(kw)


class FakeOrderRepo:
    def __init__(self, status="pending"):
        self.order = {"id": "ord1", "status": status}
        self.events, self.transitions = [], []

    def append_event(self, order_id, account_id, event):
        self.events.append(event["type"])

    def get_by_id(self, order_id, account_id):
        return self.order

    def transition_status(self, order_id, account_id, old, new, event, restock=None):
        self.transitions.append((old, new))
        self.order["status"] = new


class FakeMembership:
    """roles: {(user_id, workspace_id): role}"""

    def __init__(self, roles=None):
        self.roles = roles or {}

    def get_user_role_in_workspace(self, user_id, account_id, workspace_id):
        return self.roles.get((user_id, workspace_id))


class FakeAccounts:
    def __init__(self, account_type="club"):
        self.account_type = account_type

    def get(self, account_id):
        return {"id": account_id, "settings": {"account_type": self.account_type}}


class BankLookup:
    """The BankAccountRepo.get contract, reading straight from the (moto) BankAccount table."""

    def __init__(self, table):
        self.table = table

    def get(self, bank_account_id, account_id):
        item = self.table.get_item(Key={"id": bank_account_id}).get("Item")
        return item if item and item.get("account_id") == account_id else None


def put_bank(db, bank_id="bac_1", *, workspace_id=WS, active=True, account_id=ACC):
    from repositories.ddb_session import bank_account_table

    bank_account_table().put_item(Item={
        "id": bank_id, "account_id": account_id, "workspace_id": workspace_id,
        "is_active": active, "name": "Caja", "currency": "COP",
    })


def put_pr(item_id="pr1", *, status="pending", total="100000", paid=None, allow=None, max_installments=None,
           due="2999-01-01T00:00:00", overdue_price=None, original=None, user=PAYEE, group=WS, account=ACC,
           order_id=None, images=None, status_before_review=None, created_time=1):
    """A payment request exactly as stored: fields added by installments appear only when given."""
    item = {
        "id": item_id, "account_id": account, "create_date": "2026-01-01", "due_date": due,
        "concept": "Mensualidad", "description": "d", "category": "monthly", "user_id": user,
        "user_group": group, "user_price": Decimal(total), "payment_status": status,
        "created_time": created_time,
        "payment_request_to": {"id": user, "name": "Jugador", "email": "j@example.com"},
    }
    if overdue_price is not None:
        item["overdue_price"] = Decimal(overdue_price)
    if paid is not None:
        item["paid_amount"] = Decimal(paid)
    if allow is not None:
        item["allow_installments"] = allow
    if max_installments is not None:
        item["max_installments"] = max_installments
    if original is not None:
        item["original_amount"] = Decimal(original)
    if order_id:
        item["order_id"] = order_id
    if images:
        item["images"] = images
    if status_before_review:
        item["status_before_review"] = status_before_review
    payment_request_table().put_item(Item=item)
    return item


def get_pr(item_id="pr1"):
    return payment_request_table().get_item(Key={"id": item_id})["Item"]


def upload(svc, file_name, *, pr_id="pr1", user=PAYEE, account=ACC):
    """Put the object the player would have uploaded through the presigned URL."""
    key = svc.s3._kb.invoice_file(account, user, pr_id, file_name)
    boto3.client("s3", region_name="us-west-2").put_object(Bucket="jmanage-test-bucket", Key=key, Body=b"x")
    return key


def make_service(*, accounts=None, membership=None, order_repo=None, notifier=None, banks=True) -> PaymentRequestService:
    from repositories.ddb_session import bank_account_table

    return PaymentRequestService(
        PaymentRequestsRepo(),
        S3Adapter(),
        notifier or FakeNotifier(),
        order_repo or FakeOrderRepo(),
        submissions=PaymentSubmissionRepo(),
        adjustments=BankAdjustmentRepo(),
        bank_accounts=BankLookup(bank_account_table()) if banks else None,
        membership=membership or FakeMembership({(ADMIN, WS): "admin", (PAYEE, WS): "user"}),
        accounts=accounts or FakeAccounts(),
    )


def submit(svc, *, amount=None, files=("r1.png",), pr_id="pr1", user=PAYEE, bank=None, upload_files=True):
    if upload_files:
        for f in files:
            upload(svc, f, pr_id=pr_id)
    return svc.submit_payment(pr_id, ACC, user_id=user, amount=amount, file_names=list(files), bank_account_id=bank)


def review(svc, submission_id, approve=True, *, pr_id="pr1", reviewer=ADMIN, bank=None):
    return svc.review_payment(pr_id, ACC, reviewer_id=reviewer, submission_id=submission_id, approve=approve,
                              bank_account_id=bank)
