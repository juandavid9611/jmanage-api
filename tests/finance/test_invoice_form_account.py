"""PR #177 (finance frontend) adds an account selector to the invoice edit form: when the admin marks a
request paid it sends `account_id` in the PUT and expects that account to be credited. The v2 contract moved the
account link to the review, so PUT has to honour it too, or the UI would promise a balance that never moves."""
from decimal import Decimal

import pytest
from fastapi import HTTPException

from api.schemas.payments import BulkPutPaymentRequest
from repositories.bank_adjustment_repo_ddb import BankAdjustmentRepo
from tests.finance.installments_support import (ACC, ADMIN, PAYEE, WS, FakeAccounts, FakeMembership, get_pr,
                                                 make_service, put_bank, put_pr, submit)
from tests.finance.test_installments_api import api, as_admin  # noqa: F401  (fixture)


def body(**over):
    base = dict(createDate="x", dueDate="2999-01-01T00:00:00", concept="Mensualidad", description="d",
                category="monthly", group=WS, paymentRequestTo=[], userPrice=Decimal("100000"))
    base.update(over)
    return BulkPutPaymentRequest(**base)


def adjustments():
    return BankAdjustmentRepo().table.scan()["Items"]


def test_the_snake_case_field_of_the_invoice_form_and_the_camel_case_one_both_parse():
    assert body(account_id="bac_1").accountId == "bac_1"
    assert body(accountId="bac_2").accountId == "bac_2"
    assert body().accountId is None


def test_marking_paid_with_an_account_credits_it_once(finance_db):
    put_bank(finance_db)
    put_pr("pr1", total="100000")
    svc = make_service()
    svc.update("pr1", ACC, body(status="paid", account_id="bac_1"), updated_by=ADMIN)
    raw = get_pr("pr1")
    assert raw["payment_status"] == "paid" and raw["paid_amount"] == Decimal("100000") and raw["has_reviewed_payments"] is True
    [adj] = adjustments()
    assert adj["bank_account_id"] == "bac_1" and adj["amount"] == Decimal("100000") and adj["source"] == "payment_request"
    # the form is saved again (status paid echoed, account still selected): nothing is credited twice
    svc.update("pr1", ACC, body(status="paid", account_id="bac_1", concept="Editado"), updated_by=ADMIN)
    assert len(adjustments()) == 1
    # the detail tells the form which account to preselect
    assert svc.get_detail("pr1", ACC, viewer_id=ADMIN, viewer_is_admin=True)["accountId"] == "bac_1"
    assert svc.get_detail("pr1", ACC, viewer_id=PAYEE)["accountId"] is None  # a player does not need the ledger


def test_marking_paid_without_an_account_stays_exactly_as_before(finance_db):
    put_pr("pr1", total="100000")
    svc = make_service()
    svc.update("pr1", ACC, body(status="paid"), updated_by=ADMIN)
    assert get_pr("pr1")["payment_status"] == "paid" and adjustments() == [] and "has_reviewed_payments" not in get_pr("pr1")


def test_an_account_is_ignored_unless_the_status_becomes_paid(finance_db):
    put_bank(finance_db)
    put_pr("pr1", total="100000")
    make_service().update("pr1", ACC, body(account_id="bac_1", concept="solo texto"), updated_by=ADMIN)
    assert get_pr("pr1")["payment_status"] == "pending" and adjustments() == []


def test_a_player_request_approved_by_editing_credits_the_chosen_account(finance_db):
    put_bank(finance_db)
    put_pr("pr1", total="100000")
    svc = make_service()
    submit(svc, pr_id="pr1")  # the player asks (old alias or new endpoint)
    svc.update("pr1", ACC, body(status="paid", account_id="bac_1"), updated_by=ADMIN)
    assert get_pr("pr1")["payment_status"] == "paid"
    [adj] = adjustments()
    assert adj["bank_account_id"] == "bac_1" and adj["amount"] == Decimal("100000")


@pytest.mark.parametrize("case", ["unknown", "inactive", "other_tenant", "other_workspace_not_admin"])
def test_a_bad_account_is_refused_and_nothing_is_written(finance_db, case):
    if case == "inactive":
        put_bank(finance_db, "bac_x", active=False)
    elif case == "other_tenant":
        put_bank(finance_db, "bac_x", account_id="acc_other")
    elif case == "other_workspace_not_admin":
        put_bank(finance_db, "bac_x", workspace_id="ws_other")
    put_pr("pr1", total="100000")
    svc = make_service(membership=FakeMembership({(ADMIN, WS): "admin"}))  # admin here, nothing in ws_other
    with pytest.raises(HTTPException) as e:
        svc.update("pr1", ACC, body(status="paid", account_id="bac_x"), updated_by=ADMIN)
    assert e.value.status_code in (403, 422)
    assert get_pr("pr1")["payment_status"] == "pending" and adjustments() == []


def test_tournament_accounts_do_not_use_bank_accounts(finance_db):
    put_bank(finance_db)
    put_pr("pr1", total="100000")
    svc = make_service(accounts=FakeAccounts("tournament"))
    with pytest.raises(HTTPException) as e:
        svc.update("pr1", ACC, body(status="paid", account_id="bac_1"), updated_by=ADMIN)
    assert e.value.status_code == 422 and get_pr("pr1")["payment_status"] == "pending"


def test_over_http_the_form_payload_with_snake_case_account_id_works(api):  # noqa: F811
    put_bank(None)
    put_pr("pr1", total="100000")
    as_admin(api)
    payload = {"status": "paid", "createDate": "x", "dueDate": "2999-01-01T00:00:00", "concept": "Mensualidad",
               "description": "d", "category": "monthly", "group": WS, "paymentRequestTo": [], "userPrice": 100000,
               "account_id": "bac_1"}
    r = api.put("/payment_requests/pr1", params={"workspace_id": WS}, json=payload)
    assert r.status_code == 200, r.text
    assert get_pr("pr1")["payment_status"] == "paid" and len(adjustments()) == 1
