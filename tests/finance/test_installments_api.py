"""FastAPI TestClient smoke per endpoint (auth dependencies overridden, services on moto)."""
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.finance.installments_support import (
    ACC, ADMIN, PAYEE, WS, FakeMembership, get_pr, make_service, put_bank, put_pr, upload,
)


@pytest.fixture
def api(finance_db):
    # auth.py downloads the Cognito JWKS at import time: serve an empty key set instead.
    with mock.patch("requests.get") as fake_get:
        fake_get.return_value.json.return_value = {"keys": []}
        import auth
        import di
        from api import payments
    svc = make_service(membership=FakeMembership({(ADMIN, WS): "admin", (PAYEE, WS): "user", ("other", WS): "user"}))
    app = FastAPI()
    app.include_router(payments.router)
    state = {"user": PAYEE, "role": "user", "ws_role": "user"}
    app.dependency_overrides[auth.get_current_user] = lambda: {"sub": state["user"]}
    app.dependency_overrides[auth.get_account_id] = lambda: ACC
    app.dependency_overrides[auth.get_account_role] = lambda: state["role"]
    app.dependency_overrides[auth.get_workspace_role] = lambda: state["ws_role"]
    app.dependency_overrides[di.get_payment_request_service] = lambda: svc
    client = TestClient(app)
    client.state, client.svc = state, svc
    return client


def as_admin(client):
    client.state.update(user=ADMIN, role="admin", ws_role="admin")


def test_get_detail_and_list_use_camel_case(api):
    put_pr(total="100000", allow=True, max_installments=3)
    got = api.get("/payment_requests/pr1").json()
    assert got["allowInstallments"] is True and got["maxInstallments"] == 3
    assert got["paidAmount"] == 0 and got["originalAmount"] == 100000 and got["totalAmount"] == 100000
    assert got["submissions"] == []
    assert api.get("/payment_requests/nope").status_code == 404
    listed = api.get("/payment_requests", params={"workspace_id": WS}).json()
    assert listed[0]["paidAmount"] == 0 and "submissions" not in listed[0]


def test_submit_review_flow_over_http(api):
    put_pr(total="100000", allow=True, max_installments=3)
    put_bank(None)
    upload(api.svc, "r1.png")
    r = api.post("/payment_requests/pr1/submit_payment", params={"workspace_id": WS},
                 json={"amount": 25000.5, "fileNames": ["r1.png"], "accountId": "bac_1"})
    assert r.status_code == 200, r.text
    sub_id = r.json()["submissionId"]
    assert r.json()["amount"] == 25000.5 and r.json()["status"] == "approval_pending"

    detail = api.get("/payment_requests/pr1").json()
    assert detail["status"] == "approval_pending"
    assert detail["submissions"][0]["accountId"] == "bac_1" and detail["submissions"][0]["fileNames"] == ["r1.png"]

    # the payee cannot review
    api.state["ws_role"] = "user"
    denied = api.post("/payment_requests/pr1/review_payment", params={"workspace_id": WS},
                      json={"submissionId": sub_id, "approve": True})
    assert denied.status_code == 403

    as_admin(api)
    ok = api.post("/payment_requests/pr1/review_payment", params={"workspace_id": WS},
                  json={"submissionId": sub_id, "approve": True})
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert body["decision"] == "approved" and body["replayed"] is False
    assert body["paymentRequest"]["status"] == "partial" and body["paymentRequest"]["paidAmount"] == 25000.5
    assert api.post("/payment_requests/pr1/review_payment", params={"workspace_id": WS},
                    json={"submissionId": sub_id, "approve": True}).json()["replayed"] is True
    assert api.post("/payment_requests/pr1/review_payment", params={"workspace_id": WS},
                    json={"submissionId": sub_id, "approve": False}).status_code == 409


def test_submit_errors_map_to_http_codes(api):
    put_pr(total="100000", allow=True, max_installments=3)
    body = {"amount": 10, "fileNames": ["r1.png"]}
    api.state["user"] = "other"
    assert api.post("/payment_requests/pr1/submit_payment", params={"workspace_id": WS}, json=body).status_code == 403
    api.state["user"] = PAYEE
    assert api.post("/payment_requests/pr1/submit_payment", params={"workspace_id": "ws_x"}, json=body).status_code == 404
    assert api.post("/payment_requests/pr1/submit_payment", params={"workspace_id": WS}, json={"fileNames": []}).status_code == 422
    assert api.post("/payment_requests/pr1/submit_payment", params={"workspace_id": WS}, json={"amount": 1}).status_code == 422
    assert api.post("/payment_requests/pr1/submit_payment", params={"workspace_id": WS}, json=body).status_code == 422  # not uploaded
    assert api.post("/payment_requests/pr1/submit_payment").status_code == 422  # workspace_id is required


def test_request_approval_alias_still_works_with_a_bare_list_body(api):
    put_pr(total="100000")
    upload(api.svc, "old.png")
    r = api.post("/payment_requests/pr1/request_approval", params={"workspace_id": WS}, json=["old.png"])
    assert r.status_code == 200 and r.json() == {"requested_payment_request_approval_id": "pr1"}
    assert get_pr()["payment_status"] == "approval_pending"
    api.state["user"] = "other"
    put_pr("pr2", created_time=2)
    assert api.post("/payment_requests/pr2/request_approval", params={"workspace_id": WS}, json=["old.png"]).status_code == 403
    assert api.post("/payment_requests/pr1/request_approval", params={"workspace_id": "ws_x"}, json=["old.png"]).status_code == 400


def test_put_endpoint_guards(api):
    as_admin(api)
    put_pr(total="100000", paid="60000", status="partial")
    api.svc.submissions.table.put_item(Item={"id": "sub_a", "account_id": ACC, "payment_request_id": "pr1",
                                             "amount": 60000, "status": "approved", "source": "player"})
    payload = {"createDate": "2026-01-01", "dueDate": "2999-01-01", "concept": "c", "description": "d",
               "category": "monthly", "group": WS, "paymentRequestTo": [{"id": PAYEE, "name": "J", "email": "j@x.com"}],
               "userPrice": 50000}
    assert api.put("/payment_requests/pr1", params={"workspace_id": WS}, json=payload).status_code == 409
    payload["userPrice"] = 100000
    payload["status"] = "partial"  # same as current: accepted
    assert api.put("/payment_requests/pr1", params={"workspace_id": WS}, json=payload).status_code == 200
    payload["status"] = "pending"
    assert api.put("/payment_requests/pr1", params={"workspace_id": WS}, json=payload).status_code == 409
    payload.update(status="bogus")
    assert api.put("/payment_requests/pr1", params={"workspace_id": WS}, json=payload).status_code == 422
    put_pr("pr2", created_time=2)
    payload.update(status="partial", allowInstallments=True, maxInstallments=1)
    assert api.put("/payment_requests/pr2", params={"workspace_id": WS}, json=payload).status_code == 422


def test_create_copies_installment_settings_and_presign_is_restricted(api):
    as_admin(api)
    payload = {"createDate": "2026-01-01", "dueDate": "2999-01-01", "concept": "c", "description": "d",
               "category": "monthly", "group": WS, "paymentRequestTo": [{"id": PAYEE, "name": "J", "email": "j@x.com"}],
               "userPrice": 90000, "allowInstallments": True, "maxInstallments": 3}
    created = api.post("/payment_requests", params={"workspace_id": WS}, json=payload)
    assert created.status_code == 200, created.text
    item = created.json()[0]
    assert item["allowInstallments"] is True and item["maxInstallments"] == 3 and item["originalAmount"] == 90000

    files = [{"file_name": "r1.png", "content_type": "image/png"}]
    ok = api.post(f"/payment_requests/{item['id']}/generate-presigned-urls", params={"workspace_id": WS}, json=files)
    assert ok.status_code == 200 and "r1.png" in ok.json()["urls"]
    api.state.update(user="other", ws_role="user", role="user")
    assert api.post(f"/payment_requests/{item['id']}/generate-presigned-urls", params={"workspace_id": WS}, json=files).status_code == 403
