"""Router smoke for /expenses: wiring, status codes, snake_case wire, permissions, the frontend contract."""
from .test_expenses_support import OTHER_WS, WS, build_client, env, make_bank  # noqa: F401


def test_expense_flow_through_the_router(env):
    bank = make_bank(env, opening=1000)
    maker, approver = build_client(env, user="u1"), build_client(env, user="u2")
    created = maker.post(f"/expenses?workspace_id={WS}", json={
        "team": "general", "category": "Cancha", "subcategory": "Servita", "detail": "Cancha", "beneficiary_name": "Servita",
        "expense_date": "2026-05-10", "total_amount": 525000, "account_id": "", "proof_images": []})  # what the web form sends
    assert created.status_code == 200, created.text
    exp = created.json()
    assert exp["status"] == "pendiente" and exp["total_amount"] == 525000 and exp["requested_by"] == "u1"
    assert {"beneficiary_name", "expense_date", "paid_amount", "proof_images", "requested_by"} <= set(exp)
    assert "totalAmount" not in exp
    eid = exp["id"]

    assert maker.get(f"/expenses/{eid}?workspace_id={WS}").json()["id"] == eid
    assert maker.put(f"/expenses/{eid}?workspace_id={WS}", json={"detail": "Cancha mayo"}).json()["detail"] == "Cancha mayo"
    assert maker.post(f"/expenses/{eid}/approve?workspace_id={WS}", json={}).status_code == 409  # own expense, 2 admins
    assert approver.post(f"/expenses/{eid}/approve?workspace_id={WS}", json={}).json()["status"] == "aprobado"

    urls = maker.post(f"/expenses/{eid}/generate-presigned-urls?workspace_id={WS}&idempotency_key=key-router-01",
                      json=[{"file_name": "pago.png", "content_type": "image/png"}])
    assert urls.status_code == 200 and "pago.png" in urls.json()["urls"]

    # the exact body the frontend sends (no idempotency_key, amount omitted), then a keyed retry
    paid = maker.post(f"/expenses/{eid}/mark-paid?workspace_id={WS}", json={
        "account_id": bank["id"], "paid_at": "2026-05-11", "file_names": ["pago.png"], "idempotency_key": "key-router-01"})
    assert paid.status_code == 200, paid.text
    assert paid.json()["amount"] == 525000 and paid.json()["account_id"] == bank["id"]
    retry = maker.post(f"/expenses/{eid}/mark-paid?workspace_id={WS}", json={
        "account_id": bank["id"], "paid_at": "2026-05-11", "file_names": ["pago.png"], "idempotency_key": "key-router-01"})
    assert retry.status_code == 200 and retry.json() == paid.json()
    detail = maker.get(f"/expenses/{eid}?workspace_id={WS}").json()
    assert detail["status"] == "pagado" and detail["paid_amount"] == 525000 and len(detail["payments"]) == 1
    assert detail["proof_images"] and detail["account_id"] == bank["id"]

    listed = maker.get(f"/expenses?workspace_id={WS}&status=pagado&account_id={bank['id']}&team=general"
                       f"&category=Cancha&date_from=2026-05-01&date_to=2026-05-31").json()
    assert [e["id"] for e in listed] == [eid]
    assert maker.get(f"/expenses?workspace_id={WS}&status=pendiente").json() == []
    assert maker.get(f"/expenses?workspace_id={WS}&status=bogus").status_code == 422


def test_reject_delete_and_error_codes(env):
    client = build_client(env, user="u2")
    eid = client.post(f"/expenses?workspace_id={WS}", json={
        "team": "general", "category": "Otros", "detail": "x", "expense_date": "2026-05-10", "total_amount": 10}).json()["id"]
    assert client.post(f"/expenses/{eid}/reject?workspace_id={WS}", json={}).status_code == 422  # reason required
    rejected = client.post(f"/expenses/{eid}/reject?workspace_id={WS}", json={"reason": "duplicado"})
    assert rejected.status_code == 200 and rejected.json()["rejection_reason"] == "duplicado"
    assert client.delete(f"/expenses/{eid}?workspace_id={WS}").status_code == 409
    pending = client.post(f"/expenses?workspace_id={WS}", json={
        "team": "general", "category": "Otros", "detail": "y", "expense_date": "2026-05-10", "total_amount": 10}).json()["id"]
    assert client.delete(f"/expenses/{pending}?workspace_id={WS}").json() == {"deleted_expense_id": pending}
    assert client.get(f"/expenses/{pending}?workspace_id={WS}").status_code == 404
    assert client.post(f"/expenses/{eid}/mark-paid?workspace_id={WS}", json={"paid_at": "2026-05-11"}).status_code == 422
    assert client.get(f"/expenses/{eid}?workspace_id={OTHER_WS}").status_code == 404


def test_expense_permissions(env):
    assert build_client(env, workspace_roles={WS: "user"}).get(f"/expenses?workspace_id={WS}").status_code == 403
    assert build_client(env, workspace_roles={OTHER_WS: "admin"}).get(f"/expenses?workspace_id={WS}").status_code == 403
    assert build_client(env, club=False).post(f"/expenses?workspace_id={WS}", json={}).status_code == 403
    assert build_client(env).get("/expenses").status_code in (403, 422)  # workspace_id required
