"""Router smoke for /bank-accounts: wiring, status codes, snake_case wire, permissions."""
from .test_expenses_support import OTHER_WS, WS, build_client, env  # noqa: F401


def test_bank_accounts_endpoints_end_to_end(env):
    client = build_client(env)
    created = client.post(f"/bank-accounts?workspace_id={WS}", json={
        "name": "Bancolombia", "type": "club_bank", "opening_balance": 1500000.5, "opening_balance_date": "2026-01-01"})
    assert created.status_code == 200
    body = created.json()
    assert body["opening_balance"] == 1500000.5 and body["balance"] == 1500000.5
    assert {"opening_balance_date", "is_active", "created_at", "currency"} <= set(body)  # snake_case
    assert not any(k != k.lower() or "A" <= k[0] <= "Z" for k in body) and "openingBalance" not in body
    bank_id = body["id"]

    assert client.get(f"/bank-accounts?workspace_id={WS}").json()[0]["id"] == bank_id
    assert client.get(f"/bank-accounts/{bank_id}?workspace_id={WS}").status_code == 200
    adj = client.post(f"/bank-accounts/{bank_id}/adjustments?workspace_id={WS}",
                      json={"amount": -500.5, "date": "2026-02-01", "note": "retiro"})
    assert adj.status_code == 200 and adj.json()["source"] == "manual" and adj.json()["bank_account_id"] == bank_id
    assert [a["id"] for a in client.get(f"/bank-accounts/{bank_id}/adjustments?workspace_id={WS}").json()] == [adj.json()["id"]]
    assert client.get(f"/bank-accounts/{bank_id}?workspace_id={WS}").json()["balance"] == 1499500
    put = client.put(f"/bank-accounts/{bank_id}?workspace_id={WS}", json={"name": "Nueva", "is_active": True})
    assert put.status_code == 200 and put.json()["name"] == "Nueva"
    assert client.delete(f"/bank-accounts/{bank_id}?workspace_id={WS}").json()["is_active"] is False
    assert client.delete(f"/bank-accounts/{bank_id}?workspace_id={WS}").status_code == 200


def test_status_codes_and_permissions(env):
    client = build_client(env)
    assert client.post(f"/bank-accounts?workspace_id={WS}", json={"name": "x", "type": "nope",
                       "opening_balance": 0, "opening_balance_date": "2026-01-01"}).status_code == 422
    assert client.post(f"/bank-accounts?workspace_id={WS}", json={"name": "x"}).status_code == 422
    assert client.get(f"/bank-accounts/bac_missing?workspace_id={WS}").status_code == 404
    assert client.get("/bank-accounts").status_code in (403, 422)  # workspace_id is required
    # a plain member of the workspace, a stranger and a tournament account are all refused
    assert build_client(env, workspace_roles={WS: "user"}).get(f"/bank-accounts?workspace_id={WS}").status_code == 403
    assert build_client(env, workspace_roles={}).get(f"/bank-accounts?workspace_id={WS}").status_code == 403
    assert build_client(env, club=False).get(f"/bank-accounts?workspace_id={WS}").status_code == 403
    # admin of ANOTHER workspace only: the requested workspace is what counts
    other = build_client(env, workspace_roles={OTHER_WS: "admin"})
    assert other.get(f"/bank-accounts?workspace_id={WS}").status_code == 403
