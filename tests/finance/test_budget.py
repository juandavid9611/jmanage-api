"""Budget catalog (items + monthly quantities) against moto, plus FastAPI wiring smoke tests."""
import sys
from decimal import Decimal
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# `auth` downloads the Cognito JWKS and loads a .env at import time: stub both so the module imports offline.
if "auth" not in sys.modules:
    with mock.patch("requests.get") as _get, mock.patch("dotenv.load_dotenv"):
        _get.return_value.json.return_value = {"keys": []}
        import auth  # noqa: F401

from api import budget as budget_api  # noqa: E402
from api.budget import router  # noqa: E402
from auth import get_account_id  # noqa: E402
from di import get_budget_service  # noqa: E402
from repositories.budget_item_repo_ddb import BudgetItemRepo
from repositories.budget_quantity_repo_ddb import BudgetQuantityRepo
from repositories.ddb_session import budget_item_table, budget_quantity_table
from services.budget_service import BudgetConflict, BudgetNotFound, BudgetService, BudgetValidationError

ACC, WS = "acc_1", "ws_1"


@pytest.fixture
def svc(finance_db):
    return BudgetService(BudgetItemRepo(), BudgetQuantityRepo())


def _item(svc, name="Cancha Servita", category="Cancha", cost=525000, acc=ACC, ws=WS):
    return svc.create_item(acc, ws, category, name, cost)


# ------------------------------------------------------------------ items
def test_create_item_returns_item_and_decimal_cost_is_exact(svc):
    item = _item(svc, cost=0.1)
    assert item["id"].startswith("bgi_") and item["is_active"] is True and item["unit_cost"] == 0.1
    stored = budget_item_table().get_item(Key={"id": item["id"]})["Item"]
    assert stored["unit_cost"] == Decimal("0.10") and stored["account_id"] == ACC
    # 0.1 + 0.2 stays exact as a stored/parsed Decimal
    other = _item(svc, name="B", cost=0.2)
    total = sum(budget_item_table().get_item(Key={"id": i["id"]})["Item"]["unit_cost"] for i in (item, other))
    assert total == Decimal("0.30")


@pytest.mark.parametrize("cost", [0, -1, "abc", 1.234, None, True, float("nan")])
def test_bad_unit_cost_is_rejected(svc, cost):
    with pytest.raises(BudgetValidationError):
        _item(svc, cost=cost)


def test_category_and_name_validation(svc):
    with pytest.raises(BudgetValidationError):
        _item(svc, category="Nope")
    with pytest.raises(BudgetValidationError):
        _item(svc, name="   ")
    with pytest.raises(BudgetValidationError):
        _item(svc, name="x" * 121)


def test_name_unique_per_workspace_and_category_among_active(svc):
    first = _item(svc)
    with pytest.raises(BudgetConflict):
        _item(svc, name="  cancha   SERVITA ")
    _item(svc, category="Otros")            # same name, other category: fine
    _item(svc, ws="ws_2")                   # same name, other workspace: fine
    svc.delete_item(ACC, WS, first["id"])
    again = _item(svc)                      # inactive items do not block the name
    with pytest.raises(BudgetConflict):     # reactivating the old one would duplicate
        svc.update_item(ACC, WS, first["id"], "Cancha", "Cancha Servita", 1, True)
    svc.update_item(ACC, WS, again["id"], "Cancha", "Cancha Servita", 600000, None)  # self is excluded


def test_update_item_and_inactive_stays_inactive(svc):
    item = _item(svc)
    upd = svc.update_item(ACC, WS, item["id"], "Otros", "Nuevo", 10.5, False)
    assert (upd["category"], upd["name"], upd["unit_cost"], upd["is_active"]) == ("Otros", "Nuevo", 10.5, False)
    assert svc.update_item(ACC, WS, item["id"], "Otros", "Nuevo", 11, None)["is_active"] is False
    assert upd["created_at"] == item["created_at"]


def test_delete_is_soft_idempotent_and_listed(svc):
    item = _item(svc)
    assert svc.delete_item(ACC, WS, item["id"])["is_active"] is False
    assert svc.delete_item(ACC, WS, item["id"])["is_active"] is False
    assert [i["is_active"] for i in svc.list_items(ACC, WS)] == [False]
    assert budget_item_table().get_item(Key={"id": item["id"]}).get("Item")


def test_tenant_and_workspace_isolation(svc):
    mine = _item(svc)
    foreign = _item(svc, acc="acc_2", ws="ws_x")
    assert svc.list_items(ACC, WS) == [mine]
    assert svc.list_items(ACC, "ws_other") == []
    repo = BudgetItemRepo()
    assert repo.get(mine["id"], ACC)["id"] == mine["id"]
    assert repo.get(mine["id"], "acc_2") is None and repo.get("bgi_missing", ACC) is None
    assert repo.get_for_workspace(mine["id"], ACC, "ws_other") is None
    for call in (
        lambda: svc.update_item("acc_2", "ws_x", mine["id"], "Cancha", "x", 1, None),
        lambda: svc.update_item(ACC, "ws_other", mine["id"], "Cancha", "x", 1, None),
        lambda: svc.delete_item(ACC, "ws_other", mine["id"]),
        lambda: svc.delete_item("acc_2", "ws_x", mine["id"]),
    ):
        with pytest.raises(BudgetNotFound):
            call()
    assert svc.list_items("acc_2", "ws_x") == [foreign]


def test_list_items_follows_pagination(finance_db):
    table = budget_item_table()
    pad = "p" * 3000
    with table.batch_writer() as batch:
        for n in range(500):  # ~1.6 MB, more than one 1 MB query page
            batch.put_item(Item={"id": f"bgi_{n:04d}", "account_id": ACC, "workspace_id": WS, "category": "Otros",
                                 "name": f"item {n:04d}", "unit_cost": Decimal("1"), "is_active": True, "pad": pad})
    pages = []

    class Spy:
        def __init__(self, inner):
            self._inner = inner
            self.name = inner.name

        def query(self, **kw):
            pages.append(1)
            return self._inner.query(**kw)

    repo = BudgetItemRepo()
    repo._table = Spy(repo._table)
    assert len(repo.list_by_workspace(ACC, WS)) == 500
    assert len(pages) >= 2


# ------------------------------------------------------------- quantities
def test_set_and_get_quantities_semantics(svc):
    a, b, c = (_item(svc, name=n)["id"] for n in "ABC")
    out = svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 4}, {"item_id": b, "quantity": 2}])
    assert out == sorted([{"item_id": a, "quantity": 4}, {"item_id": b, "quantity": 2}], key=lambda r: r["item_id"])
    # unlisted untouched, 0 deletes, c never gets a row
    out = svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 0}, {"item_id": c, "quantity": 0}])
    assert out == [{"item_id": b, "quantity": 2}]
    assert budget_quantity_table().get_item(Key={"id": f"bgq_{a}_2026-10"}).get("Item") is None
    assert svc.list_quantities(ACC, WS, "2026-11") == []


def test_replay_is_idempotent(svc):
    a = _item(svc)["id"]
    body = [{"item_id": a, "quantity": 3}]
    first = svc.set_quantities(ACC, WS, "2026-10", body)
    assert svc.set_quantities(ACC, WS, "2026-10", body) == first
    assert budget_quantity_table().scan()["Count"] == 1
    row = budget_quantity_table().get_item(Key={"id": f"bgq_{a}_2026-10"})["Item"]
    assert (row["account_id"], row["workspace_id"], row["month"], row["quantity"]) == (ACC, WS, "2026-10", 3)


@pytest.mark.parametrize("month", ["2026-13", "2026-00", "26-10", "2026-1", "2026-10-01", "", "abcd-ef"])
def test_month_validation(svc, month):
    with pytest.raises(BudgetValidationError):
        svc.list_quantities(ACC, WS, month)
    with pytest.raises(BudgetValidationError):
        svc.set_quantities(ACC, WS, month, [])


def test_atomic_validation_nothing_written(svc):
    a = _item(svc)["id"]
    foreign = _item(svc, acc="acc_2", ws="ws_x")["id"]
    other_ws = _item(svc, ws="ws_2")["id"]
    for bad in ("bgi_ghost", foreign, other_ws):
        with pytest.raises(BudgetValidationError) as err:
            svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 5}, {"item_id": bad, "quantity": 1}])
        assert bad in str(err.value)
    assert budget_quantity_table().scan()["Count"] == 0


@pytest.mark.parametrize("qty", [-1, 100001, 1.5, True, "3", None])
def test_quantity_bounds_nothing_written(svc, qty):
    a = _item(svc)["id"]
    with pytest.raises(BudgetValidationError):
        svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 1}, {"item_id": a + "x", "quantity": qty}])
    assert budget_quantity_table().scan()["Count"] == 0
    svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 100000}])


def test_duplicate_item_ids_rejected(svc):
    a = _item(svc)["id"]
    with pytest.raises(BudgetValidationError):
        svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 1}, {"item_id": a, "quantity": 2}])
    assert budget_quantity_table().scan()["Count"] == 0


def test_soft_deleted_item_keeps_quantities_readable(svc):
    a = _item(svc)["id"]
    svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 7}])
    svc.delete_item(ACC, WS, a)
    assert svc.list_quantities(ACC, WS, "2026-10") == [{"item_id": a, "quantity": 7}]


def test_quantities_isolated_by_tenant_and_workspace(svc):
    a = _item(svc)["id"]
    b = _item(svc, acc="acc_2", ws="ws_x")["id"]
    svc.set_quantities(ACC, WS, "2026-10", [{"item_id": a, "quantity": 1}])
    svc.set_quantities("acc_2", "ws_x", "2026-10", [{"item_id": b, "quantity": 9}])
    assert svc.list_quantities(ACC, WS, "2026-10") == [{"item_id": a, "quantity": 1}]
    assert svc.list_quantities(ACC, "ws_other", "2026-10") == []
    assert svc.list_quantities("acc_2", "ws_x", "2026-10") == [{"item_id": b, "quantity": 9}]


def test_more_than_100_quantities_in_one_call(svc):
    ids = [_item(svc, name=f"item {n}", category="Otros")["id"] for n in range(130)]
    out = svc.set_quantities(ACC, WS, "2026-10", [{"item_id": i, "quantity": 2} for i in ids])
    assert len(out) == 130


# ------------------------------------------------------------ HTTP wiring
@pytest.fixture
def client(finance_db):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[budget_api.CLUB_ONLY] = lambda: True
    app.dependency_overrides[budget_api.ADMIN] = lambda: True
    app.dependency_overrides[get_account_id] = lambda: ACC
    app.dependency_overrides[get_budget_service] = lambda: BudgetService(BudgetItemRepo(), BudgetQuantityRepo())
    return TestClient(app)


def test_http_item_crud_and_snake_case(client):
    q = {"workspace_id": WS}
    resp = client.post("/budget-items", params=q, json={"category": "Cancha", "name": "Servita", "unit_cost": 525000})
    assert resp.status_code == 200
    item = resp.json()
    assert set(item) == {"id", "workspace_id", "category", "name", "unit_cost", "is_active", "created_at", "updated_at"}
    assert item["unit_cost"] == 525000
    assert client.post("/budget-items", params=q, json={"category": "Cancha", "name": "servita", "unit_cost": 1}).status_code == 409
    assert client.post("/budget-items", params=q, json={"category": "X", "name": "n", "unit_cost": 1}).status_code == 422
    assert client.post("/budget-items", params=q, json={"category": "Cancha", "name": "n", "unit_cost": 0}).status_code == 422
    assert client.post("/budget-items", json={"category": "Cancha", "name": "n", "unit_cost": 1}).status_code == 422  # no workspace
    put = client.put(f"/budget-items/{item['id']}", params=q,
                     json={"category": "Cancha", "name": "Servita", "unit_cost": 0.1, "is_active": True})
    assert put.status_code == 200 and put.json()["unit_cost"] == 0.1
    assert client.put("/budget-items/bgi_x", params=q, json={"category": "Cancha", "name": "n", "unit_cost": 1}).status_code == 404
    assert client.get("/budget-items", params={"workspace_id": "other"}).json() == []
    assert client.delete(f"/budget-items/{item['id']}", params={"workspace_id": "other"}).status_code == 404
    deleted = client.delete(f"/budget-items/{item['id']}", params=q)
    assert deleted.status_code == 200 and deleted.json()["is_active"] is False
    assert client.get("/budget-items", params=q).json()[0]["is_active"] is False


def test_http_quantities(client):
    q = {"workspace_id": WS, "month": "2026-10"}
    item = client.post("/budget-items", params={"workspace_id": WS},
                       json={"category": "Otros", "name": "n", "unit_cost": 5}).json()
    resp = client.put("/budget-quantities", params=q, json=[{"item_id": item["id"], "quantity": 3}])
    assert resp.status_code == 200 and resp.json() == [{"item_id": item["id"], "quantity": 3}]
    assert client.get("/budget-quantities", params=q).json() == [{"item_id": item["id"], "quantity": 3}]
    assert client.get("/budget-quantities", params={**q, "month": "2026-13"}).status_code == 422
    assert client.get("/budget-quantities", params={"workspace_id": WS}).status_code == 422
    assert client.put("/budget-quantities", params=q, json=[{"item_id": "ghost", "quantity": 1}]).status_code == 422
    assert client.put("/budget-quantities", params=q, json=[{"item_id": item["id"], "quantity": 1.5}]).status_code == 422
    dup = [{"item_id": item["id"], "quantity": 1}] * 2
    assert client.put("/budget-quantities", params=q, json=dup).status_code == 422
    assert client.get("/budget-quantities", params=q).json()[0]["quantity"] == 3
    assert client.put("/budget-quantities", params=q, json=[{"item_id": item["id"], "quantity": 0}]).json() == []


def test_http_requires_club_and_admin():
    """Without overrides the routes depend on the real checkers (wiring proof, no handler is reached)."""
    deps = {d.dependency for d in router.dependencies}
    assert deps == {budget_api.CLUB_ONLY, budget_api.ADMIN}
    assert budget_api.ADMIN.required_permissions == ["admin"]
    from fastapi import HTTPException

    app = FastAPI()
    app.include_router(router)

    def deny():
        raise HTTPException(status_code=403, detail="no")

    app.dependency_overrides[budget_api.ADMIN] = deny
    app.dependency_overrides[budget_api.CLUB_ONLY] = lambda: True
    app.dependency_overrides[get_account_id] = lambda: ACC
    resp = TestClient(app).get("/budget-items", params={"workspace_id": WS})
    assert resp.status_code == 403
