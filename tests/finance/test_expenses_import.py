"""migrations/import_expenses_from_csv.py: dry run, safety guard, idempotent writes, API compatibility."""
import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest

from .test_expenses_support import ACC, WS, env, make_bank  # noqa: F401

SCRIPT = Path(__file__).resolve().parents[2] / "migrations" / "import_expenses_from_csv.py"
HEADER = "expense_date,team,category,subcategory,detail,beneficiary_name,total_amount,paid_amount,paid_at,bank_account_id\n"


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("import_expenses_from_csv", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def csv_file(tmp_path, *rows):
    path = tmp_path / "history.csv"
    path.write_text(HEADER + "\n".join(rows) + "\n", encoding="utf-8")
    return str(path)


def args(script, path, bank=None, *extra):
    cmd = [path, "--account-id", ACC, "--workspace-id", WS, *extra]
    if bank:
        cmd += ["--bank-account-id", bank]
    return cmd


def run(script, argv):
    return script.run(script.parse_args(argv))


ROWS = (
    "2026-01-10,general,Cancha,Servita,Cancha enero,Servita,500000,500000,2026-01-12,",
    "2026-02-10,masculino,Arbitraje,,Arbitraje fecha 1,Juez,80000.50,30000,,",
    "2026-03-10,femenino,Otros,,Sin pagar,,1000,0,,",
)


def test_dry_run_writes_nothing(script, env, tmp_path, capsys):
    bank = make_bank(env)
    stats = run(script, args(script, csv_file(tmp_path, *ROWS), bank["id"]))
    assert stats["rows"] == 3 and stats["errors"] == 0 and stats["written"] == 0
    assert "[dry-run]" in capsys.readouterr().out
    assert env.expense_repo.list_filtered(ACC, WS) == []


def test_confirm_writes_expenses_and_ledger_compatible_with_the_api(script, env, tmp_path):
    bank = make_bank(env, opening=1_000_000)
    path = csv_file(tmp_path, *ROWS)
    assert run(script, args(script, path, bank["id"], "--confirm"))["written"] == 3
    by_detail = {e["detail"]: e for e in env.expenses.list_expenses(WS, ACC)}
    assert by_detail["Cancha enero"]["status"] == "pagado" and by_detail["Cancha enero"]["paid_amount"] == 500000
    assert by_detail["Arbitraje fecha 1"]["status"] == "aprobado" and by_detail["Arbitraje fecha 1"]["paid_amount"] == 30000
    assert by_detail["Sin pagar"]["status"] == "aprobado" and by_detail["Sin pagar"]["account_id"] is None
    detail = env.expenses.get(by_detail["Cancha enero"]["id"], WS, ACC)
    assert detail["payments"][0]["amount"] == 500000 and detail["source"] == "import"
    assert env.payment_repo.list_by_expense(detail["id"], ACC)[0]["source"] == "import"
    assert env.banks.get(bank["id"], WS, ACC)["balance"] == 1_000_000 - 500000 - 30000
    # an imported partially-paid expense can still be completed through the API
    from .test_expenses_support import pay
    pay(env, by_detail["Arbitraje fecha 1"]["id"], bank["id"], amount=50000.5, key="key-after-import")
    assert env.expenses.get(by_detail["Arbitraje fecha 1"]["id"], WS, ACC)["status"] == "pagado"
    # re-running the same file duplicates nothing
    again = run(script, args(script, path, bank["id"], "--confirm"))
    assert again["written"] == 0 and again["skipped_existing"] == 3
    assert len(env.expenses.list_expenses(WS, ACC)) == 3


def test_bad_rows_are_reported_not_written(script, env, tmp_path):
    bank = make_bank(env)
    path = csv_file(
        tmp_path,
        "2026-01-10,general,Cancha,,ok,,100,0,,",
        "10/01/2026,general,Cancha,,fecha mala,,100,0,,",
        "2026-01-10,nadie,Cancha,,equipo malo,,100,0,,",
        "2026-01-10,general,Cancha,,paga de mas,,100,150,,",
        "2026-01-10,general,Cancha,,monto,,1.234,0,,",
    )
    stats = run(script, args(script, path, bank["id"], "--confirm"))
    assert stats["written"] == 1 and stats["errors"] == 4
    assert [e["detail"] for e in env.expenses.list_expenses(WS, ACC)] == ["ok"]


def test_paid_rows_need_a_bank_account_and_it_must_belong_to_the_tenant(script, env, tmp_path):
    path = csv_file(tmp_path, ROWS[0])
    assert run(script, args(script, path))["errors"] == 1
    foreign = make_bank(env, acct="acc2")
    with pytest.raises(SystemExit):
        run(script, args(script, path, foreign["id"]))


def test_refuses_non_dev_tables_without_allow_prod(script, env, tmp_path, monkeypatch):
    monkeypatch.setenv("EXPENSE_TABLE_NAME", "Jmanage-Expense-vittoriacd-prod")
    with pytest.raises(SystemExit) as exc:
        run(script, args(script, csv_file(tmp_path, ROWS[2])))
    assert "allow-prod" in str(exc.value)
    assert run(script, args(script, csv_file(tmp_path, ROWS[2]), None, "--allow-prod"))["rows"] == 1  # dry run
