"""Foundation: money parsing, the transaction helper and the adjustment repo (on moto)."""
from decimal import Decimal

import pytest

from core.finance_money import MoneyError, new_id, parse_money, plain_number, valid_file_name, valid_idempotency_key
from repositories.bank_adjustment_repo_ddb import AdjustmentExists, BankAdjustmentRepo, payment_request_adjustment_id
from repositories.ddb_session import bank_account_table, expense_table
from repositories.ddb_transactions import TransactionConflict, query_all, transact_write, tx_put, tx_update


# ---------------------------------------------------------------- money
def test_parse_money_is_exact_and_bounded():
    assert parse_money(0.1) + parse_money(0.2) == Decimal("0.30")
    assert parse_money("1500000") == Decimal("1500000.00")
    assert parse_money(Decimal("12.5")) == Decimal("12.50")
    for bad in (True, None, "abc", float("nan"), float("inf"), "1.234", -5, 0, 10**13):
        with pytest.raises(MoneyError):
            parse_money(bad)
    assert parse_money(-5, allow_negative=True) == Decimal("-5.00")
    assert parse_money(0, allow_zero=True) == Decimal("0.00")


def test_plain_number_and_ids_and_keys():
    assert plain_number(Decimal("15000.00")) == 15000 and isinstance(plain_number(Decimal("15000.00")), int)
    assert plain_number(Decimal("1.5")) == 1.5
    assert new_id("exp").startswith("exp_") and new_id("exp") != new_id("exp")
    assert valid_idempotency_key("abc-12345_X") and not valid_idempotency_key("short")
    assert not valid_idempotency_key("has space 12345") and not valid_idempotency_key(None)
    assert valid_file_name("comprobante.png") and not valid_file_name("../x.png") and not valid_file_name("a/b.png")
    assert not valid_file_name("") and not valid_file_name("x" * 200)


# ---------------------------------------------------------------- transactions
def test_transaction_is_all_or_nothing_and_reports_the_failed_condition(finance_db):
    accounts, expenses = bank_account_table(), expense_table()
    expenses.put_item(Item={"id": "exp_1", "paid_amount": Decimal("0"), "total_amount": Decimal("100")})

    transact_write([
        tx_put(accounts, {"id": "bac_1", "name": "Caja"}, condition="attribute_not_exists(id)"),
        tx_update(expenses, {"id": "exp_1"}, "SET paid_amount = paid_amount + :a",
                  condition="paid_amount <= :max", values={":a": Decimal("60"), ":max": Decimal("40")}),
    ])
    assert accounts.get_item(Key={"id": "bac_1"})["Item"]["name"] == "Caja"
    assert expenses.get_item(Key={"id": "exp_1"})["Item"]["paid_amount"] == Decimal("60")

    # second payment of 60 would overpay (60 > 100-60): nothing of the transaction is written
    with pytest.raises(TransactionConflict) as caught:
        transact_write([
            tx_put(accounts, {"id": "bac_2", "name": "Otra"}, condition="attribute_not_exists(id)"),
            tx_update(expenses, {"id": "exp_1"}, "SET paid_amount = paid_amount + :a",
                      condition="paid_amount <= :max", values={":a": Decimal("60"), ":max": Decimal("40")}),
        ])
    assert caught.value.conditional_failed == [1]
    assert "Item" not in accounts.get_item(Key={"id": "bac_2"})
    assert expenses.get_item(Key={"id": "exp_1"})["Item"]["paid_amount"] == Decimal("60")


def test_replaying_a_put_with_the_same_id_conflicts(finance_db):
    accounts = bank_account_table()
    item = tx_put(accounts, {"id": "bac_x"}, condition="attribute_not_exists(id)")
    transact_write([item])
    with pytest.raises(TransactionConflict) as caught:
        transact_write([item])
    assert caught.value.conditional_failed == [0]


def test_query_all_follows_pagination(finance_db):
    table = bank_account_table()
    for i in range(7):
        table.put_item(Item={"id": f"bac_{i}", "account_id": "acc1"})
    # Limit=3 forces 3 pages: a single query would return only 3 rows
    rows = query_all(table, IndexName="account_id_index",
                     KeyConditionExpression="account_id = :a", ExpressionAttributeValues={":a": "acc1"}, Limit=3)
    assert len(rows) == 7


# ---------------------------------------------------------------- adjustment repo
def test_adjustment_repo_is_tenant_scoped_and_idempotent(finance_db):
    repo = BankAdjustmentRepo()
    adj_id = payment_request_adjustment_id("pr1", "sub1")
    assert adj_id == "adj_pr_pr1_sub1"
    repo.create({"id": adj_id, "account_id": "acc1", "bank_account_id": "bac_1", "amount": Decimal("50000")})
    with pytest.raises(AdjustmentExists):
        repo.create({"id": adj_id, "account_id": "acc1", "bank_account_id": "bac_1", "amount": Decimal("50000")})
    repo.create({"id": "adj_2", "account_id": "acc2", "bank_account_id": "bac_1", "amount": Decimal("1")})
    assert repo.get(adj_id, "acc1")["amount"] == Decimal("50000")
    assert repo.get(adj_id, "acc2") is None
    assert [a["id"] for a in repo.list_by_bank_account("bac_1", "acc1")] == [adj_id]
