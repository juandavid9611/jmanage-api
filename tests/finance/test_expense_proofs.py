"""PR #177's expense form uploads attachments (presigned PUT) and sends `proof_images` on create and edit; the edit
form sends back whatever the detail returned. The attachments belong to the expense (quote, invoice), the payments
carry their own proofs."""
import pytest
from fastapi import HTTPException

from api.schemas.expenses import CreateExpense, ExpenseFileSpec, UpdateExpense
from tests.finance.test_expenses_support import ACC, OTHER_WS, WS, approved_expense, env, make_bank, make_expense, pay  # noqa: F401


def create(env, **over):  # noqa: F811
    base = dict(team="general", category="Cancha", detail="Cancha octubre", expense_date="2026-10-10", total_amount=100)
    base.update(over)
    return env.expenses.create(CreateExpense(**base), WS, ACC, "u1")


def test_attachments_sent_on_create_come_back_as_urls_never_as_raw_keys(env):  # noqa: F811
    exp = create(env, proof_images=["cotizacion.pdf", "factura.png"])
    assert len(exp["proof_images"]) == 2 and all(u.startswith("https://") for u in exp["proof_images"])
    assert "proof_keys" not in exp and not any("proof_keys" in str(p) for p in env.expenses.list_expenses(WS, ACC))
    assert env.expenses.get(exp["id"], WS, ACC)["proof_images"] and "proof_keys" not in env.expenses.get(exp["id"], WS, ACC)
    assert env.expense_repo.get(exp["id"], ACC)["proof_keys"][0].endswith("/uploads/cotizacion.pdf")


def test_the_edit_form_round_trip_keeps_old_files_adds_new_ones_and_drops_foreign_urls(env):  # noqa: F811
    exp = create(env, proof_images=["a.png"])
    other = create(env, proof_images=["secret.png"], detail="otro")
    foreign_url = other["proof_images"][0]
    # the form sends back what the detail returned + a new file + a URL of ANOTHER expense it must not be able to attach
    sent = exp["proof_images"] + ["b.png", foreign_url]
    updated = env.expenses.update(exp["id"], UpdateExpense(proof_images=sent), WS, ACC, "u1")
    keys = env.expense_repo.get(exp["id"], ACC)["proof_keys"]
    assert [k.rsplit("/", 1)[1] for k in keys] == ["a.png", "b.png"] and len(updated["proof_images"]) == 2
    assert all(f"/expenses/{exp['id']}/" in k for k in keys)
    # removing one in the form removes it
    env.expenses.update(exp["id"], UpdateExpense(proof_images=["b.png"]), WS, ACC, "u1")
    assert [k.rsplit("/", 1)[1] for k in env.expense_repo.get(exp["id"], ACC)["proof_keys"]] == ["b.png"]


def test_a_bad_file_name_is_a_422_and_a_missing_field_leaves_proofs_alone(env):  # noqa: F811
    for bad in ("../x.png", "a/b.png", "", "x" * 200):
        with pytest.raises(HTTPException) as e:
            create(env, proof_images=[bad])
        assert e.value.status_code == 422
    exp = create(env, proof_images=["a.png"])
    env.expenses.update(exp["id"], UpdateExpense(detail="otra cosa"), WS, ACC, "u1")  # no proof_images: untouched
    assert len(env.expense_repo.get(exp["id"], ACC)["proof_keys"]) == 1


def test_attaching_files_does_not_send_an_approved_expense_back_to_pending(env):  # noqa: F811
    exp = approved_expense(env, total=100)
    env.expenses.update(exp["id"], UpdateExpense(proof_images=["extra.png"]), WS, ACC, "u1")
    assert env.expense_repo.get(exp["id"], ACC)["status"] == "aprobado"


def test_payment_proofs_are_listed_after_the_expense_attachments(env):  # noqa: F811
    exp = create(env, proof_images=["cotizacion.pdf"])
    env.expenses.approve(exp["id"], WS, ACC, "u2")
    bank = make_bank(env)
    pay(env, exp["id"], bank["id"], files=["transferencia.png"])
    detail = env.expenses.get(exp["id"], WS, ACC)
    assert len(detail["proof_images"]) == 2 and "cotizacion" in detail["proof_images"][0] and "transferencia" in detail["proof_images"][1]
