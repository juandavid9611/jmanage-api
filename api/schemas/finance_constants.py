"""Enums and limits shared by the finance module (bank accounts, expenses, budget)."""

# Wire values of the finance module are snake_case (decided in jmanage-web#175 / #191).

EXPENSE_TEAMS = ("masculino", "femenino", "master", "general")

# Same list the frontend offers (expense-new-edit-form.jsx) and the budget catalog reuses.
EXPENSE_CATEGORIES = (
    "Cancha",
    "Entrenador",
    "Arbitraje",
    "Transporte",
    "Indumentaria",
    "Servicios",
    "Torneos",
    "Multas",
    "Otros",
)

BANK_ACCOUNT_TYPES = ("personal_bank", "club_bank", "cash", "other")

EXPENSE_STATUSES = ("pendiente", "aprobado", "rechazado", "pagado")

# Id prefixes (`<prefix>_<uuid4 hex>`).
ID_PREFIXES = {
    "bank_account": "bac",
    "adjustment": "adj",
    "expense": "exp",
    "expense_payment": "epy",
    "budget_item": "bgi",
    "budget_quantity": "bgq",
    "payment_submission": "sub",
}

MAX_TEXT = 280
MAX_NAME = 120
MAX_FILE_NAME = 120
