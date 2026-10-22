"""The Lambda zip must ship scheduled_handler.py.

The PaymentScheduler Lambda (handler `scheduled_handler.handler`) failed every night with
`Runtime.ImportModuleError: No module named 'scheduled_handler'` because the packaging scripts only
copied app.py/auth.py/di.py/JWTBearer.py from the repo root, so the overdue job never ran.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _root_py_files(path: Path) -> list[str]:
    match = re.search(r'^\s*ROOT_PY_FILES="([^"]*)"', path.read_text(), flags=re.M)
    assert match, f"ROOT_PY_FILES not found in {path.name}"
    return match.group(1).split()


def test_handler_module_exists_where_the_scheduler_expects_it():
    assert (ROOT / "scheduled_handler.py").is_file()
    assert "def handler(" in (ROOT / "scheduled_handler.py").read_text()


def test_every_packaging_path_ships_the_handler():
    for relative in (".github/workflows/deploy.yml", "package_for_lambda.sh"):
        assert "scheduled_handler.py" in _root_py_files(ROOT / relative), relative


def test_the_build_fails_when_the_handler_is_missing_from_the_zip():
    for relative in (".github/workflows/deploy.yml", "package_for_lambda.sh"):
        text = (ROOT / relative).read_text()
        assert re.search(r"unzip -l .*grep -q .* scheduled_handler\.py", text), relative
