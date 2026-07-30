"""Core-only callers must not import or initialize the optional SQLite adapter."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_core_import_plan_and_run_do_not_load_sqlite_or_create_a_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "must-not-exist.sqlite3"
    program = f"""
import sys
from pathlib import Path


class BlockSqliteImports:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlite3" or fullname.startswith("sqlite3."):
            raise ModuleNotFoundError("sqlite3 is deliberately unavailable in this core smoke")
        return None


for module_name in list(sys.modules):
    if module_name == "sqlite3" or module_name.startswith("sqlite3."):
        del sys.modules[module_name]
sys.meta_path.insert(0, BlockSqliteImports())

import kazeflow
from kazeflow.assets import AssetRegistry

database = Path({str(database)!r})
registry = AssetRegistry()


def target():
    return "core-only"


registry.register(target)
plan = kazeflow.Flow(["target"], registry=registry).plan()
result = kazeflow.run(["target"], registry=registry)

assert plan.tasks[0].name == "target"
assert result.status.value == "success"
assert result.tasks[0].attempts[0].output == "core-only"
assert not database.exists()
assert "kazeflow.sqlite_store" not in sys.modules
assert not any(name == "sqlite3" or name.startswith("sqlite3.") for name in sys.modules)
"""

    completed = subprocess.run(
        [sys.executable, "-c", program],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == ""
    assert completed.stderr == ""
    assert not database.exists()
