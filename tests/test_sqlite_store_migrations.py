"""Schema compatibility and migration safety tests for the SQLite run store."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import kazeflow.sqlite_store as sqlite_store
from kazeflow.sqlite_store import SQLiteRunStore


def _user_version(path: Path) -> int:
    with sqlite3.connect(path) as connection:
        return connection.execute("PRAGMA user_version").fetchone()[0]


def _has_table(path: Path, table_name: str) -> bool:
    with sqlite3.connect(path) as connection:
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            is not None
        )


def _make_v1_database(path: Path) -> None:
    with SQLiteRunStore(path) as store:
        assert store.schema_version == 1


def test_new_database_initializes_schema_version_one_transactionally(
    tmp_path: Path,
) -> None:
    database = tmp_path / "runs.sqlite3"

    with SQLiteRunStore(database) as store:
        assert store.schema_version == 1

    assert _user_version(database) == 1
    assert _has_table(database, "runs")


def test_future_schema_is_rejected_without_mutating_database(tmp_path: Path) -> None:
    database = tmp_path / "future.sqlite3"
    _make_v1_database(database)
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version = 2")
        before_tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()

    with pytest.raises(ValueError, match="newer than supported"):
        SQLiteRunStore(database)

    assert _user_version(database) == 2
    with sqlite3.connect(database) as connection:
        after_tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    assert after_tables == before_tables


def test_supported_forward_migration_advances_version_and_preserves_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "migrate.sqlite3"
    _make_v1_database(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
            VALUES ('existing', 1, 'success', '2026-07-31T00:00:00+00:00', '{}')
            """
        )

    monkeypatch.setattr(sqlite_store, "CURRENT_SCHEMA_VERSION", 2)
    monkeypatch.setattr(
        SQLiteRunStore,
        "_MIGRATIONS",
        {1: ("CREATE TABLE migration_marker (name TEXT NOT NULL)",)},
    )

    with SQLiteRunStore(database) as store:
        assert store.schema_version == 2

    assert _user_version(database) == 2
    assert _has_table(database, "migration_marker")
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT run_id FROM runs").fetchall() == [
            ("existing",)
        ]


def test_failed_migration_rolls_back_schema_version_and_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "rollback.sqlite3"
    _make_v1_database(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
            VALUES ('existing', 1, 'success', '2026-07-31T00:00:00+00:00', '{}')
            """
        )

    monkeypatch.setattr(sqlite_store, "CURRENT_SCHEMA_VERSION", 2)
    monkeypatch.setattr(
        SQLiteRunStore,
        "_MIGRATIONS",
        {
            1: (
                "CREATE TABLE transient_migration (name TEXT NOT NULL)",
                """
                INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
                VALUES ('partial', 1, 'success', '2026-07-31T00:00:01+00:00', '{}')
                """,
                "THIS IS NOT VALID SQL",
            )
        },
    )

    with pytest.raises(sqlite3.OperationalError, match="syntax error"):
        SQLiteRunStore(database)

    assert _user_version(database) == 1
    assert not _has_table(database, "transient_migration")
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT run_id FROM runs").fetchall() == [
            ("existing",)
        ]


@pytest.mark.parametrize(
    "commit_statement",
    [
        "; COMMIT",
        "-- a leading comment\n; COMMIT",
        "/* a leading comment */ ; COMMIT",
    ],
    ids=[
        "leading-semicolon",
        "line-comment-leading-semicolon",
        "block-comment-leading-semicolon",
    ],
)
def test_transaction_control_step_is_rejected_before_any_ddl_is_applied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, commit_statement: str
) -> None:
    database = tmp_path / "transaction-control.sqlite3"
    _make_v1_database(database)

    monkeypatch.setattr(sqlite_store, "CURRENT_SCHEMA_VERSION", 2)
    monkeypatch.setattr(
        SQLiteRunStore,
        "_MIGRATIONS",
        {
            1: (
                "CREATE TABLE must_roll_back (name TEXT NOT NULL)",
                commit_statement,
            )
        },
    )

    with pytest.raises(ValueError, match="must not control transactions: COMMIT"):
        SQLiteRunStore(database)

    assert _user_version(database) == 1
    assert not _has_table(database, "must_roll_back")


def test_unversioned_nonempty_and_non_sqlite_databases_are_rejected(
    tmp_path: Path,
) -> None:
    unversioned = tmp_path / "unversioned.sqlite3"
    with sqlite3.connect(unversioned) as connection:
        connection.execute("CREATE TABLE unrelated (value TEXT)")

    with pytest.raises(ValueError, match="non-empty"):
        SQLiteRunStore(unversioned)

    malformed = tmp_path / "not-a-database.sqlite3"
    malformed.write_text("not sqlite", encoding="utf-8")
    with pytest.raises(sqlite3.DatabaseError):
        SQLiteRunStore(malformed)


def test_closed_store_rejects_schema_and_history_access(tmp_path: Path) -> None:
    store = SQLiteRunStore(tmp_path / "closed.sqlite3")
    store.close()

    with pytest.raises(ValueError, match="closed"):
        _ = store.schema_version
    with pytest.raises(ValueError, match="closed"):
        store.list_runs()
