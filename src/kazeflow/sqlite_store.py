"""Explicit SQLite storage for portable :class:`~kazeflow.results.RunResult` records.

This adapter is deliberately outside the core import graph.  It persists the lossy
``RunResult.to_record()`` projection, not arbitrary task outputs, exceptions, or
partition-key values.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Callable

from .results import FlowStatus, RunResult


CURRENT_SCHEMA_VERSION = 1
_TERMINAL_FLOW_STATUSES = frozenset(
    {FlowStatus.SUCCESS.value, FlowStatus.FAILED.value, FlowStatus.CANCELLED.value}
)
_PORTABLE_RUN_RECORD_KEYS = frozenset(
    {"run_id", "status", "started_at", "ended_at", "duration_seconds", "tasks"}
)
_TRANSACTION_CONTROL_KEYWORDS = frozenset(
    {"BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"}
)


@dataclass(frozen=True, slots=True)
class StoredRunSummary:
    """Deterministic metadata for a run listed from :class:`SQLiteRunStore`."""

    run_id: str
    schema_version: int
    status: str
    saved_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("run_id must be a non-empty string")
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version <= 0
        ):
            raise ValueError("schema_version must be a positive integer")
        if (
            not isinstance(self.status, str)
            or self.status not in _TERMINAL_FLOW_STATUSES
        ):
            raise ValueError("status must be a terminal flow status string")
        _validate_utc_timestamp(self.saved_at, "saved_at")


@dataclass(frozen=True, slots=True)
class StoredRunRecord(StoredRunSummary):
    """An immutable envelope around a freshly decoded portable run record."""

    record_json: str

    def __post_init__(self) -> None:
        StoredRunSummary.__post_init__(self)
        _decode_portable_run_record(self.record_json, self.run_id, self.status)

    @property
    def record(self) -> dict[str, object]:
        """Return a new decoded copy of the portable, deliberately lossy record."""
        return _decode_portable_run_record(self.record_json, self.run_id, self.status)


class SQLiteRunStore:
    """A caller-owned SQLite database of versioned portable run records.

    A new or empty database initializes to schema version 1.  A non-empty database
    with ``PRAGMA user_version = 0`` is rejected as unversioned; a malformed or
    non-SQLite file raises the underlying :class:`sqlite3.DatabaseError` before any
    record is written.  Future-version databases are rejected before mutation.
    """

    _MIGRATIONS: dict[int, tuple[str, ...]] = {}

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._connection = sqlite3.connect(self._path)
        try:
            self._initialize_or_migrate()
        except BaseException:
            self._connection.close()
            raise

    def __enter__(self) -> SQLiteRunStore:
        self._ensure_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the caller-owned database connection; this operation is idempotent."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def save(self, result: RunResult) -> StoredRunRecord:
        """Persist one completed result without retaining in-memory-only values."""
        self._ensure_open()
        if not isinstance(result, RunResult):
            raise TypeError("result must be a RunResult")

        record = result.to_record()
        record_json = json.dumps(
            record,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        saved_at = datetime.now(timezone.utc)
        try:
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO runs (run_id, schema_version, status, saved_at, record_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        result.run_id,
                        CURRENT_SCHEMA_VERSION,
                        result.status.value,
                        saved_at.isoformat(),
                        record_json,
                    ),
                )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"run id already exists: {result.run_id}") from error

        return StoredRunRecord(
            run_id=result.run_id,
            schema_version=CURRENT_SCHEMA_VERSION,
            status=result.status.value,
            saved_at=saved_at,
            record_json=record_json,
        )

    def load(self, run_id: str) -> StoredRunRecord:
        """Load a persistent portable record without rebuilding a ``RunResult``."""
        self._ensure_open()
        self._validate_run_id(run_id)
        row = self._connection.execute(
            """
            SELECT run_id, schema_version, status, saved_at, record_json
            FROM runs WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return self._record_from_row(row)

    def list_runs(self, *, limit: int | None = None) -> tuple[StoredRunSummary, ...]:
        """List run metadata in deterministic saved-time then run-id order."""
        self._ensure_open()
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit < 0
        ):
            raise ValueError("limit must be a non-negative integer or None")

        query = """
            SELECT run_id, schema_version, status, saved_at
            FROM runs ORDER BY saved_at ASC, run_id ASC
        """
        params: tuple[object, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        rows = self._connection.execute(query, params).fetchall()
        return tuple(self._summary_from_row(row) for row in rows)

    @property
    def schema_version(self) -> int:
        """Return the current database schema version."""
        self._ensure_open()
        return self._read_schema_version()

    def _initialize_or_migrate(self) -> None:
        version = self._read_schema_version()
        if version > CURRENT_SCHEMA_VERSION:
            raise ValueError(
                "database schema version "
                f"{version} is newer than supported version {CURRENT_SCHEMA_VERSION}"
            )
        if version == CURRENT_SCHEMA_VERSION:
            return
        if version == 0:
            if self._has_user_tables():
                raise ValueError(
                    "database is non-empty but has no kazeflow schema version"
                )

            def initialize() -> None:
                self._connection.execute(
                    """
                    CREATE TABLE runs (
                        run_id TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        saved_at TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    )
                    """
                )
                self._connection.execute(
                    "CREATE INDEX runs_saved_at_run_id ON runs (saved_at, run_id)"
                )
                self._set_schema_version(CURRENT_SCHEMA_VERSION)

            self._run_transaction(initialize)
            return

        while version < CURRENT_SCHEMA_VERSION:
            steps = self._MIGRATIONS.get(version)
            if steps is None:
                raise ValueError(
                    f"no supported migration from schema version {version}"
                )
            self._validate_migration_steps(steps)

            def migrate_one() -> None:
                for statement in steps:
                    self._connection.execute(statement)
                self._set_schema_version(version + 1)

            self._run_transaction(migrate_one)
            version += 1

    def _run_transaction(self, action: Callable[[], None]) -> None:
        """Run one schema transition atomically, including DDL."""
        self._connection.execute("BEGIN")
        try:
            action()
        except BaseException:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

    @staticmethod
    def _validate_migration_steps(steps: tuple[str, ...]) -> None:
        if not isinstance(steps, tuple) or not steps:
            raise ValueError("a migration must be a non-empty tuple of SQL steps")
        for statement in steps:
            if not isinstance(statement, str) or not statement.strip():
                raise ValueError("a migration SQL step must be a non-empty string")
            keyword = _first_sql_keyword(statement)
            if keyword in _TRANSACTION_CONTROL_KEYWORDS:
                raise ValueError(
                    f"migration SQL steps must not control transactions: {keyword}"
                )

    def _read_schema_version(self) -> int:
        row = self._connection.execute("PRAGMA user_version").fetchone()
        if row is None or not isinstance(row[0], int) or row[0] < 0:
            raise ValueError("database schema version must be a non-negative integer")
        return row[0]

    def _set_schema_version(self, version: int) -> None:
        self._connection.execute(f"PRAGMA user_version = {version}")

    def _has_user_tables(self) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
        ).fetchone()
        return row is not None

    def _record_from_row(self, row: tuple[object, ...]) -> StoredRunRecord:
        run_id, schema_version, status, saved_at, record_json = row
        if (
            not isinstance(run_id, str)
            or not isinstance(schema_version, int)
            or not isinstance(status, str)
            or not isinstance(record_json, str)
        ):
            raise ValueError("stored run row has invalid types")
        return StoredRunRecord(
            run_id=run_id,
            schema_version=schema_version,
            status=status,
            saved_at=self._parse_saved_at(saved_at),
            record_json=record_json,
        )

    def _summary_from_row(self, row: tuple[object, ...]) -> StoredRunSummary:
        run_id, schema_version, status, saved_at = row
        if (
            not isinstance(run_id, str)
            or not isinstance(schema_version, int)
            or not isinstance(status, str)
        ):
            raise ValueError("stored run summary row has invalid types")
        return StoredRunSummary(
            run_id=run_id,
            schema_version=schema_version,
            status=status,
            saved_at=self._parse_saved_at(saved_at),
        )

    @staticmethod
    def _parse_saved_at(value: object) -> datetime:
        if not isinstance(value, str):
            raise ValueError("stored saved_at must be an ISO timestamp")
        parsed = datetime.fromisoformat(value)
        _validate_utc_timestamp(parsed, "stored saved_at")
        return parsed

    @staticmethod
    def _validate_run_id(run_id: str) -> None:
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")

    def _ensure_open(self) -> None:
        if self._connection is None:
            raise ValueError("SQLiteRunStore is closed")


def _validate_utc_timestamp(value: object, field_name: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"{field_name} must be an aware UTC datetime")


def _decode_portable_run_record(
    record_json: str, run_id: str, status: str
) -> dict[str, object]:
    if not isinstance(record_json, str):
        raise ValueError("record_json must be a JSON object string")
    try:
        decoded = json.loads(record_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError("record_json must contain valid JSON") from error
    if not isinstance(decoded, dict) or set(decoded) != _PORTABLE_RUN_RECORD_KEYS:
        raise ValueError("stored run record has invalid top-level keys")
    if decoded["run_id"] != run_id or decoded["status"] != status:
        raise ValueError("stored run record does not match its envelope")
    if not isinstance(decoded["tasks"], list):
        raise ValueError("stored run record tasks must be a list")
    return decoded


def _first_sql_keyword(statement: str) -> str:
    """Return the first SQL keyword after leading whitespace, comments, and semis."""
    remaining = statement.lstrip()
    while remaining:
        if remaining.startswith("--"):
            newline = remaining.find("\n")
            remaining = "" if newline < 0 else remaining[newline + 1 :].lstrip()
        elif remaining.startswith("/*"):
            end = remaining.find("*/", 2)
            if end < 0:
                return ""
            remaining = remaining[end + 2 :].lstrip()
        elif remaining.startswith(";"):
            remaining = remaining[1:].lstrip()
        else:
            break
    if not remaining:
        return ""
    return remaining.split(None, 1)[0].rstrip(";").upper()
