"""测绘校核分片执行的 SQLite 模式与事务辅助。"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path


SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('planner', 'reviewer', 'auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS rule_sets (
    rule_set_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    title TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (rule_set_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS survey_jobs (
    job_id TEXT PRIMARY KEY,
    rule_set_id TEXT NOT NULL,
    rule_set_version INTEGER NOT NULL,
    rule_set_sha256 TEXT NOT NULL CHECK (length(rule_set_sha256) = 64),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    manifest_json TEXT NOT NULL,
    shard_count INTEGER NOT NULL CHECK (shard_count > 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    backoff_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'cancelled', 'completed')),
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    cancelled_by TEXT,
    cancelled_at TEXT,
    cancel_reason TEXT,
    completed_at TEXT,
    FOREIGN KEY (rule_set_id, rule_set_version) REFERENCES rule_sets(rule_set_id, version)
);

CREATE TABLE IF NOT EXISTS shards (
    job_id TEXT NOT NULL REFERENCES survey_jobs(job_id),
    shard_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    slice_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('waiting', 'ready', 'leased', 'succeeded', 'manual', 'skipped')),
    ready_reason TEXT NOT NULL CHECK (ready_reason IN ('initial', 'dependency')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    last_error TEXT,
    output_json TEXT,
    output_sha256 TEXT CHECK (output_sha256 IS NULL OR length(output_sha256) = 64),
    completed_by TEXT,
    completed_at TEXT,
    manual_note TEXT,
    manual_by TEXT,
    manual_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, shard_id),
    UNIQUE (job_id, sequence)
);

CREATE TABLE IF NOT EXISTS shard_dependencies (
    job_id TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (job_id, shard_id, depends_on),
    FOREIGN KEY (job_id, shard_id) REFERENCES shards(job_id, shard_id),
    FOREIGN KEY (job_id, depends_on) REFERENCES shards(job_id, shard_id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

REQUIRED_TABLES = frozenset({
    "schema_meta", "users", "rule_sets", "survey_jobs", "shards",
    "shard_dependencies", "audit_events",
})


def connect(path: str | Path) -> sqlite3.Connection:
    """打开连接并启用严格的事务与外键设置。"""

    connection = sqlite3.connect(str(path), isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    """显式事务；异常时保证回滚。"""

    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def initialize(connection: sqlite3.Connection) -> None:
    """初始化基础资料表，重复执行不改变已有数据。"""

    connection.executescript(SCHEMA_SQL)
    with transaction(connection, immediate=True):
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )


def inspect_schema(connection: sqlite3.Connection) -> dict[str, object]:
    """返回适合机器检查的数据库结构摘要。"""

    table_rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    tables = tuple(row["name"] for row in table_rows)
    version_row = connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    missing = sorted(REQUIRED_TABLES - set(tables))
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    return {
        "tables": tables,
        "missing_tables": missing,
        "schema_version": None if version_row is None else version_row["value"],
        "foreign_keys": bool(foreign_keys),
    }
