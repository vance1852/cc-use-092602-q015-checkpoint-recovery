"""测绘校核服务的 SQLite 模式与事务辅助。"""

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

CREATE TABLE IF NOT EXISTS survey_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('coordinator', 'reviewer', 'auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS survey_jobs (
    job_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'completed', 'cancelled')),
    rule_version TEXT NOT NULL,
    rule_json TEXT NOT NULL,
    rule_sha256 TEXT NOT NULL CHECK (length(rule_sha256) = 64),
    rule_revision INTEGER NOT NULL DEFAULT 1 CHECK (rule_revision > 0),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    input_revision INTEGER NOT NULL DEFAULT 1 CHECK (input_revision > 0),
    parcel_count INTEGER NOT NULL CHECK (parcel_count > 0),
    lease_seconds INTEGER NOT NULL CHECK (lease_seconds > 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    retry_delay_seconds INTEGER NOT NULL CHECK (retry_delay_seconds >= 0),
    created_by TEXT NOT NULL REFERENCES survey_users(user_id),
    created_at TEXT NOT NULL,
    cancelled_by TEXT REFERENCES survey_users(user_id),
    cancelled_at TEXT,
    cancel_reason TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS survey_parcels (
    job_id TEXT NOT NULL REFERENCES survey_jobs(job_id),
    parcel_id TEXT NOT NULL,
    zone TEXT NOT NULL,
    declared_area_mu TEXT NOT NULL,
    declared_boundary_json TEXT NOT NULL,
    surveyed_boundary_json TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    updated_by TEXT NOT NULL REFERENCES survey_users(user_id),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job_id, parcel_id)
);

CREATE TABLE IF NOT EXISTS survey_shards (
    shard_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES survey_jobs(job_id),
    shard_key TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('boundary-compare', 'area-stats', 'summary')),
    zone TEXT,
    state TEXT NOT NULL CHECK (state IN ('blocked', 'ready', 'leased', 'succeeded', 'needs_review', 'skipped')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    fencing_token INTEGER NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    claimed_input_sha256 TEXT CHECK (claimed_input_sha256 IS NULL OR length(claimed_input_sha256) = 64),
    output_sha256 TEXT CHECK (output_sha256 IS NULL OR length(output_sha256) = 64),
    result_json TEXT,
    last_error TEXT,
    completed_by TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (job_id, shard_key)
);

CREATE INDEX IF NOT EXISTS idx_shards_claimable
ON survey_shards(state, available_at);

CREATE TABLE IF NOT EXISTS survey_shard_deps (
    job_id TEXT NOT NULL,
    shard_key TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (job_id, shard_key, depends_on),
    FOREIGN KEY (job_id, shard_key) REFERENCES survey_shards(job_id, shard_key),
    FOREIGN KEY (job_id, depends_on) REFERENCES survey_shards(job_id, shard_key)
);

CREATE TABLE IF NOT EXISTS survey_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_survey_audit_entity
ON survey_audit_events(entity_type, entity_id, event_id);
"""

REQUIRED_TABLES = frozenset({
    "schema_meta", "survey_users", "survey_jobs", "survey_parcels",
    "survey_shards", "survey_shard_deps", "survey_audit_events",
})


def connect(path: str | Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    """打开连接并启用严格的事务与外键设置。

    HTTP 服务以多线程处理请求时传入 check_same_thread=False，
    由 api 层的调度锁保证同一时刻只有一个线程使用该连接。
    """

    connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=check_same_thread)
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
