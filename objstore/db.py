"""SQLite 连接与 schema。

所有账务与元数据都在短事务内完成；数据流的读写发生在事务外，
因此一把可重入锁即可保证线程安全，又不会让大文件上传阻塞其他操作。
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    name           TEXT PRIMARY KEY,
    quota_bytes    INTEGER NOT NULL,
    used_bytes     INTEGER NOT NULL DEFAULT 0,
    reserved_bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS objects (
    account    TEXT NOT NULL,
    key        TEXT NOT NULL,
    size       INTEGER NOT NULL,
    status     TEXT NOT NULL,           -- 'active' | 'trash'
    deleted_at REAL,
    PRIMARY KEY (account, key)
);

CREATE TABLE IF NOT EXISTS blobs (
    key      TEXT PRIMARY KEY,
    size     INTEGER NOT NULL,
    refcount INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id            TEXT PRIMARY KEY,
    account       TEXT NOT NULL,
    declared_size INTEGER NOT NULL,
    status        TEXT NOT NULL,        -- 'open' | 'committing' | 'committed' | 'abandoned'
    result_key    TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS parts (
    session_id  TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    path        TEXT NOT NULL,
    PRIMARY KEY (session_id, part_number)
);

CREATE TABLE IF NOT EXISTS reservations (
    id         TEXT PRIMARY KEY,
    account    TEXT NOT NULL,
    bytes      INTEGER NOT NULL,
    session_id TEXT                      -- 一次性上传为 NULL
);
"""


class Database:
    """单连接 + 可重入锁的线程安全 SQLite 封装。"""

    def __init__(self, path: str) -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """短事务：持锁执行，退出时提交，异常时回滚。"""
        with self._lock:
            try:
                yield self._conn
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return cur.fetchall()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
