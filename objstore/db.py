"""SQLite schema and connection helpers for the object store."""

from __future__ import annotations

import queue
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id     TEXT PRIMARY KEY,
    quota_bytes    INTEGER NOT NULL,
    used_bytes     INTEGER NOT NULL DEFAULT 0,
    reserved_bytes INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS objects (
    account_id TEXT NOT NULL,
    key        TEXT NOT NULL,
    size       INTEGER NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('active', 'trash')),
    deleted_at REAL,
    PRIMARY KEY (account_id, key)
);

CREATE TABLE IF NOT EXISTS contents (
    sha256   TEXT PRIMARY KEY,
    size     INTEGER NOT NULL,
    refcount INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id    TEXT PRIMARY KEY,
    account_id    TEXT NOT NULL,
    declared_size INTEGER NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('open', 'committing', 'committed', 'aborted')),
    reserved      INTEGER NOT NULL,
    created_at    REAL NOT NULL,
    last_part_at  REAL NOT NULL,
    committed_key TEXT
);

CREATE TABLE IF NOT EXISTS parts (
    session_id  TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    PRIMARY KEY (session_id, part_number)
);
"""


class Database:
    """A bounded pool of SQLite connections against one database file.

    The pool bounds the number of open file descriptors no matter how many
    caller threads come and go. Transactions are short, so a small pool
    never becomes a throughput bottleneck.
    """

    def __init__(self, path: str, max_connections: int = 8) -> None:
        self._path = path
        self._max = max_connections
        self._pool: "queue.Queue[sqlite3.Connection]" = queue.Queue()
        self._created = 0
        self._created_lock = threading.Lock()
        self._closed = False
        with self.connection() as conn:
            conn.executescript(SCHEMA)

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._acquire()
        try:
            yield conn
        finally:
            self._release(conn)

    def _acquire(self) -> sqlite3.Connection:
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            pass
        with self._created_lock:
            if self._created < self._max:
                self._created += 1
                try:
                    return self._new_connection()
                except BaseException:
                    self._created -= 1
                    raise
        return self._pool.get()

    def _release(self, conn: sqlite3.Connection) -> None:
        if self._closed:
            conn.close()
            with self._created_lock:
                self._created -= 1
            return
        self._pool.put(conn)

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                self._pool.get_nowait().close()
            except queue.Empty:
                break
