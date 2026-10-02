"""Object store: content-addressed uploads with per-account quota accounting."""

from __future__ import annotations

import hashlib
import os
import uuid
import threading
import time
from contextlib import contextmanager
from typing import BinaryIO, Dict, Iterator, Optional

from .db import Database
from .errors import (
    AccountNotFound,
    CommitIncomplete,
    InvalidPart,
    ObjectNotFound,
    ObjectTooLarge,
    QuotaExceeded,
    SessionClosed,
    SessionNotFound,
    SizeMismatch,
)

MAX_OBJECT_SIZE = 500 * 1024 * 1024          # 500 MB
PART_SIZE = 5 * 1024 * 1024                  # 5 MiB
DEFAULT_QUOTA = 10 * 1024 * 1024 * 1024      # 10 GB
TRASH_RETENTION_SECONDS = 7 * 24 * 3600      # 7 days
SESSION_IDLE_TIMEOUT_SECONDS = 24 * 3600     # 24 hours

_CHUNK = 1024 * 1024                         # 1 MiB streaming buffer


class ObjectStore:
    """Library entry point used by the upper-layer storage service."""

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir
        self._content_dir = os.path.join(base_dir, "content")
        self._tmp_dir = os.path.join(base_dir, "tmp")
        self._parts_dir = os.path.join(base_dir, "parts")
        for path in (self._content_dir, self._tmp_dir, self._parts_dir):
            os.makedirs(path, exist_ok=True)
        self._db = Database(os.path.join(base_dir, "meta.db"))
        # Serializes content-file lifecycle (refcount 0<->1 transitions and
        # the matching file create/delete) inside this process.
        self._content_lock = threading.Lock()
        # Per-session locks so concurrent commits of one session serialize.
        self._session_locks: Dict[str, threading.Lock] = {}
        self._session_locks_guard = threading.Lock()

    # ---- internal helpers --------------------------------------------------
    def _query(self, sql: str, params=()) -> list:
        with self._db.connection() as conn:
            return conn.execute(sql, params).fetchall()

    @contextmanager
    def _txn(self) -> Iterator:
        with self._db.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()

    def _content_path(self, sha256: str) -> str:
        return os.path.join(self._content_dir, sha256)

    # ---- account / quota ---------------------------------------------------
    def create_account(self, account_id: str, quota_bytes: int = DEFAULT_QUOTA) -> None:
        with self._txn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO accounts (account_id, quota_bytes) VALUES (?, ?)",
                (account_id, quota_bytes),
            )

    def set_quota(self, account_id: str, quota_bytes: int) -> None:
        with self._txn() as conn:
            cur = conn.execute(
                "UPDATE accounts SET quota_bytes = ? WHERE account_id = ?",
                (quota_bytes, account_id),
            )
            if cur.rowcount == 0:
                raise AccountNotFound(account_id)

    def usage(self, account_id: str) -> Dict[str, int]:
        rows = self._query(
            "SELECT quota_bytes, used_bytes, reserved_bytes FROM accounts "
            "WHERE account_id = ?",
            (account_id,),
        )
        if not rows:
            raise AccountNotFound(account_id)
        row = rows[0]
        return {"quota": row[0], "used": row[1], "reserved": row[2]}

    def _reserve(self, conn, account_id: str, size: int) -> None:
        """Reserve quota inside a transaction; raises QuotaExceeded."""
        cur = conn.execute(
            "UPDATE accounts SET reserved_bytes = reserved_bytes + ? "
            "WHERE account_id = ? AND used_bytes + reserved_bytes + ? <= quota_bytes",
            (size, account_id, size),
        )
        if cur.rowcount == 0:
            exists = conn.execute(
                "SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)
            ).fetchone()
            if exists is None:
                raise AccountNotFound(account_id)
            raise QuotaExceeded(account_id)

    def _release_reservation(self, conn, account_id: str, size: int) -> None:
        conn.execute(
            "UPDATE accounts SET reserved_bytes = reserved_bytes - ? WHERE account_id = ?",
            (size, account_id),
        )

    # ---- one-shot upload ---------------------------------------------------
    def upload(self, account_id: str, declared_size: int, stream: BinaryIO) -> str:
        if declared_size < 0 or declared_size > MAX_OBJECT_SIZE:
            raise ObjectTooLarge(declared_size)
        with self._txn() as conn:
            self._reserve(conn, account_id, declared_size)
        tmp_path = os.path.join(self._tmp_dir, uuid.uuid4().hex)
        try:
            sha256, actual = self._stream_to_file(stream, tmp_path)
            if actual != declared_size:
                raise SizeMismatch(f"declared {declared_size}, got {actual}")
        except BaseException:
            os.unlink(tmp_path) if os.path.exists(tmp_path) else None
            with self._txn() as conn:
                self._release_reservation(conn, account_id, declared_size)
            raise
        return self._finalize_upload(account_id, sha256, actual, tmp_path)

    @staticmethod
    def _stream_to_file(stream: BinaryIO, path: str):
        hasher = hashlib.sha256()
        total = 0
        with open(path, "wb") as out:
            while True:
                chunk = stream.read(_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
                out.write(chunk)
                total += len(chunk)
        return hasher.hexdigest(), total

    def _finalize_upload(self, account_id: str, sha256: str, size: int, tmp_path: str,
                         extra=None) -> str:
        """Convert the reservation into a charge (or release it on dedup)."""
        with self._content_lock:
            moved = False
            try:
                with self._txn() as conn:
                    row = conn.execute(
                        "SELECT status FROM objects WHERE account_id = ? AND key = ?",
                        (account_id, sha256),
                    ).fetchone()
                    if row is not None:
                        # Already held by this account: dedup, release reservation.
                        if row[0] == "trash":
                            conn.execute(
                                "UPDATE objects SET status = 'active', deleted_at = NULL "
                                "WHERE account_id = ? AND key = ?",
                                (account_id, sha256),
                            )
                        self._release_reservation(conn, account_id, size)
                        if extra is not None:
                            extra(conn, sha256)
                        return sha256
                    conn.execute(
                        "INSERT INTO objects (account_id, key, size, status, deleted_at) "
                        "VALUES (?, ?, ?, 'active', NULL)",
                        (account_id, sha256, size),
                    )
                    conn.execute(
                        "UPDATE accounts SET used_bytes = used_bytes + ?, "
                        "reserved_bytes = reserved_bytes - ? WHERE account_id = ?",
                        (size, size, account_id),
                    )
                    content = conn.execute(
                        "SELECT refcount FROM contents WHERE sha256 = ?", (sha256,)
                    ).fetchone()
                    if content is None:
                        conn.execute(
                            "INSERT INTO contents (sha256, size, refcount) VALUES (?, ?, 1)",
                            (sha256, size),
                        )
                        os.replace(tmp_path, self._content_path(sha256))
                        moved = True
                    else:
                        conn.execute(
                            "UPDATE contents SET refcount = refcount + 1 WHERE sha256 = ?",
                            (sha256,),
                        )
                    if extra is not None:
                        extra(conn, sha256)
                return sha256
            except BaseException:
                if moved:
                    try:
                        os.unlink(self._content_path(sha256))
                    except OSError:
                        pass
                raise
            finally:
                if not moved and os.path.exists(tmp_path):
                    os.unlink(tmp_path)

    # ---- multipart upload --------------------------------------------------
    def create_session(self, account_id: str, declared_size: int) -> str:
        if declared_size < 0 or declared_size > MAX_OBJECT_SIZE:
            raise ObjectTooLarge(declared_size)
        session_id = uuid.uuid4().hex
        now = time.time()
        with self._txn() as conn:
            self._reserve(conn, account_id, declared_size)
            conn.execute(
                "INSERT INTO sessions (session_id, account_id, declared_size, status, "
                "reserved, created_at, last_part_at) VALUES (?, ?, ?, 'open', ?, ?, ?)",
                (session_id, account_id, declared_size, declared_size, now, now),
            )
        os.makedirs(self._session_parts_dir(session_id), exist_ok=True)
        return session_id

    def _session_parts_dir(self, session_id: str) -> str:
        return os.path.join(self._parts_dir, session_id)

    def _session_lock(self, session_id: str) -> threading.Lock:
        with self._session_locks_guard:
            lock = self._session_locks.get(session_id)
            if lock is None:
                lock = threading.Lock()
                self._session_locks[session_id] = lock
            return lock

    def put_part(self, session_id: str, part_number: int, stream: BinaryIO) -> None:
        if part_number < 1:
            raise InvalidPart(part_number)
        tmp_path = os.path.join(self._tmp_dir, uuid.uuid4().hex)
        _, size = self._stream_to_file(stream, tmp_path)
        if size < 1 or size > PART_SIZE:
            os.unlink(tmp_path)
            raise InvalidPart(f"part size {size} out of range")
        try:
            with self._txn() as conn:
                row = conn.execute(
                    "SELECT status FROM sessions WHERE session_id = ?", (session_id,)
                ).fetchone()
                if row is None:
                    raise SessionNotFound(session_id)
                if row[0] != "open":
                    raise SessionClosed(session_id)
                parts_dir = self._session_parts_dir(session_id)
                os.makedirs(parts_dir, exist_ok=True)
                os.replace(tmp_path, os.path.join(parts_dir, str(part_number)))
                conn.execute(
                    "INSERT INTO parts (session_id, part_number, size) VALUES (?, ?, ?) "
                    "ON CONFLICT (session_id, part_number) DO UPDATE SET size = excluded.size",
                    (session_id, part_number, size),
                )
                conn.execute(
                    "UPDATE sessions SET last_part_at = ? WHERE session_id = ?",
                    (time.time(), session_id),
                )
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def commit_session(self, session_id: str) -> str:
        with self._session_lock(session_id):
            return self._commit_session_locked(session_id)

    def _commit_session_locked(self, session_id: str) -> str:
        with self._txn() as conn:
            row = conn.execute(
                "SELECT account_id, declared_size, status, committed_key "
                "FROM sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                raise SessionNotFound(session_id)
            account_id, declared_size, status, committed_key = row
            if status == "committed":
                return committed_key
            if status != "open":
                raise SessionClosed(session_id)
            conn.execute(
                "UPDATE sessions SET status = 'committing' WHERE session_id = ?",
                (session_id,),
            )
            parts = conn.execute(
                "SELECT part_number, size FROM parts WHERE session_id = ? "
                "ORDER BY part_number",
                (session_id,),
            ).fetchall()
        try:
            self._validate_parts(parts, declared_size)
            tmp_path = os.path.join(self._tmp_dir, uuid.uuid4().hex)
            try:
                sha256, actual = self._concat_parts(session_id, parts, tmp_path)
                if actual != declared_size:
                    raise CommitIncomplete(
                        f"total {actual} != declared {declared_size}")
            except BaseException:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except BaseException:
            with self._txn() as conn:
                conn.execute(
                    "UPDATE sessions SET status = 'open' WHERE session_id = ? "
                    "AND status = 'committing'",
                    (session_id,),
                )
            raise

        def _mark_committed(conn, key):
            conn.execute(
                "UPDATE sessions SET status = 'committed', committed_key = ?, "
                "reserved = 0 WHERE session_id = ?",
                (key, session_id),
            )
            conn.execute("DELETE FROM parts WHERE session_id = ?", (session_id,))

        try:
            key = self._finalize_upload(account_id, sha256, declared_size, tmp_path,
                                        extra=_mark_committed)
        except BaseException:
            with self._txn() as conn:
                conn.execute(
                    "UPDATE sessions SET status = 'open' WHERE session_id = ? "
                    "AND status = 'committing'",
                    (session_id,),
                )
            raise
        self._remove_part_files(session_id, [p[0] for p in parts])
        return key

    @staticmethod
    def _validate_parts(parts, declared_size: int) -> None:
        expected = declared_size // PART_SIZE + (1 if declared_size % PART_SIZE else 0)
        if declared_size == 0:
            expected = 0
        if len(parts) != expected:
            raise CommitIncomplete(
                f"have {len(parts)} parts, need {expected}")
        for index, (number, size) in enumerate(parts, start=1):
            if number != index:
                raise CommitIncomplete(f"missing part {index}")
            if index < expected and size != PART_SIZE:
                raise CommitIncomplete(f"part {index} size {size} != {PART_SIZE}")
        if expected and not (1 <= parts[-1][1] <= PART_SIZE):
            raise CommitIncomplete("last part size out of range")

    def _concat_parts(self, session_id: str, parts, tmp_path: str):
        hasher = hashlib.sha256()
        total = 0
        parts_dir = self._session_parts_dir(session_id)
        with open(tmp_path, "wb") as out:
            for number, _size in parts:
                with open(os.path.join(parts_dir, str(number)), "rb") as part_file:
                    while True:
                        chunk = part_file.read(_CHUNK)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        out.write(chunk)
                        total += len(chunk)
        return hasher.hexdigest(), total

    def _remove_part_files(self, session_id: str, part_numbers) -> None:
        parts_dir = self._session_parts_dir(session_id)
        for number in part_numbers:
            try:
                os.unlink(os.path.join(parts_dir, str(number)))
            except OSError:
                pass
        try:
            os.rmdir(parts_dir)
        except OSError:
            pass

    # ---- object lifecycle --------------------------------------------------
    def read(self, account_id: str, key: str) -> BinaryIO:
        rows = self._query(
            "SELECT status FROM objects WHERE account_id = ? AND key = ?",
            (account_id, key),
        )
        if not rows:
            raise ObjectNotFound(key)
        return open(self._content_path(key), "rb")

    def delete(self, account_id: str, key: str) -> None:
        with self._txn() as conn:
            cur = conn.execute(
                "UPDATE objects SET status = 'trash', deleted_at = ? "
                "WHERE account_id = ? AND key = ? AND status = 'active'",
                (time.time(), account_id, key),
            )
            if cur.rowcount == 0:
                raise ObjectNotFound(key)

    def restore(self, account_id: str, key: str) -> None:
        with self._txn() as conn:
            cur = conn.execute(
                "UPDATE objects SET status = 'active', deleted_at = NULL "
                "WHERE account_id = ? AND key = ? AND status = 'trash'",
                (account_id, key),
            )
            if cur.rowcount == 0:
                raise ObjectNotFound(key)

    def cleanup(self, now: Optional[float] = None) -> None:
        """Purge expired trash and abort idle multipart sessions."""
        if now is None:
            now = time.time()
        self._purge_trash(now)
        self._abort_idle_sessions(now)

    def _purge_trash(self, now: float) -> None:
        cutoff = now - TRASH_RETENTION_SECONDS
        with self._content_lock:
            with self._txn() as conn:
                rows = conn.execute(
                    "SELECT account_id, key, size FROM objects "
                    "WHERE status = 'trash' AND deleted_at <= ?",
                    (cutoff,),
                ).fetchall()
                zero_ref = []
                for account_id, key, size in rows:
                    conn.execute(
                        "DELETE FROM objects WHERE account_id = ? AND key = ?",
                        (account_id, key),
                    )
                    conn.execute(
                        "UPDATE accounts SET used_bytes = used_bytes - ? "
                        "WHERE account_id = ?",
                        (size, account_id),
                    )
                    conn.execute(
                        "UPDATE contents SET refcount = refcount - 1 WHERE sha256 = ?",
                        (key,),
                    )
                    refcount = conn.execute(
                        "SELECT refcount FROM contents WHERE sha256 = ?", (key,)
                    ).fetchone()[0]
                    if refcount == 0:
                        conn.execute(
                            "DELETE FROM contents WHERE sha256 = ?", (key,))
                        zero_ref.append(key)
            for key in zero_ref:
                try:
                    os.unlink(self._content_path(key))
                except OSError:
                    pass

    def _abort_idle_sessions(self, now: float) -> None:
        cutoff = now - SESSION_IDLE_TIMEOUT_SECONDS
        with self._txn() as conn:
            rows = conn.execute(
                "SELECT session_id, account_id, reserved FROM sessions "
                "WHERE status = 'open' AND last_part_at <= ?",
                (cutoff,),
            ).fetchall()
            for session_id, account_id, reserved in rows:
                conn.execute(
                    "UPDATE sessions SET status = 'aborted', reserved = 0 "
                    "WHERE session_id = ?",
                    (session_id,),
                )
                self._release_reservation(conn, account_id, reserved)
                conn.execute(
                    "DELETE FROM parts WHERE session_id = ?", (session_id,))
        for session_id, _account_id, _reserved in rows:
            parts_dir = self._session_parts_dir(session_id)
            if os.path.isdir(parts_dir):
                for name in os.listdir(parts_dir):
                    try:
                        os.unlink(os.path.join(parts_dir, name))
                    except OSError:
                        pass
                try:
                    os.rmdir(parts_dir)
                except OSError:
                    pass

    def close(self) -> None:
        self._db.close()
