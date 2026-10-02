"""对象存储与额度核算主模块。

设计要点：
- 元数据与账务全部在 SQLite 短事务内完成，数据流读写发生在事务外，
  因此大文件上传不会阻塞同账户的其他操作，内存占用与对象大小无关。
- 账户的 used_bytes / reserved_bytes 是随每次账务操作事务性维护的计数器，
  查询用量为 O(1)，与对象数量无关。
- 预占以 reservations 行的存在为准，释放即删行，保证不重复释放、不遗漏。
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import BinaryIO, Callable

from .db import Database
from .errors import (
    InvalidPart,
    ObjectNotFound,
    ObjectTooLarge,
    QuotaExceeded,
    SessionNotFound,
    SessionNotOpen,
    SizeMismatch,
    UploadIncomplete,
)

DEFAULT_QUOTA_BYTES = 10 * 1024**3
MAX_OBJECT_BYTES = 500 * 1024**2
PART_SIZE = 5 * 1024**2
TRASH_RETENTION_SECONDS = 7 * 24 * 3600
SESSION_IDLE_SECONDS = 24 * 3600

_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class Usage:
    used_bytes: int
    reserved_bytes: int
    quota_bytes: int


class ObjectStore:
    def __init__(
        self,
        db_path: str,
        data_dir: str,
        *,
        part_size: int = PART_SIZE,
        max_object_bytes: int = MAX_OBJECT_BYTES,
        default_quota_bytes: int = DEFAULT_QUOTA_BYTES,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self._db = Database(db_path)
        self._blob_dir = os.path.join(data_dir, "blobs")
        self._tmp_dir = os.path.join(data_dir, "tmp")
        os.makedirs(self._blob_dir, exist_ok=True)
        os.makedirs(self._tmp_dir, exist_ok=True)
        self._part_size = part_size
        self._max_object_bytes = max_object_bytes
        self._default_quota_bytes = default_quota_bytes
        self._time_fn = time_fn
        self._session_locks: dict[str, threading.Lock] = {}
        self._session_locks_guard = threading.Lock()

    def _session_lock(self, session_id: str) -> threading.Lock:
        """每个会话一把锁，保证同一会话的并发提交只生效一次。"""
        with self._session_locks_guard:
            lock = self._session_locks.get(session_id)
            if lock is None:
                lock = threading.Lock()
                self._session_locks[session_id] = lock
            return lock

    # ------------------------------------------------------------------
    # 账户与额度
    # ------------------------------------------------------------------

    def create_account(self, account: str, quota_bytes: int | None = None) -> None:
        quota = self._default_quota_bytes if quota_bytes is None else quota_bytes
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO accounts(name, quota_bytes) VALUES (?, ?)",
                (account, quota),
            )

    def set_quota(self, account: str, quota_bytes: int) -> None:
        self._ensure_account(account)
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE accounts SET quota_bytes = ? WHERE name = ?",
                (quota_bytes, account),
            )

    def usage(self, account: str) -> Usage:
        self._ensure_account(account)
        rows = self._db.query(
            "SELECT used_bytes, reserved_bytes, quota_bytes FROM accounts WHERE name = ?",
            (account,),
        )
        used, reserved, quota = rows[0]
        return Usage(used_bytes=used, reserved_bytes=reserved, quota_bytes=quota)

    def _ensure_account(self, account: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO accounts(name, quota_bytes) VALUES (?, ?)",
                (account, self._default_quota_bytes),
            )

    def _check_declared_size(self, declared_size: int) -> None:
        if declared_size < 0:
            raise ValueError("declared_size must be >= 0")
        if declared_size > self._max_object_bytes:
            raise ObjectTooLarge(
                f"declared size {declared_size} exceeds limit {self._max_object_bytes}"
            )

    def _reserve(self, account: str, size: int, session_id: str | None) -> str:
        """预占额度：同事务内检查 已用+预占+声明 <= 额度 并记账。"""
        self._check_declared_size(size)
        reservation_id = uuid.uuid4().hex
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO accounts(name, quota_bytes) VALUES (?, ?)",
                (account, self._default_quota_bytes),
            )
            row = conn.execute(
                "SELECT used_bytes, reserved_bytes, quota_bytes FROM accounts WHERE name = ?",
                (account,),
            ).fetchone()
            used, reserved, quota = row
            if used + reserved + size > quota:
                raise QuotaExceeded(
                    f"account {account!r}: used {used} + reserved {reserved} "
                    f"+ declared {size} exceeds quota {quota}"
                )
            conn.execute(
                "UPDATE accounts SET reserved_bytes = reserved_bytes + ? WHERE name = ?",
                (size, account),
            )
            conn.execute(
                "INSERT INTO reservations(id, account, bytes, session_id) VALUES (?, ?, ?, ?)",
                (reservation_id, account, size, session_id),
            )
        return reservation_id

    def _release_reservation(self, reservation_id: str) -> None:
        """释放预占。以删行成功为准，重复释放是安全的空操作。"""
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT account, bytes FROM reservations WHERE id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                return
            account, size = row
            conn.execute("DELETE FROM reservations WHERE id = ?", (reservation_id,))
            conn.execute(
                "UPDATE accounts SET reserved_bytes = reserved_bytes - ? WHERE name = ?",
                (size, account),
            )

    # ------------------------------------------------------------------
    # 一次性上传
    # ------------------------------------------------------------------

    def put(self, account: str, declared_size: int, stream: BinaryIO) -> str:
        reservation_id = self._reserve(account, declared_size, None)
        tmp_path = self._tmp_path()
        try:
            key, actual_size = self._stream_to_file(stream, tmp_path)
            if actual_size != declared_size:
                raise SizeMismatch(
                    f"declared {declared_size} bytes but received {actual_size}"
                )
        except BaseException:
            self._release_reservation(reservation_id)
            self._remove_file(tmp_path)
            raise
        try:
            return self._finalize_upload(
                account, key, actual_size, tmp_path, reservation_id
            )
        except BaseException:
            self._release_reservation(reservation_id)
            self._remove_file(tmp_path)
            raise

    def open_reader(self, account: str, key: str) -> BinaryIO:
        rows = self._db.query(
            "SELECT status FROM objects WHERE account = ? AND key = ?",
            (account, key),
        )
        if not rows:
            raise ObjectNotFound(f"no object {key} for account {account!r}")
        return open(self._blob_path(key), "rb")

    # ------------------------------------------------------------------
    # 上传落盘的内部 helpers
    # ------------------------------------------------------------------

    def _blob_path(self, key: str) -> str:
        return os.path.join(self._blob_dir, key)

    def _tmp_path(self) -> str:
        return os.path.join(self._tmp_dir, uuid.uuid4().hex)

    @staticmethod
    def _remove_file(path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def _stream_to_file(self, stream: BinaryIO, path: str) -> tuple[str, int]:
        """把数据流写入临时文件，返回 (sha256, 字节数)。分块读写，内存恒定。"""
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

    def _finalize_upload(
        self,
        account: str,
        key: str,
        size: int,
        tmp_path: str,
        reservation_id: str | None,
        session_id: str | None = None,
    ) -> str:
        """上传成功后的账务落盘（单个短事务）：

        - 本账户已有正常对象：返回原键，预占全释放，不加费；
        - 本账户回收站里有同内容：恢复为正常，预占全释放，不加费；
        - 否则新建对象，预占转计费，blob 引用计数 +1（磁盘只存一份）。
        """
        with self._db.transaction() as conn:
            part_files = []
            if session_id is not None:
                part_files = [
                    r[0]
                    for r in conn.execute(
                        "SELECT path FROM parts WHERE session_id = ?",
                        (session_id,),
                    ).fetchall()
                ]
                conn.execute(
                    "DELETE FROM parts WHERE session_id = ?", (session_id,)
                )
                conn.execute(
                    "UPDATE sessions SET status = 'committed', result_key = ? "
                    "WHERE id = ?",
                    (key, session_id),
                )
            row = conn.execute(
                "SELECT status FROM objects WHERE account = ? AND key = ?",
                (account, key),
            ).fetchone()
            # 释放预占（行可能已被并发清理删掉，删行失败则不动账务）
            res = conn.execute(
                "SELECT bytes FROM reservations WHERE id = ?", (reservation_id,)
            ).fetchone()
            if res is not None:
                conn.execute(
                    "DELETE FROM reservations WHERE id = ?", (reservation_id,)
                )
                conn.execute(
                    "UPDATE accounts SET reserved_bytes = reserved_bytes - ? "
                    "WHERE name = ?",
                    (res[0], account),
                )
            if row is not None:
                if row[0] == "trash":
                    conn.execute(
                        "UPDATE objects SET status = 'active', deleted_at = NULL "
                        "WHERE account = ? AND key = ?",
                        (account, key),
                    )
                self._remove_file(tmp_path)
                for path in part_files:
                    self._remove_file(path)
                return key
            conn.execute(
                "INSERT INTO objects(account, key, size, status) "
                "VALUES (?, ?, ?, 'active')",
                (account, key, size),
            )
            conn.execute(
                "INSERT INTO blobs(key, size, refcount) VALUES (?, ?, 1) "
                "ON CONFLICT(key) DO UPDATE SET refcount = refcount + 1",
                (key, size),
            )
            conn.execute(
                "UPDATE accounts SET used_bytes = used_bytes + ? WHERE name = ?",
                (size, account),
            )
            blob_path = self._blob_path(key)
            if os.path.exists(blob_path):
                self._remove_file(tmp_path)
            else:
                os.replace(tmp_path, blob_path)
            for path in part_files:
                self._remove_file(path)
            return key
    # ------------------------------------------------------------------
    # 分段上传
    # ------------------------------------------------------------------

    def begin_upload(self, account: str, declared_size: int) -> str:
        """开分段会话并按声明大小预占额度，返回会话 id。"""
        session_id = uuid.uuid4().hex
        self._reserve(account, declared_size, session_id)
        now = self._time_fn()
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    "INSERT INTO sessions(id, account, declared_size, status, "
                    "created_at, updated_at) VALUES (?, ?, ?, 'open', ?, ?)",
                    (session_id, account, declared_size, now, now),
                )
        except BaseException:
            self._release_reservation_by_session(session_id)
            raise
        return session_id

    def upload_part(
        self, session_id: str, part_number: int, stream: BinaryIO
    ) -> None:
        """传一个段。段可乱序、并行、重复传，同一段以最后一次成功的为准。"""
        rows = self._db.query(
            "SELECT account, declared_size, status FROM sessions WHERE id = ?",
            (session_id,),
        )
        if not rows:
            raise SessionNotFound(session_id)
        _, declared_size, status = rows[0]
        if status != "open":
            raise SessionNotOpen(f"session {session_id} is {status}")
        num_parts = self._num_parts(declared_size)
        if part_number < 1 or part_number > num_parts:
            raise InvalidPart(
                f"part {part_number} out of range 1..{num_parts}"
            )
        tmp_path = self._tmp_path()
        try:
            _, size = self._stream_to_file(stream, tmp_path)
        except BaseException:
            self._remove_file(tmp_path)
            raise
        now = self._time_fn()
        try:
            with self._db.transaction() as conn:
                cur = conn.execute(
                    "UPDATE sessions SET updated_at = ? "
                    "WHERE id = ? AND status = 'open'",
                    (now, session_id),
                )
                if cur.rowcount == 0:
                    raise SessionNotOpen(f"session {session_id} is not open")
                old = conn.execute(
                    "SELECT path FROM parts WHERE session_id = ? AND part_number = ?",
                    (session_id, part_number),
                ).fetchone()
                if old is not None:
                    self._remove_file(old[0])
                    conn.execute(
                        "UPDATE parts SET size = ?, path = ? "
                        "WHERE session_id = ? AND part_number = ?",
                        (size, tmp_path, session_id, part_number),
                    )
                else:
                    conn.execute(
                        "INSERT INTO parts(session_id, part_number, size, path) "
                        "VALUES (?, ?, ?, ?)",
                        (session_id, part_number, size, tmp_path),
                    )
        except BaseException:
            self._remove_file(tmp_path)
            raise

    def commit_upload(self, session_id: str) -> str:
        """提交会话：校验段齐且总大小相符，拼段算键，预占转计费。

        段不齐或大小不符时抛 UploadIncomplete，会话保留可再提交；
        同一会话重复/并发提交只产生一个对象、只计一次费。
        """
        with self._session_lock(session_id):
            rows = self._db.query(
                "SELECT account, declared_size, status, result_key "
                "FROM sessions WHERE id = ?",
                (session_id,),
            )
            if not rows:
                raise SessionNotFound(session_id)
            account, declared_size, status, result_key = rows[0]
            if status == "committed":
                return result_key
            if status != "open":
                raise SessionNotOpen(f"session {session_id} is {status}")
            with self._db.transaction() as conn:
                cur = conn.execute(
                    "UPDATE sessions SET status = 'committing' "
                    "WHERE id = ? AND status = 'open'",
                    (session_id,),
                )
                if cur.rowcount == 0:
                    raise SessionNotOpen(
                        f"session {session_id} was concurrently closed"
                    )
            tmp_path = self._tmp_path()
            try:
                key = self._assemble(session_id, declared_size, tmp_path)
            except BaseException:
                with self._db.transaction() as conn:
                    conn.execute(
                        "UPDATE sessions SET status = 'open' "
                        "WHERE id = ? AND status = 'committing'",
                        (session_id,),
                    )
                self._remove_file(tmp_path)
                raise
            res = self._db.query(
                "SELECT id FROM reservations WHERE session_id = ?",
                (session_id,),
            )
            reservation_id = res[0][0] if res else None
            return self._finalize_upload(
                account, key, declared_size, tmp_path, reservation_id,
                session_id=session_id,
            )

    def _num_parts(self, declared_size: int) -> int:
        if declared_size <= 0:
            return 0
        return (declared_size + self._part_size - 1) // self._part_size

    def _assemble(self, session_id: str, declared_size: int, tmp_path: str) -> str:
        """校验段齐全、尺寸合规，按序拼接并算 sha-256。事务外执行。"""
        rows = self._db.query(
            "SELECT part_number, size, path FROM parts WHERE session_id = ?",
            (session_id,),
        )
        parts = {num: (size, path) for num, size, path in rows}
        num_parts = self._num_parts(declared_size)
        if len(parts) != num_parts or set(parts) != set(range(1, num_parts + 1)):
            raise UploadIncomplete(
                f"session {session_id}: have {len(parts)} of {num_parts} parts"
            )
        total = 0
        for num in range(1, num_parts + 1):
            size, _ = parts[num]
            expected = (
                self._part_size
                if num < num_parts
                else declared_size - self._part_size * (num_parts - 1)
            )
            if size != expected:
                raise UploadIncomplete(
                    f"session {session_id}: part {num} has {size} bytes, "
                    f"expected {expected}"
                )
            total += size
        if total != declared_size:
            raise UploadIncomplete(
                f"session {session_id}: parts total {total}, "
                f"declared {declared_size}"
            )
        hasher = hashlib.sha256()
        with open(tmp_path, "wb") as out:
            for num in range(1, num_parts + 1):
                _, path = parts[num]
                with open(path, "rb") as part_file:
                    while True:
                        chunk = part_file.read(_CHUNK)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        out.write(chunk)
        return hasher.hexdigest()

    def _release_reservation_by_session(self, session_id: str) -> None:
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT id, account, bytes FROM reservations WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                return
            reservation_id, account, size = row
            conn.execute(
                "DELETE FROM reservations WHERE id = ?", (reservation_id,)
            )
            conn.execute(
                "UPDATE accounts SET reserved_bytes = reserved_bytes - ? "
                "WHERE name = ?",
                (size, account),
            )
    # ------------------------------------------------------------------
    # 删除 / 恢复 / 清理
    # ------------------------------------------------------------------

    def delete(self, account: str, key: str) -> None:
        """对象进回收站，留存期间照常计费。"""
        now = self._time_fn()
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT status FROM objects WHERE account = ? AND key = ?",
                (account, key),
            ).fetchone()
            if row is None:
                raise ObjectNotFound(f"no object {key} for account {account!r}")
            if row[0] == "active":
                conn.execute(
                    "UPDATE objects SET status = 'trash', deleted_at = ? "
                    "WHERE account = ? AND key = ?",
                    (now, account, key),
                )

    def restore(self, account: str, key: str) -> None:
        """回收站里的对象恢复为正常。"""
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT status FROM objects WHERE account = ? AND key = ?",
                (account, key),
            ).fetchone()
            if row is None:
                raise ObjectNotFound(f"no object {key} for account {account!r}")
            if row[0] == "trash":
                conn.execute(
                    "UPDATE objects SET status = 'active', deleted_at = NULL "
                    "WHERE account = ? AND key = ?",
                    (account, key),
                )

    def cleanup(self, now: float | None = None) -> None:
        """由上层定时触发：

        1. 回收站满 7 天的对象彻底清除，释放额度；所有账户都不再持有
           的内容，磁盘文件一并删除。
        2. 连续 24 小时没有新段的分段会话视为放弃，释放预占、删掉已传的段。
        """
        if now is None:
            now = self._time_fn()
        self._purge_trash(now - TRASH_RETENTION_SECONDS)
        self._abandon_stale_sessions(now - SESSION_IDLE_SECONDS)

    def _purge_trash(self, deleted_before: float) -> None:
        with self._db.transaction() as conn:
            rows = conn.execute(
                "SELECT account, key, size FROM objects "
                "WHERE status = 'trash' AND deleted_at <= ?",
                (deleted_before,),
            ).fetchall()
            files_to_remove = []
            for account, key, size in rows:
                conn.execute(
                    "DELETE FROM objects WHERE account = ? AND key = ?",
                    (account, key),
                )
                conn.execute(
                    "UPDATE accounts SET used_bytes = used_bytes - ? WHERE name = ?",
                    (size, account),
                )
                conn.execute(
                    "UPDATE blobs SET refcount = refcount - 1 WHERE key = ?",
                    (key,),
                )
                refcount = conn.execute(
                    "SELECT refcount FROM blobs WHERE key = ?", (key,)
                ).fetchone()[0]
                if refcount <= 0:
                    conn.execute("DELETE FROM blobs WHERE key = ?", (key,))
                    files_to_remove.append(self._blob_path(key))
            for path in files_to_remove:
                self._remove_file(path)

    def _abandon_stale_sessions(self, idle_before: float) -> None:
        with self._db.transaction() as conn:
            rows = conn.execute(
                "SELECT id, account FROM sessions "
                "WHERE status = 'open' AND updated_at <= ?",
                (idle_before,),
            ).fetchall()
            for session_id, account in rows:
                conn.execute(
                    "UPDATE sessions SET status = 'abandoned' WHERE id = ?",
                    (session_id,),
                )
                res = conn.execute(
                    "SELECT id, bytes FROM reservations WHERE session_id = ?",
                    (session_id,),
                ).fetchone()
                if res is not None:
                    conn.execute(
                        "DELETE FROM reservations WHERE id = ?", (res[0],)
                    )
                    conn.execute(
                        "UPDATE accounts SET reserved_bytes = reserved_bytes - ? "
                        "WHERE name = ?",
                        (res[1], account),
                    )
                part_paths = [
                    r[0]
                    for r in conn.execute(
                        "SELECT path FROM parts WHERE session_id = ?",
                        (session_id,),
                    ).fetchall()
                ]
                conn.execute(
                    "DELETE FROM parts WHERE session_id = ?", (session_id,)
                )
                for path in part_paths:
                    self._remove_file(path)
    def close(self) -> None:
        self._db.close()
