"""Acceptance-level concurrency tests.

Runs a mixed multi-account, multi-thread workload and then verifies the
final state against the business rules recomputed from scratch.
"""
import hashlib
import io
import os
import random
import threading
import time

import pytest

from objstore import ObjectStore, PART_SIZE, SessionClosed, StoreError


def blob(seed: int, size: int) -> bytes:
    unit = hashlib.sha256(str(seed).encode()).digest()
    return (unit * (size // 32 + 1))[:size]


class FailingStream:
    def __init__(self, data, fail_after):
        self._data = data
        self._pos = 0
        self._fail_after = fail_after

    def read(self, n=-1):
        if self._pos >= self._fail_after:
            raise IOError("stream died mid-upload")
        chunk = self._data[self._pos:self._pos + 65536]
        self._pos += len(chunk)
        return chunk


class MixedWorkload:
    def __init__(self, store, accounts, small_blobs, big_blobs):
        self.store = store
        self.accounts = accounts
        self.small = small_blobs      # [(key, data)] one-shot pool
        self.big = big_blobs          # [(key, data)] multipart pool (2 parts)
        self.small_by_key = dict(small_blobs)
        self.big_by_key = dict(big_blobs)
        self.errors = []
        self._lock = threading.Lock()
        # quota-decrease bookkeeping: account -> max load seen at a decrease
        self.over_allowance = {a: 0 for a in accounts}

    def record_error(self, exc):
        with self._lock:
            self.errors.append(exc)

    # ---- random operations (each silently skips expected failures) --------
    def op_upload(self, rng, account):
        key, data = rng.choice(self.small)
        mode = rng.random()
        try:
            if mode < 0.1:
                # declared size does not match -> must fail cleanly
                self.store.upload(account, len(data) + 1, io.BytesIO(data))
            elif mode < 0.2:
                # stream dies mid-way -> must fail cleanly
                self.store.upload(account, len(data),
                                  FailingStream(data, len(data) // 2))
            else:
                self.store.upload(account, len(data), io.BytesIO(data))
        except StoreError:
            pass
        except IOError:
            pass

    def op_multipart(self, rng, account):
        key, data = rng.choice(self.big)
        try:
            sid = self.store.create_session(account, len(data))
        except StoreError:
            return
        parts = [data[:PART_SIZE], data[PART_SIZE:]]
        if rng.random() < 0.3:
            parts = list(reversed(parts))
            order = [2, 1]
        else:
            order = [1, 2]
        threads = []
        for num, chunk in zip(order, parts):
            threads.append(threading.Thread(
                target=self._put_part_quiet, args=(sid, num, chunk)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        try:
            self.store.commit_session(sid)
        except StoreError:
            pass

    def _put_part_quiet(self, sid, num, chunk):
        try:
            self.store.put_part(sid, num, io.BytesIO(chunk))
        except StoreError:
            pass

    def op_delete(self, rng, account):
        rows = self.store._query(
            "SELECT key FROM objects WHERE account_id = ? AND status = 'active'",
            (account,))
        if not rows:
            return
        try:
            self.store.delete(account, rng.choice(rows)[0])
        except StoreError:
            pass

    def op_restore(self, rng, account):
        rows = self.store._query(
            "SELECT key FROM objects WHERE account_id = ? AND status = 'trash'",
            (account,))
        if not rows:
            return
        try:
            self.store.restore(account, rng.choice(rows)[0])
        except StoreError:
            pass

    def op_cleanup(self, rng, account):
        # Random horizon: sometimes purges trash / aborts idle sessions.
        horizon = rng.choice([0, 3600, 8 * 24 * 3600, 25 * 3600])
        self.store.cleanup(now=time.time() + horizon)

    def op_set_quota(self, rng, account):
        usage = self.store.usage(account)
        load = usage["used"] + usage["reserved"]
        new_quota = rng.choice([
            10 * 1024**3,
            max(0, load - rng.randint(0, 10_000)),   # decrease below load
            load + rng.randint(0, 50_000),
        ])
        self.store.set_quota(account, new_quota)
        if load > new_quota:
            with self._lock:
                self.over_allowance[account] = max(
                    self.over_allowance[account], load)

    def run_worker(self, worker_id, ops):
        rng = random.Random(worker_id * 7919)
        ops_table = [self.op_upload] * 40 + [self.op_multipart] * 10 + \
            [self.op_delete] * 15 + [self.op_restore] * 10 + \
            [self.op_cleanup] * 3 + [self.op_set_quota] * 4
        try:
            for _ in range(ops):
                account = rng.choice(self.accounts)
                rng.choice(ops_table)(rng, account)
        except Exception as exc:  # unexpected -> record, fail at the end
            self.record_error(exc)


def test_mixed_workload_final_state_consistent(tmp_path):
    store = ObjectStore(str(tmp_path))
    accounts = [f"acct{i}" for i in range(4)]
    for a in accounts:
        store.create_account(a)

    small_blobs = []
    for seed in range(12):
        data = blob(seed, rng_size := 1000 + seed * 1377)
        small_blobs.append((hashlib.sha256(data).hexdigest(), data))
    big_blobs = []
    for seed in range(100, 103):
        data = blob(seed, PART_SIZE + 1000 + seed)
        big_blobs.append((hashlib.sha256(data).hexdigest(), data))
    all_blobs = dict(small_blobs + big_blobs)

    wl = MixedWorkload(store, accounts, small_blobs, big_blobs)
    threads = [threading.Thread(target=wl.run_worker, args=(i, 120))
               for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not wl.errors, f"unexpected errors: {wl.errors[:3]}"

    # Stop the world: purge all trash, abort all lingering sessions.
    store.cleanup(now=time.time() + 30 * 24 * 3600)

    held_keys = set()
    for account in accounts:
        usage = store.usage(account)
        # Recompute usage from scratch per the business rules: each distinct
        # active object counts once at its size; no reservations may remain.
        expected_used = store._query(
            "SELECT COALESCE(SUM(size), 0) FROM objects "
            "WHERE account_id = ? AND status = 'active'", (account,))[0][0]
        distinct = store._query(
            "SELECT COUNT(DISTINCT key) FROM objects "
            "WHERE account_id = ? AND status = 'active'", (account,))[0][0]
        total_rows = store._query(
            "SELECT COUNT(*) FROM objects WHERE account_id = ? AND status = 'active'",
            (account,))[0][0]
        assert distinct == total_rows, "duplicate object rows for one account"
        assert usage["used"] == expected_used, (
            f"{account}: counter {usage['used']} != recomputed {expected_used}")
        assert usage["reserved"] == 0, f"{account}: leftover reservation"
        # Quota invariant: over-quota only if a decrease allowed it.
        assert usage["used"] + usage["reserved"] <= max(
            usage["quota"], wl.over_allowance[account])
        # Every surviving object reads back byte-for-byte.
        for (key,) in store._query(
                "SELECT key FROM objects WHERE account_id = ? AND status = 'active'",
                (account,)):
            assert store.read(account, key).read() == all_blobs[key]
            held_keys.add(key)
        # No trash may remain after the final cleanup.
        assert store._query(
            "SELECT COUNT(*) FROM objects WHERE account_id = ? AND status = 'trash'",
            (account,))[0][0] == 0

    # Disk holds exactly the contents still referenced, with exact refcounts.
    disk_files = set(os.listdir(tmp_path / "content"))
    assert disk_files == held_keys
    for key in held_keys:
        refcount = store._query(
            "SELECT refcount FROM contents WHERE sha256 = ?", (key,))[0][0]
        holders = store._query(
            "SELECT COUNT(*) FROM objects WHERE key = ?", (key,))[0][0]
        assert refcount == holders
    assert store._query(
        "SELECT COUNT(*) FROM contents WHERE sha256 NOT IN "
        "(SELECT key FROM objects)")[0][0] == 0
    # No leftover reservations anywhere.
    assert store._query(
        "SELECT COALESCE(SUM(reserved_bytes), 0) FROM accounts")[0][0] == 0
    store.close()


def test_trash_purge_races_upload_of_same_content(tmp_path):
    """Expired trash purge vs. concurrent upload of the same content."""
    store = ObjectStore(str(tmp_path))
    data = blob(1, 5000)
    key = hashlib.sha256(data).hexdigest()
    for round_no in range(30):
        account = f"a{round_no}"
        store.create_account(account)
        store.upload(account, len(data), io.BytesIO(data))
        store.delete(account, key)
        with store._txn() as conn:  # force the trash to look expired
            conn.execute(
                "UPDATE objects SET deleted_at = ? WHERE account_id = ? AND key = ?",
                (time.time() - 8 * 24 * 3600, account, key))
        barrier = threading.Barrier(2)
        outcome = {}

        def do_cleanup():
            barrier.wait()
            store.cleanup(now=time.time() + 8 * 24 * 3600)

        def do_upload():
            barrier.wait()
            outcome["key"] = store.upload(account, len(data), io.BytesIO(data))

        t1 = threading.Thread(target=do_cleanup)
        t2 = threading.Thread(target=do_upload)
        t1.start(); t2.start(); t1.join(); t2.join()

        # Whatever the interleaving: object is active, readable, billed once.
        assert outcome["key"] == key
        usage = store.usage(account)
        assert usage["used"] == len(data), f"round {round_no}: {usage}"
        assert usage["reserved"] == 0
        assert store.read(account, key).read() == data
        refcount = store._query(
            "SELECT refcount FROM contents WHERE sha256 = ?", (key,))[0][0]
        holders = store._query(
            "SELECT COUNT(*) FROM objects WHERE key = ?", (key,))[0][0]
        assert refcount == holders
    store.close()


def test_commit_races_session_expiry(tmp_path):
    """Concurrent commit and idle-session cleanup of the same session."""
    store = ObjectStore(str(tmp_path))
    for round_no in range(30):
        account = f"a{round_no}"
        store.create_account(account)
        data = blob(round_no, PART_SIZE + 100)
        key = hashlib.sha256(data).hexdigest()
        sid = store.create_session(account, len(data))
        store.put_part(sid, 1, io.BytesIO(data[:PART_SIZE]))
        store.put_part(sid, 2, io.BytesIO(data[PART_SIZE:]))
        with store._txn() as conn:  # force the session to look idle
            conn.execute(
                "UPDATE sessions SET last_part_at = ? WHERE session_id = ?",
                (time.time() - 25 * 3600, sid))
        barrier = threading.Barrier(2)
        outcome = {}

        def do_cleanup():
            barrier.wait()
            store.cleanup(now=time.time() + 25 * 3600)

        def do_commit():
            barrier.wait()
            try:
                outcome["key"] = store.commit_session(sid)
            except SessionClosed:
                outcome["key"] = None

        t1 = threading.Thread(target=do_cleanup)
        t2 = threading.Thread(target=do_commit)
        t1.start(); t2.start(); t1.join(); t2.join()

        usage = store.usage(account)
        assert usage["reserved"] == 0, f"round {round_no}: reservation leaked"
        if outcome["key"] is None:
            # Cleanup won: no object, no charge, no file.
            assert usage["used"] == 0
            with pytest.raises(StoreError):
                store.read(account, key)
        else:
            # Commit won: exactly one object, billed once, byte-identical.
            assert outcome["key"] == key
            assert usage["used"] == len(data)
            assert store.read(account, key).read() == data
            # recommit stays idempotent
            assert store.commit_session(sid) == key
            assert store.usage(account)["used"] == len(data)
        assert not os.path.isdir(tmp_path / "parts" / sid)
    store.close()


def test_large_upload_does_not_block_other_ops(tmp_path):
    """A slow, large upload must not serialize other ops of the same account."""
    store = ObjectStore(str(tmp_path))
    store.create_account("a")
    store.create_account("b")
    big = blob(7, 4 * 1024 * 1024)
    small = blob(8, 2000)
    small_key = hashlib.sha256(small).hexdigest()

    class SlowStream:
        def __init__(self):
            self._pos = 0

        def read(self, n=-1):
            if self._pos >= len(big):
                return b""
            time.sleep(0.002)
            chunk = big[self._pos:self._pos + 65536]
            self._pos += len(chunk)
            return chunk

    errors = []

    def run_upload():
        try:
            store.upload("a", len(big), SlowStream())
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    t = threading.Thread(target=run_upload)
    t.start()
    time.sleep(0.05)  # let the big upload get into its streaming phase
    start = time.monotonic()
    for _ in range(30):
        store.usage("a")
        store.upload("a", len(small), io.BytesIO(small))
        store.delete("a", small_key)
        store.restore("a", small_key)
        store.upload("b", len(small), io.BytesIO(small))
    elapsed = time.monotonic() - start
    t.join()
    assert not errors
    assert elapsed < 5.0, f"ops blocked behind large upload: {elapsed:.2f}s"
    assert store.read("a", hashlib.sha256(big).hexdigest()).read() == big
    store.close()
