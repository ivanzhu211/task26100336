"""验收级并发测试：

1. 多账户多线程混合做一次性上传、并行分段上传、删除、恢复、清理、下调额度，
   同内容频繁碰撞，穿插失败上传。全部停下后逐账户对账，并逐字节读回校验。
2. 回收站到期清理 与 同账户同内容上传 同时发生。
3. 同一会话的提交 与 过期清理 同时发生。
"""

import io
import os
import random
import threading
import time

import pytest

from objstore import ObjectStore, QuotaExceeded, SessionNotOpen, SizeMismatch
from objstore.store import SESSION_IDLE_SECONDS, TRASH_RETENTION_SECONDS

from conftest import FakeClock, key_of
from test_store_basic import BrokenStream

PART = 4096


class Model:
    """按业务规则维护的账本：测试每步操作都同步更新它，最后与库对账。

    库操作与模型更新的顺序约定（保证模型顺序 == 数据库生效顺序）：
    - delete / restore / cleanup / set_quota / 拨时钟：全程持 model 锁；
    - put / commit 的流式部分在锁外，成功后持锁更新模型；
    - 正在上传的 (account, key) 记入 in_flight，delete/restore 避开它们。
      （put 与清理回收站、put 与 put 的最终结果与顺序无关，故无需避开。）
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.objects: dict[str, dict[str, list]] = {}  # acc -> key -> [size,status,deleted_at]
        self.in_flight: dict[tuple[str, str], int] = {}
        self.quota_exempt: set[str] = set()

    def ensure(self, account):
        return self.objects.setdefault(account, {})

    def add_flight(self, account, key):
        k = (account, key)
        self.in_flight[k] = self.in_flight.get(k, 0) + 1

    def drop_flight(self, account, key):
        k = (account, key)
        self.in_flight[k] -= 1
        if self.in_flight[k] == 0:
            del self.in_flight[k]

    def put_success(self, account, key, size):
        objs = self.ensure(account)
        if key in objs:
            objs[key][1] = "active"
            objs[key][2] = None
        else:
            objs[key] = [size, "active", None]

    def purge_expired(self, now):
        for objs in self.objects.values():
            for key in [k for k, v in objs.items()
                        if v[1] == "trash" and v[2] <= now - TRASH_RETENTION_SECONDS]:
                del objs[key]

    def expected_used(self, account):
        return sum(v[0] for v in self.ensure(account).values())


def test_mixed_workload_reconciles(tmp_path):
    clock = FakeClock()
    store = ObjectStore(
        str(tmp_path / "meta.db"), str(tmp_path / "data"),
        part_size=PART, time_fn=clock,
    )
    rng0 = random.Random(20261003)
    accounts = [f"acc{i}" for i in range(6)]
    for i, acc in enumerate(accounts):
        store.create_account(acc, quota_bytes=(200_000 if i % 3 == 0 else 5_000_000))
    contents = [rng0.randbytes(rng0.randint(100, 20_000)) for _ in range(30)]
    model = Model()
    errors = []

    def op_put(rng):
        acc, data = rng.choice(accounts), rng.choice(contents)
        key = key_of(data)
        with model.lock:
            model.add_flight(acc, key)
        try:
            store.put(acc, len(data), io.BytesIO(data))
        except QuotaExceeded:
            pass
        else:
            with model.lock:
                model.put_success(acc, key, len(data))
        finally:
            with model.lock:
                model.drop_flight(acc, key)

    def op_put_bad_size(rng):
        acc, data = rng.choice(accounts), rng.choice(contents)
        # 额度满时会在读流之前直接拒绝，两种结果都不留残余
        with pytest.raises((SizeMismatch, QuotaExceeded)):
            store.put(acc, len(data) + 1, io.BytesIO(data))

    def op_put_broken_stream(rng):
        acc, data = rng.choice(accounts), rng.choice(contents)
        with pytest.raises((IOError, QuotaExceeded)):
            store.put(acc, len(data), BrokenStream())

    def op_multipart(rng):
        acc, data = rng.choice(accounts), rng.choice(contents)
        key = key_of(data)
        with model.lock:
            model.add_flight(acc, key)
        try:
            try:
                sid = store.begin_upload(acc, len(data))
            except QuotaExceeded:
                return
            nums = list(range(1, (len(data) + PART - 1) // PART + 1))
            rng.shuffle(nums)
            for n in nums:
                chunk = data[(n - 1) * PART: n * PART]
                store.upload_part(sid, n, io.BytesIO(chunk))
                if rng.random() < 0.2:
                    store.upload_part(sid, n, io.BytesIO(chunk))  # 重复传
            store.commit_upload(sid)
        except SessionNotOpen:
            return  # 会话被清理放弃：不计费，模型不变
        else:
            with model.lock:
                model.put_success(acc, key, len(data))
        finally:
            with model.lock:
                model.drop_flight(acc, key)

    def op_multipart_abandon(rng):
        acc, data = rng.choice(accounts), rng.choice(contents)
        try:
            sid = store.begin_upload(acc, len(data))
        except QuotaExceeded:
            return
        try:
            n_parts = (len(data) + PART - 1) // PART + 1
            for n in range(1, max(2, n_parts // 2)):
                store.upload_part(sid, n, io.BytesIO(data[(n - 1) * PART: n * PART]))
        except SessionNotOpen:
            pass
        # 故意不提交，等清理放弃

    def op_delete(rng):
        acc = rng.choice(accounts)
        with model.lock:
            candidates = [k for k, v in model.ensure(acc).items()
                          if v[1] == "active" and model.in_flight.get((acc, k), 0) == 0]
            if not candidates:
                return
            key = rng.choice(candidates)
            store.delete(acc, key)
            model.objects[acc][key][1] = "trash"
            model.objects[acc][key][2] = clock.now

    def op_restore(rng):
        acc = rng.choice(accounts)
        with model.lock:
            candidates = [k for k, v in model.ensure(acc).items()
                          if v[1] == "trash" and model.in_flight.get((acc, k), 0) == 0]
            if not candidates:
                return
            key = rng.choice(candidates)
            store.restore(acc, key)
            model.objects[acc][key][1] = "active"
            model.objects[acc][key][2] = None

    def op_cleanup(rng):
        with model.lock:
            clock.advance(rng.uniform(0, 26 * 3600))
            store.cleanup(now=clock.now)
            model.purge_expired(clock.now)

    def op_set_quota(rng):
        acc = rng.choice(accounts)
        quota = rng.randint(50_000, 5_000_000)
        with model.lock:
            store.set_quota(acc, quota)
            usage = store.usage(acc)
            if usage.used_bytes + usage.reserved_bytes > quota:
                model.quota_exempt.add(acc)

    ops = [
        (op_put, 30),
        (op_put_bad_size, 4),
        (op_put_broken_stream, 4),
        (op_multipart, 20),
        (op_multipart_abandon, 5),
        (op_delete, 12),
        (op_restore, 8),
        (op_cleanup, 6),
        (op_set_quota, 3),
    ]
    weighted = [op for op, w in ops for _ in range(w)]

    def worker(seed):
        rng = random.Random(seed)
        try:
            for _ in range(250):
                rng.choice(weighted)(rng)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(1000 + i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"worker errors: {errors!r}"

    # 收尾：拨快时钟清理，放弃所有遗留会话、清除到期回收站
    with model.lock:
        clock.advance(2 * SESSION_IDLE_SECONDS)
        store.cleanup(now=clock.now)
        model.purge_expired(clock.now)

    # 1) 每个账户：已用 == 模型重算；预占 == 0
    for acc in accounts:
        usage = store.usage(acc)
        assert usage.reserved_bytes == 0, f"{acc}: reserved={usage.reserved_bytes}"
        assert usage.used_bytes == model.expected_used(acc), (
            f"{acc}: used={usage.used_bytes} expected={model.expected_used(acc)}"
        )
        if acc not in model.quota_exempt:
            assert usage.used_bytes + usage.reserved_bytes <= usage.quota_bytes

    # 2) 对象表与模型逐行一致
    rows = store._db.query(
        "SELECT account, key, size, status, deleted_at FROM objects"
    )
    db_objects = {}
    for acc, key, size, status, deleted_at in rows:
        db_objects[(acc, key)] = (size, status, deleted_at)
    model_objects = {}
    for acc, objs in model.objects.items():
        for key, (size, status, deleted_at) in objs.items():
            model_objects[(acc, key)] = (size, status, deleted_at)
    assert db_objects == model_objects

    # 3) 所有未彻底清除的对象逐字节读回
    by_key = {key_of(c): c for c in contents}
    for (acc, key), _ in model_objects.items():
        with store.open_reader(acc, key) as reader:
            assert reader.read() == by_key[key]

    # 4) 磁盘文件恰好是仍有账户持有的内容；临时目录无残余
    held = {key for (_, key) in model_objects}
    assert set(os.listdir(store._blob_dir)) == held
    blob_rows = store._db.query("SELECT key, refcount FROM blobs")
    assert {r[0] for r in blob_rows} == held
    for _, refcount in blob_rows:
        assert refcount >= 1
    assert os.listdir(store._tmp_dir) == []

    store.close()


def test_trash_purge_races_concurrent_reupload(tmp_path):
    """回收站到期清理 与 同账户同内容上传并发：
    结束后对象正常可读且只计一次费，预占恰好释放一次。"""
    clock = FakeClock()
    store = ObjectStore(str(tmp_path / "m.db"), str(tmp_path / "d"),
                        part_size=PART, time_fn=clock)
    store.create_account("acc")
    rng = random.Random(9)
    for i in range(120):
        data = rng.randbytes(500)
        key = store.put("acc", len(data), io.BytesIO(data))
        store.delete("acc", key)
        purge_at = clock.now + TRASH_RETENTION_SECONDS
        barrier = threading.Barrier(2)

        def do_cleanup():
            barrier.wait()
            store.cleanup(now=purge_at)

        def do_put():
            barrier.wait()
            store.put("acc", len(data), io.BytesIO(data))

        t1, t2 = threading.Thread(target=do_cleanup), threading.Thread(target=do_put)
        t1.start(); t2.start(); t1.join(); t2.join()

        usage = store.usage("acc")
        assert usage.used_bytes == len(data), f"iter {i}: {usage}"
        assert usage.reserved_bytes == 0, f"iter {i}: {usage}"
        assert store.open_reader("acc", key).read() == data
        rows = store._db.query(
            "SELECT status FROM objects WHERE account='acc' AND key=?", (key,))
        assert rows == [("active",)]
        assert store._db.query(
            "SELECT refcount FROM blobs WHERE key=?", (key,)) == [(1,)]

        # 复位：删除并彻底清理
        store.delete("acc", key)
        store.cleanup(now=clock.now + TRASH_RETENTION_SECONDS)
        assert store.usage("acc").used_bytes == 0
        assert os.listdir(store._blob_dir) == []
    store.close()


def test_commit_races_session_expiry_cleanup(tmp_path):
    """同一会话的提交与过期清理并发：
    要么提交成功（对象正常、计一次费），要么会话被放弃（不计费）；
    预占既不重复释放也不遗漏。"""
    clock = FakeClock()
    store = ObjectStore(str(tmp_path / "m.db"), str(tmp_path / "d"),
                        part_size=PART, time_fn=clock)
    store.create_account("acc")
    rng = random.Random(11)
    outcomes = {"committed": 0, "abandoned": 0}
    for i in range(120):
        data = rng.randbytes(3 * PART)
        sid = store.begin_upload("acc", len(data))
        for n in range(1, 4):
            store.upload_part(sid, n, io.BytesIO(data[(n - 1) * PART: n * PART]))
        barrier = threading.Barrier(2)
        result = {}

        def do_cleanup():
            barrier.wait()
            store.cleanup(now=clock.now + SESSION_IDLE_SECONDS)

        def do_commit():
            barrier.wait()
            if i % 2 == 0:
                time.sleep(0.002)  # 让清理先动手，覆盖“放弃”分支
            try:
                result["key"] = store.commit_upload(sid)
            except SessionNotOpen:
                result["key"] = None

        t1, t2 = threading.Thread(target=do_cleanup), threading.Thread(target=do_commit)
        t1.start(); t2.start(); t1.join(); t2.join()

        usage = store.usage("acc")
        assert usage.reserved_bytes == 0, f"iter {i}: {usage}"
        if result["key"] is not None:
            outcomes["committed"] += 1
            assert result["key"] == key_of(data)
            assert usage.used_bytes == len(data), f"iter {i}: {usage}"
            assert store.open_reader("acc", result["key"]).read() == data
            # 重复提交仍只计一次费
            assert store.commit_upload(sid) == result["key"]
            assert store.usage("acc").used_bytes == len(data)
            store.delete("acc", result["key"])
            store.cleanup(now=clock.now + TRASH_RETENTION_SECONDS)
            assert store.usage("acc").used_bytes == 0
        else:
            outcomes["abandoned"] += 1
            assert usage.used_bytes == 0, f"iter {i}: {usage}"
            with pytest.raises(SessionNotOpen):
                store.commit_upload(sid)
        assert os.listdir(store._tmp_dir) == []
    assert outcomes["committed"] > 0 and outcomes["abandoned"] > 0
    store.close()
