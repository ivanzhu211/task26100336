import io
import os

import pytest

from objstore import (
    ObjectNotFound,
    ObjectStore,
    ObjectTooLarge,
    QuotaExceeded,
    SizeMismatch,
)
from objstore.store import TRASH_RETENTION_SECONDS

from conftest import key_of, make_contents


class BrokenStream(io.RawIOBase):
    def read(self, n=-1):
        raise IOError("stream died mid-upload")


def test_put_and_read_back(store):
    data = b"checkpoint" * 100
    key = store.put("acc", len(data), io.BytesIO(data))
    assert key == key_of(data)
    assert store.open_reader("acc", key).read() == data
    usage = store.usage("acc")
    assert usage.used_bytes == len(data)
    assert usage.reserved_bytes == 0


def test_same_account_dedup_charges_once(store):
    data = b"dup" * 50
    k1 = store.put("acc", len(data), io.BytesIO(data))
    k2 = store.put("acc", len(data), io.BytesIO(data))
    assert k1 == k2
    assert store.usage("acc").used_bytes == len(data)


def test_cross_account_dedup_charges_each_but_stores_once(store):
    data = b"shared" * 40
    key = store.put("a", len(data), io.BytesIO(data))
    store.put("b", len(data), io.BytesIO(data))
    assert store.usage("a").used_bytes == len(data)
    assert store.usage("b").used_bytes == len(data)
    assert set(os.listdir(store._blob_dir)) == {key}


def test_quota_exceeded_rejected_before_upload(store):
    store.set_quota("acc", 100)
    with pytest.raises(QuotaExceeded):
        store.put("acc", 101, io.BytesIO(b"x" * 101))
    usage = store.usage("acc")
    assert usage.used_bytes == 0
    assert usage.reserved_bytes == 0


def test_reservation_counts_against_quota(store):
    store.set_quota("acc", 100)
    sid = store.begin_upload("acc", 60)
    assert store.usage("acc").reserved_bytes == 60
    with pytest.raises(QuotaExceeded):
        store.put("acc", 50, io.BytesIO(b"y" * 50))
    store.upload_part(sid, 1, io.BytesIO(b"z" * 60))
    store.commit_upload(sid)
    usage = store.usage("acc")
    assert usage.used_bytes == 60
    assert usage.reserved_bytes == 0


def test_failed_upload_leaves_no_trace(store):
    data = b"ok" * 30
    store.put("acc", len(data), io.BytesIO(data))
    before = store.usage("acc")
    with pytest.raises(SizeMismatch):
        store.put("acc", 10, io.BytesIO(b"actually-longer-than-that"))
    with pytest.raises(SizeMismatch):
        store.put("acc", 100, io.BytesIO(b"short"))
    with pytest.raises(IOError):
        store.put("acc", 64, BrokenStream())
    after = store.usage("acc")
    assert after == before
    assert os.listdir(store._tmp_dir) == []


def test_object_too_large(store):
    small = make_contents(1, 1, 1, 1)[0]
    store._max_object_bytes = 10
    with pytest.raises(ObjectTooLarge):
        store.put("acc", 11, io.BytesIO(b"x" * 11))
    with pytest.raises(ObjectTooLarge):
        store.begin_upload("acc", 11)
    store.put("acc", len(small), io.BytesIO(small))
    assert store.usage("acc").used_bytes == len(small)


def test_default_quota_is_10gb(tmp_path, clock):
    s = ObjectStore(str(tmp_path / "m.db"), str(tmp_path / "d"), time_fn=clock)
    try:
        assert s.usage("newbie").quota_bytes == 10 * 1024**3
    finally:
        s.close()


def test_delete_restore_reupload_cycle(store, clock):
    data = b"cycle" * 20
    key = store.put("acc", len(data), io.BytesIO(data))
    store.delete("acc", key)
    # 回收站期间照常计费
    assert store.usage("acc").used_bytes == len(data)
    # 回收站中同内容再传：恢复为正常，不加费
    assert store.put("acc", len(data), io.BytesIO(data)) == key
    assert store.usage("acc").used_bytes == len(data)
    store.delete("acc", key)
    store.restore("acc", key)
    assert store.open_reader("acc", key).read() == data
    with pytest.raises(ObjectNotFound):
        store.delete("acc", "0" * 64)
    with pytest.raises(ObjectNotFound):
        store.restore("acc", "0" * 64)


def test_trash_purged_after_retention(store, clock):
    data = b"purge me" * 10
    key = store.put("acc", len(data), io.BytesIO(data))
    store.delete("acc", key)
    deleted_at = clock.now
    store.cleanup(now=deleted_at + TRASH_RETENTION_SECONDS - 1)
    assert store.usage("acc").used_bytes == len(data)
    store.cleanup(now=deleted_at + TRASH_RETENTION_SECONDS)
    assert store.usage("acc").used_bytes == 0
    with pytest.raises(ObjectNotFound):
        store.open_reader("acc", key)
    assert os.listdir(store._blob_dir) == []


def test_blob_removed_only_when_last_holder_purged(store, clock):
    data = b"two holders" * 10
    key = store.put("a", len(data), io.BytesIO(data))
    store.put("b", len(data), io.BytesIO(data))
    store.delete("a", key)
    store.delete("b", key)
    t0 = clock.now
    store.cleanup(now=t0 + TRASH_RETENTION_SECONDS)
    # 两个账户同刻删除，一起到期
    assert store.usage("a").used_bytes == 0
    assert store.usage("b").used_bytes == 0
    assert os.listdir(store._blob_dir) == []


def test_quota_reduction_blocks_new_uploads_until_usage_drops(store):
    data1 = b"a" * 100
    data2 = b"b" * 100
    store.set_quota("acc", 1000)
    k1 = store.put("acc", len(data1), io.BytesIO(data1))
    sid = store.begin_upload("acc", len(data2))
    store.upload_part(sid, 1, io.BytesIO(data2[:64]))
    store.upload_part(sid, 2, io.BytesIO(data2[64:]))
    # 下调到低于 已用+预占：已有对象和已批准预占不受影响
    store.set_quota("acc", 50)
    k2 = store.commit_upload(sid)
    assert store.usage("acc").used_bytes == 200
    # 用量降回额度内之前，新上传一律拒绝
    with pytest.raises(QuotaExceeded):
        store.put("acc", 1, io.BytesIO(b"z"))
    store.delete("acc", k1)
    store.delete("acc", k2)
    store.cleanup(now=store._time_fn() + TRASH_RETENTION_SECONDS)
    assert store.usage("acc").used_bytes == 0
    store.put("acc", 1, io.BytesIO(b"z"))
    assert store.usage("acc").used_bytes == 1
