import io
import random

import pytest

from objstore import (
    InvalidPart,
    QuotaExceeded,
    SessionNotFound,
    SessionNotOpen,
    UploadIncomplete,
)
from objstore.store import SESSION_IDLE_SECONDS

from conftest import key_of, make_contents


def upload_content_in_parts(store, session_id, data, part_size, rng=None):
    """乱序、带重复地把 data 按 part_size 分段传完。"""
    rng = rng or random.Random(0)
    num_parts = (len(data) + part_size - 1) // part_size
    order = list(range(1, num_parts + 1))
    rng.shuffle(order)
    for n in order:
        chunk = data[(n - 1) * part_size : n * part_size]
        store.upload_part(session_id, n, io.BytesIO(chunk))
        if rng.random() < 0.3:  # 重复传同一段，内容相同
            store.upload_part(session_id, n, io.BytesIO(chunk))


def test_multipart_roundtrip_out_of_order(store):
    data = make_contents(7, 1, 500, 500)[0]
    sid = store.begin_upload("acc", len(data))
    upload_content_in_parts(store, sid, data, 64)
    key = store.commit_upload(sid)
    assert key == key_of(data)
    assert store.open_reader("acc", key).read() == data
    usage = store.usage("acc")
    assert usage.used_bytes == len(data)
    assert usage.reserved_bytes == 0


def test_last_part_may_be_short(store):
    data = b"x" * (64 * 3 + 17)
    sid = store.begin_upload("acc", len(data))
    upload_content_in_parts(store, sid, data, 64)
    assert store.commit_upload(sid) == key_of(data)


def test_part_overwrite_last_write_wins(store):
    good = b"g" * 64
    sid = store.begin_upload("acc", 64)
    store.upload_part(sid, 1, io.BytesIO(b"b" * 64))
    store.upload_part(sid, 1, io.BytesIO(good))
    key = store.commit_upload(sid)
    assert store.open_reader("acc", key).read() == good


def test_commit_incomplete_fails_session_kept(store):
    data = b"y" * 128
    sid = store.begin_upload("acc", len(data))
    store.upload_part(sid, 1, io.BytesIO(data[:64]))
    with pytest.raises(UploadIncomplete):
        store.commit_upload(sid)
    assert store.usage("acc").reserved_bytes == len(data)
    # 补齐后可再提交
    store.upload_part(sid, 2, io.BytesIO(data[64:]))
    assert store.commit_upload(sid) == key_of(data)
    assert store.usage("acc").reserved_bytes == 0


def test_commit_wrong_part_size_fails_then_fix(store):
    sid = store.begin_upload("acc", 128)
    store.upload_part(sid, 1, io.BytesIO(b"s" * 10))  # 非最后一段必须整段
    store.upload_part(sid, 2, io.BytesIO(b"t" * 64))
    with pytest.raises(UploadIncomplete):
        store.commit_upload(sid)
    store.upload_part(sid, 1, io.BytesIO(b"s" * 64))
    store.commit_upload(sid)


def test_repeated_commit_single_charge(store):
    data = b"once" * 64
    sid = store.begin_upload("acc", len(data))
    upload_content_in_parts(store, sid, data, 64)
    k1 = store.commit_upload(sid)
    k2 = store.commit_upload(sid)
    k3 = store.commit_upload(sid)
    assert k1 == k2 == k3
    assert store.usage("acc").used_bytes == len(data)


def test_invalid_part_number(store):
    sid = store.begin_upload("acc", 64)
    with pytest.raises(InvalidPart):
        store.upload_part(sid, 0, io.BytesIO(b"x"))
    with pytest.raises(InvalidPart):
        store.upload_part(sid, 2, io.BytesIO(b"x" * 64))


def test_unknown_session(store):
    with pytest.raises(SessionNotFound):
        store.upload_part("nope", 1, io.BytesIO(b"x"))
    with pytest.raises(SessionNotFound):
        store.commit_upload("nope")


def test_session_abandoned_after_idle(store, clock):
    data = b"idle" * 64
    sid = store.begin_upload("acc", len(data))
    store.upload_part(sid, 1, io.BytesIO(data[:64]))
    assert store.usage("acc").reserved_bytes == len(data)
    # 23 小时 59 分：清理不动
    store.cleanup(now=clock.now + SESSION_IDLE_SECONDS - 60)
    assert store.usage("acc").reserved_bytes == len(data)
    # 传新段刷新闲置计时
    store.upload_part(sid, 2, io.BytesIO(data[64:128]))
    store.cleanup(now=clock.now + SESSION_IDLE_SECONDS - 60)
    assert store.usage("acc").reserved_bytes == len(data)
    # 满 24 小时无新段：放弃，预占释放，传段/提交都失败
    store.cleanup(now=clock.now + SESSION_IDLE_SECONDS)
    assert store.usage("acc").reserved_bytes == 0
    with pytest.raises(SessionNotOpen):
        store.upload_part(sid, 3, io.BytesIO(b"z" * 64))
    with pytest.raises(SessionNotOpen):
        store.commit_upload(sid)


def test_multipart_respects_quota(store):
    store.set_quota("acc", 100)
    with pytest.raises(QuotaExceeded):
        store.begin_upload("acc", 101)
    sid = store.begin_upload("acc", 100)
    with pytest.raises(QuotaExceeded):
        store.begin_upload("acc", 1)
    store.upload_part(sid, 1, io.BytesIO(b"q" * 64))
    store.upload_part(sid, 2, io.BytesIO(b"q" * 36))
    store.commit_upload(sid)
    assert store.usage("acc").used_bytes == 100


def test_multipart_dedup_with_oneshot(store):
    data = b"shared-content" * 10
    key1 = store.put("acc", len(data), io.BytesIO(data))
    sid = store.begin_upload("acc", len(data))
    upload_content_in_parts(store, sid, data, 64)
    key2 = store.commit_upload(sid)
    assert key1 == key2
    assert store.usage("acc").used_bytes == len(data)


def test_zero_length_object(store):
    key = store.put("acc", 0, io.BytesIO(b""))
    assert store.open_reader("acc", key).read() == b""
    sid = store.begin_upload("acc", 0)
    assert store.commit_upload(sid) == key
    assert store.usage("acc").used_bytes == 0
