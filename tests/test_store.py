import hashlib
import io
import os
import time

import pytest

from objstore import (
    CommitIncomplete,
    ObjectNotFound,
    ObjectStore,
    ObjectTooLarge,
    PART_SIZE,
    QuotaExceeded,
    SessionClosed,
    SizeMismatch,
)


@pytest.fixture()
def store(tmp_path):
    s = ObjectStore(str(tmp_path))
    yield s
    s.close()


def make_data(size, seed=b"x"):
    return (seed * (size // len(seed) + 1))[:size]


class FailingStream:
    """Yields some bytes, then blows up mid-stream."""

    def __init__(self, chunks):
        self._chunks = iter(chunks)

    def read(self, n=-1):
        try:
            return next(self._chunks)
        except StopIteration:
            raise IOError("stream died")


def test_upload_and_read_roundtrip(store):
    store.create_account("a")
    data = make_data(100_000)
    key = store.upload("a", len(data), io.BytesIO(data))
    assert key == hashlib.sha256(data).hexdigest()
    assert store.read("a", key).read() == data
    assert store.usage("a") == {
        "quota": 10 * 1024**3, "used": len(data), "reserved": 0}


def test_dedup_same_account_charges_once(store):
    store.create_account("a")
    data = make_data(10_000)
    k1 = store.upload("a", len(data), io.BytesIO(data))
    k2 = store.upload("a", len(data), io.BytesIO(data))
    assert k1 == k2
    assert store.usage("a")["used"] == len(data)


def test_cross_account_billed_separately_one_file(store, tmp_path):
    store.create_account("a")
    store.create_account("b")
    data = make_data(10_000)
    store.upload("a", len(data), io.BytesIO(data))
    store.upload("b", len(data), io.BytesIO(data))
    assert store.usage("a")["used"] == len(data)
    assert store.usage("b")["used"] == len(data)
    files = os.listdir(tmp_path / "content")
    assert files == [hashlib.sha256(data).hexdigest()]


def test_size_mismatch_rolls_back(store):
    store.create_account("a")
    before = store.usage("a")
    with pytest.raises(SizeMismatch):
        store.upload("a", 100, io.BytesIO(b"short"))
    assert store.usage("a") == before


def test_stream_error_rolls_back(store):
    store.create_account("a")
    before = store.usage("a")
    with pytest.raises(IOError):
        store.upload("a", 100_000, FailingStream([b"chunk"] * 3))
    assert store.usage("a") == before
    assert store._query("SELECT COUNT(*) FROM objects")[0][0] == 0


def test_quota_enforced_on_reserve(store):
    store.create_account("a", quota_bytes=1000)
    store.upload("a", 600, io.BytesIO(make_data(600)))
    with pytest.raises(QuotaExceeded):
        store.upload("a", 500, io.BytesIO(make_data(500)))
    # reservation from the rejected upload is gone
    assert store.usage("a")["reserved"] == 0


def test_object_too_large_rejected(store):
    store.create_account("a")
    with pytest.raises(ObjectTooLarge):
        store.upload("a", 500 * 1024 * 1024 + 1, io.BytesIO(b""))


def test_quota_decrease_blocks_new_uploads_but_keeps_objects(store):
    store.create_account("a")
    data = make_data(1000)
    store.upload("a", len(data), io.BytesIO(data))
    store.set_quota("a", 10)  # below current usage
    assert store.read("a", hashlib.sha256(data).hexdigest()).read() == data
    with pytest.raises(QuotaExceeded):
        store.upload("a", 1, io.BytesIO(b"z"))


def test_delete_restore_cycle(store):
    store.create_account("a")
    data = make_data(500)
    key = store.upload("a", len(data), io.BytesIO(data))
    store.delete("a", key)
    assert store.usage("a")["used"] == len(data)  # trash still billed
    with pytest.raises(ObjectNotFound):
        store.delete("a", key)  # already trashed
    store.restore("a", key)
    assert store.read("a", key).read() == data


def test_upload_while_in_trash_restores_without_charge(store):
    store.create_account("a")
    data = make_data(500)
    key = store.upload("a", len(data), io.BytesIO(data))
    store.delete("a", key)
    key2 = store.upload("a", len(data), io.BytesIO(data))
    assert key2 == key
    assert store.usage("a")["used"] == len(data)
    assert store.read("a", key).read() == data


def test_trash_purged_after_retention(store, tmp_path):
    store.create_account("a")
    now = time.time()
    data = make_data(500)
    key = store.upload("a", len(data), io.BytesIO(data))
    store.delete("a", key)
    store.cleanup(now=now + 6 * 24 * 3600)
    assert store.usage("a")["used"] == len(data)
    store.cleanup(now=now + 7 * 24 * 3600 + 1)
    assert store.usage("a")["used"] == 0
    with pytest.raises(ObjectNotFound):
        store.read("a", key)
    assert not (tmp_path / "content" / key).exists()


def test_content_file_removed_only_after_last_holder_purged(store, tmp_path):
    store.create_account("a")
    store.create_account("b")
    data = make_data(500)
    key = store.upload("a", len(data), io.BytesIO(data))
    store.upload("b", len(data), io.BytesIO(data))
    now = time.time()
    store.delete("a", key)
    store.cleanup(now=now + 8 * 24 * 3600)
    assert (tmp_path / "content" / key).exists()  # b still holds it
    store.delete("b", key)
    store.cleanup(now=now + 16 * 24 * 3600)
    assert not (tmp_path / "content" / key).exists()


def test_multipart_roundtrip_out_of_order_and_duplicate_parts(store):
    store.create_account("a")
    data = make_data(2 * PART_SIZE + 7, seed=b"p")
    sid = store.create_session("a", len(data))
    store.put_part(sid, 3, io.BytesIO(data[2 * PART_SIZE:]))
    store.put_part(sid, 1, io.BytesIO(b"wrong" * 1000))
    store.put_part(sid, 1, io.BytesIO(data[:PART_SIZE]))  # last write wins
    store.put_part(sid, 2, io.BytesIO(data[PART_SIZE:2 * PART_SIZE]))
    key = store.commit_session(sid)
    assert key == hashlib.sha256(data).hexdigest()
    assert store.read("a", key).read() == data
    assert store.usage("a")["reserved"] == 0


def test_commit_incomplete_keeps_session_and_reservation(store):
    store.create_account("a")
    size = PART_SIZE + 5
    sid = store.create_session("a", size)
    store.put_part(sid, 1, io.BytesIO(make_data(PART_SIZE, seed=b"q")))
    with pytest.raises(CommitIncomplete):
        store.commit_session(sid)
    assert store.usage("a")["reserved"] == size
    store.put_part(sid, 2, io.BytesIO(b"tail!"))
    key = store.commit_session(sid)
    assert store.usage("a")["reserved"] == 0
    assert store.usage("a")["used"] == size
    # recommit is idempotent
    assert store.commit_session(sid) == key


def test_session_expiry_releases_reservation_and_blocks_ops(store):
    store.create_account("a")
    sid = store.create_session("a", PART_SIZE)
    store.put_part(sid, 1, io.BytesIO(make_data(PART_SIZE)))
    assert store.usage("a")["reserved"] == PART_SIZE
    store.cleanup(now=time.time() + 25 * 3600)
    assert store.usage("a")["reserved"] == 0
    with pytest.raises(SessionClosed):
        store.put_part(sid, 1, io.BytesIO(b"x"))
    with pytest.raises(SessionClosed):
        store.commit_session(sid)


def test_multipart_dedup_against_existing_object(store):
    store.create_account("a")
    data = make_data(PART_SIZE + 3, seed=b"d")
    key = store.upload("a", len(data), io.BytesIO(data))
    sid = store.create_session("a", len(data))
    store.put_part(sid, 1, io.BytesIO(data[:PART_SIZE]))
    store.put_part(sid, 2, io.BytesIO(data[PART_SIZE:]))
    assert store.commit_session(sid) == key
    assert store.usage("a")["used"] == len(data)
    assert store.usage("a")["reserved"] == 0
