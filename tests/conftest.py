import hashlib
import random

import pytest

from objstore import ObjectStore


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(tmp_path, clock):
    s = ObjectStore(
        str(tmp_path / "meta.db"),
        str(tmp_path / "data"),
        part_size=64,
        time_fn=clock,
    )
    yield s
    s.close()


def make_contents(seed: int, count: int, min_size: int, max_size: int) -> list[bytes]:
    rng = random.Random(seed)
    return [
        rng.randbytes(rng.randint(min_size, max_size)) for _ in range(count)
    ]


def key_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
