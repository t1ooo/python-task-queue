from types import FunctionType
import pytest
from dataclasses import dataclass
from python_task_queue.encoder import Encoder


@dataclass(frozen=True)
class T:
    a: int
    b: str


def f(a: int, b: int) -> int:
    return a + b


def test_encode_decode():
    encoder = Encoder()

    data = [1, 2, 3]
    enc = encoder.encode(data)
    dec = encoder.decode(list, enc)
    assert data == dec

    data = "test"
    enc = encoder.encode(data)
    dec = encoder.decode(str, enc)
    assert data == dec

    data = T(1, "test")
    enc = encoder.encode(data)
    dec = encoder.decode(T, enc)
    assert data == dec

    data = f
    enc = encoder.encode(data)
    dec = encoder.decode(FunctionType, enc)
    assert data == dec


def test_encode_none():
    encoder = Encoder()

    data = None
    with pytest.raises(Exception) as exc_info:
        encoder.encode(data)
        assert str(exc_info.value) == "data is None"


def test_decode_none():
    encoder = Encoder()

    data = None
    out = encoder.decode(list, data)
    assert out is None


def test_decode_invalid_data_type():
    encoder = Encoder()

    data = "invalid_data"
    with pytest.raises(Exception) as exc_info:
        encoder.decode(list, data)
        assert str(exc_info.value) == "invalid data type"
