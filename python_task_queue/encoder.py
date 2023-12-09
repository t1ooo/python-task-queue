import pickle
from typing import Any, TypeVar
from .shared import checked_cast

_T = TypeVar("_T")


class Encoder:
    def encode(self, data: Any) -> bytes:
        if data is None:
            raise Exception("data is None")
        return pickle.dumps(data)

    def decode(self, typ: type[_T], data: bytes | None) -> _T | None:
        if data is None:
            return None
        if type(data) is bytes:
            return checked_cast(typ, pickle.loads(data))
        raise Exception("invalid data type")
