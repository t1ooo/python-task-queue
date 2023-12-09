import datetime as dt
import time
from typing import Any, Generator, Iterable, Mapping, TypeVar

_T = TypeVar("_T")


def checked_cast(typ: type[_T], value: _T) -> _T:
    if type(value) is not typ:
        raise Exception(f"invalid type: expected {typ}, got {type(value)}")
    return value


Args = Iterable[Any]
KwArgs = Mapping[str, Any]


def sleep(delay: dt.timedelta, step: dt.timedelta) -> Generator[None, None, None]:
    """Sleep for a specified duration and to yield control periodically with a specified step duration."""
    stop = time.monotonic() + delay.total_seconds()
    while True:
        t = time.monotonic()
        if stop <= t:
            break
        s = min(step.total_seconds(), stop - t)

        time.sleep(s)
        yield


def datetime_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def build_key(*args: str) -> str:
    return "_".join(args)
