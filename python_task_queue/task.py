import uuid

from .shared import Args, KwArgs
from .result import Result
from .shared import datetime_now
from .state import State


import dataclasses as dc
import datetime as dt
from typing import Any, Callable


def _validate_not_empty(value: str, name: str):
    if value == "":
        raise ValueError(f"{name} must not me empty", value)


def _validate_positive(value: int | float, name: str):
    if value <= 0:
        raise ValueError(f"{name} must be > 0", value)


def gen_uuid() -> str:
    return str(uuid.uuid4())


@dc.dataclass(frozen=True)
class Options:
    id: str = dc.field(default_factory=gen_uuid)

    # number of attempts to complete the task
    tries: int = 3

    # delay between attempts to complete a task
    tries_delay: dt.timedelta = dt.timedelta(seconds=1)

    # time to complete a task, after which it is killed
    timeout: dt.timedelta = dt.timedelta(minutes=1)

    # task lifetime in queue
    ttl: dt.timedelta = dt.timedelta(days=1)

    # lifetime of the task result in storage
    result_ttl: dt.timedelta = dt.timedelta(days=1)

    def __post_init__(self):
        _validate_not_empty(self.id, "id")
        _validate_positive(self.tries, "tries")
        _validate_positive(self.timeout.total_seconds(), "timeout")
        _validate_positive(self.ttl.total_seconds(), "ttl")
        _validate_positive(self.result_ttl.total_seconds(), "result_ttl")


@dc.dataclass(frozen=True)
class Task:
    # TODO: add generic function type
    fn: Callable[..., Any]
    args: Args = dc.field(default_factory=tuple)
    kwargs: KwArgs = dc.field(default_factory=dict)
    opts: Options = dc.field(default_factory=Options)

    state: State = State.PENDING
    enqueued_at: dt.datetime = dc.field(default_factory=datetime_now)
    started_at: dt.datetime | None = None
    ended_at: dt.datetime | None = None
    result: Result | None = None

    @property
    def id(self) -> str:
        return self.opts.id

    def update(self, **changes: Any) -> "Task":
        task = dc.replace(self, **changes)
        return task

    def is_expired(self):
        return self.enqueued_at + self.opts.ttl < datetime_now()
