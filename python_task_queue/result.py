import dataclasses as dc
from typing import Any

@dc.dataclass(frozen=True)
class Result:
    # TODO: generic result
    value: Any


@dc.dataclass(frozen=True)
class Ok(Result):
    pass


@dc.dataclass(frozen=True)
class Error(Result):
    pass