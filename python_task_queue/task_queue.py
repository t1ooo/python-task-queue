import dataclasses as dc

from .shared import Args, KwArgs
from .broker import Broker
from .shared import sleep
from .logger import cls_logger
from .result import Error, Result
from .state import State
from .storage import Storage, StorageWrapper
from .task import Options, Task
from .worker import Worker


import datetime as dt
from typing import Any, Callable


@dc.dataclass(frozen=True)
class TimeoutError:
    task_id: str
    timeout: dt.timedelta


@dc.dataclass(frozen=True)
class CanceledError(Exception):
    task_id: str


@dc.dataclass(frozen=True)
class ExpiredError(Exception):
    task_id: str


class TaskQueue:
    def __init__(
        self,
        broker: Broker,
        storage: Storage,
        worker: Worker,
    ) -> None:
        self.broker = broker
        self.storage = StorageWrapper(storage)
        self.worker = worker
        self._logger = cls_logger(self)

    def close(self):
        self._logger.info("close")
        self.broker.close()
        self.storage.close()
        self.worker.stop()
        self._logger.info("closed")

    def submit(
        self,
        fn: Callable[..., Any],
        args: Args | None = None,
        kwargs: KwArgs | None = None,
        opts: Options | None = None,
    ) -> str:
        args = args or tuple()
        kwargs = kwargs or {}
        opts = opts or Options()
        task = Task(fn=fn, args=args, kwargs=kwargs, opts=opts)

        self._logger.debug(f"submit task: {task}")

        initial_ttl = max(task.opts.ttl, task.opts.result_ttl)
        self.storage.set(task.id, task, initial_ttl)
        self.broker.enqueue(task.id)

        return task.id

    def task(self, task_id: str) -> Task | None:
        return self.storage.get(task_id)

    def state(self, task_id: str) -> State | None:
        task = self.storage.get(task_id)
        if task is None:
            return None
        return task.state

    def result(self, task_id: str, timeout: dt.timedelta) -> Result | None:
        for _ in sleep(timeout, step=dt.timedelta(milliseconds=100)):
            if self.worker.running_tasks.is_registered(task_id):
                continue

            if task := self.storage.get(task_id):
                if task.state == State.CANCELLED:
                    return Error(CanceledError(task_id))
                if task.state == State.EXPIRED:
                    return Error(ExpiredError(task_id))

                if task.result is not None:
                    return task.result

        return Error(TimeoutError(task_id, timeout))

    def cancel(self, task_id: str) -> bool:
        task = self.storage.get(task_id)
        if task is None:
            return False

        self.worker.kill_task(task_id)
        task = task.update(state=State.CANCELLED)
        self.storage.set(task.id, task, task.opts.result_ttl)
        return True