import datetime as dt
import threading
from abc import abstractmethod

from .shared import sleep

from .executor import ExecutorStopError, ProcessExecutor, ProcessKillError
from .result import Error, Ok, Result
from .state import State
from .task import Task
import time
from .broker import Broker, Message
from .encoder import Encoder
from .storage import Storage, StorageWrapper
from .shared import (
    datetime_now,
)
from .logger import cls_logger, logger


class PeriodicThread(threading.Thread):
    """Run periodic function with delay"""

    SLEEP_STEP = dt.timedelta(milliseconds=100)

    def __init__(
        self,
        delay: dt.timedelta,
    ) -> None:
        super().__init__()
        self._delay = delay

        self._stopped = False
        self._logger = cls_logger(self)

    def join(self, timeout: float | None = None) -> None:
        try:
            self._logger.info("join")
            self._stopped = True
            super().join(timeout)
            self._logger.info("joined")
        except Exception as e:
            self._logger.exception(e)

    def run(self):
        while not self._stopped:
            for _ in sleep(self._delay, self.SLEEP_STEP):
                if self._stopped:
                    break

            try:
                self.run_next()
            except Exception as e:
                self._logger.exception(e)

    @abstractmethod
    def run_next(self):
        NotImplementedError()


class RunningTasks:
    def __init__(self):
        self._task_ids: set[str] = set()
        self._lock = threading.Lock()
        self._logger = cls_logger(self)

    def is_registered(self, task_id: str) -> bool:
        with self._lock:
            return task_id in self._task_ids

    def register(self, task_id: str) -> bool:
        """Register the task if it has not been registered yet and return True, otherwise return False"""
        with self._lock:
            if task_id in self._task_ids:
                return False
            self._task_ids.add(task_id)
            self._logger.debug(f"register task: {task_id}")
            return True

    def remove(self, task_id: str):
        with self._lock:
            self._task_ids.remove(task_id)
            self._logger.debug(f"remove task: {task_id}")


class ConsumerWorker(PeriodicThread):
    def __init__(
        self,
        broker: Broker,
        storage: StorageWrapper,
        executor: "ProcessExecutor",
        polling_delay: dt.timedelta,
        running_tasks: RunningTasks,
    ) -> None:
        self._broker = broker
        self._storage = storage
        self._executor = executor
        self._running_tasks = running_tasks
        self._logger = cls_logger(self)
        super().__init__(polling_delay)

    def _on_already_running(self, message: Message):
        self._logger.warning(f"task already running, skip: {message.task_id}")

    def _on_not_found(self, message: Message):
        self._logger.warning(f"task not found, skip: {message.task_id}")
        self._broker.done(message)

    def _on_bad_state(self, message: Message, task: Task):
        assert task.result is not None
        self._logger.warning(f"task state not in (pending, running), skip: {task.id}")

    def _on_expired(self, message: Message, task: Task):
        self._logger.info(f"task expired, skip: {task.id}")
        task = task.update(state=State.EXPIRED)
        self._storage.set(task.id, task, task.opts.result_ttl)
        self._broker.done(message)

    def _on_execute(self, message: Message, task: Task):
        assert task.result is None
        task = task.update(state=State.RUNNING)
        self._storage.set(task.id, task, task.opts.result_ttl)

        started_at = datetime_now()
        result = self._execute(task)
        ended_at = datetime_now()

        state = State.SUCCESS
        if type(result) is Error:
            if type(result.value) is ExecutorStopError:
                self._logger.info(f"executor stopped, ignore result: {task.id}")
                return
            elif type(result.value) is ProcessKillError:
                state = State.CANCELLED
            else:
                state = State.FAILURE

        task = task.update(
            started_at=started_at, ended_at=ended_at, result=result, state=state
        )
        self._storage.set(task.id, task, task.opts.result_ttl)
        self._logger.debug(f"result: {task.id}, {state}, {result}")

        self._broker.done(message)

    def run_next(self):
        message = self._broker.dequeue()
        if not message:
            return

        self._logger.debug(f"receive message: {message}")

        task_id = message.task_id

        try:
            if not self._running_tasks.register(task_id):
                self._on_already_running(message)
                return

            task = self._storage.get(task_id)
            if task is None:
                self._on_not_found(message)
                return

            # NOTE: if task still have status running,  maybe it failed
            if task.state not in (State.PENDING, State.RUNNING):
                self._on_bad_state(message, task)
                return

            if task.is_expired():
                self._on_expired(message, task)
                return

            self._on_execute(message, task)
            return
        finally:
            self._running_tasks.remove(task_id)

    def _execute(self, task: Task) -> Result:
        result = None

        for i in range(task.opts.tries):
            is_last = i == task.opts.tries - 1

            result = self._executor.execute(
                name=task.id,
                fn=task.fn,
                args=task.args,
                kwargs=task.kwargs,
                timeout=task.opts.timeout,
            )

            if type(result) is Ok:
                return result

            if type(result) is Error:
                if type(result.value) is ProcessKillError:
                    return result

                if type(result.value) is ExecutorStopError:
                    return result

                if not is_last:
                    s = task.opts.tries_delay.total_seconds()
                    time.sleep(s)

        assert result is not None  # NOTE: never

        return result


class ExpiredResultsWorker(PeriodicThread):
    def __init__(
        self,
        storage: StorageWrapper,
        polling_delay: dt.timedelta,
    ) -> None:
        self._storage = storage
        self._logger = cls_logger(self)
        super().__init__(polling_delay)

    def run_next(self):
        self._logger.info("remove_expired")
        self._storage.remove_expired()


class DeadMessagesWorker(PeriodicThread):
    def __init__(
        self,
        broker: Broker,
        polling_delay: dt.timedelta,
    ) -> None:
        self._broker = broker
        self._logger = cls_logger(self)
        super().__init__(polling_delay)

    def run_next(self):
        self._logger.info("requeue")
        self._broker.requeue()


class Worker:
    def __init__(
        self,
        broker: Broker,
        storage: Storage,
        workers_count: int = 4,
        polling_delay: dt.timedelta = dt.timedelta(milliseconds=100),
        failed_polling_delay: dt.timedelta = dt.timedelta(minutes=1),
        expired_polling_delay: dt.timedelta = dt.timedelta(minutes=10),
    ) -> None:
        self._broker = broker
        self._storage = StorageWrapper(storage, Encoder())
        self._workers_count = workers_count
        self._workers: list[PeriodicThread] = []
        self._polling_delay = polling_delay
        self._failed_polling_delay = failed_polling_delay
        self._expired_polling_delay = expired_polling_delay
        self._executor = ProcessExecutor()
        self.running_tasks = RunningTasks()
        self._logger = cls_logger(self)
        self._event = threading.Event()
        self._stopped = False
        self._init_workers()

    def _init_workers(self):
        for _ in range(self._workers_count):
            w = ConsumerWorker(
                self._broker,
                self._storage,
                self._executor,
                self._polling_delay,
                self.running_tasks,
            )
            self._workers.append(w)

        w = DeadMessagesWorker(self._broker, self._failed_polling_delay)
        self._workers.append(w)

        w = ExpiredResultsWorker(self._storage, self._expired_polling_delay)
        self._workers.append(w)

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self._stop()

    def _stop(self):
        self._logger.info("close")
        try:
            self._executor.stop()

            for t in self._workers:
                t.join()
        except Exception as e:
            self._logger.exception(e)
        self._logger.info("closed")

    def start(self, block: bool = True):
        self._logger.info("start")

        for t in self._workers:
            t.start()

        self._logger.info("started")

        if block:
            self._event.wait()

    def kill_task(self, task_id: str) -> bool:
        return self._executor.kill(task_id)
