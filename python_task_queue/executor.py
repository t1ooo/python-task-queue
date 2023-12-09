import dataclasses as dc
import datetime as dt
import multiprocessing
from typing import Any, Callable
import threading

from .shared import Args
from .logger import cls_logger
from .shared import KwArgs
from .result import Error, Ok, Result


def wrap(fn: Callable[..., Any], out: dict[Any, Any]) -> Callable[..., None]:
    def wrapper(*args: Args, **kwargs: KwArgs):
        try:
            out["result"] = Ok(fn(*args, **kwargs))
        except BaseException as e:
            out["result"] = Error(e)

    return wrapper


@dc.dataclass(frozen=True)
class ProcessTimeoutError:
    name: str
    timeout: dt.timedelta


@dc.dataclass(frozen=True)
class ProcessKillError:
    # NOTE: killed outside
    name: str


@dc.dataclass(frozen=True)
class ExecutorStopError:
    name: str


class ProcessExecutor:
    def __init__(self):
        self._manager = multiprocessing.Manager()
        self._processes: dict[str, multiprocessing.Process] = {}
        self._logger = cls_logger(self)
        self._lock = threading.Lock()
        self._stopped = False

    def stop(self):
        if self._stopped:
            return
        self._stopped = True

        self._logger.info("close")
        try:
            self._kill_all()
            self._manager.shutdown()
        except Exception as e:
            self._logger.exception(e)

        self._logger.info("closed")

    def kill(self, name: str) -> bool:
        self._logger.info(f"kill: {name}")
        return self._kill(name)

    def _kill_all(self):
        with self._lock:
            for p in self._processes.values():
                self._kill_process(p)
            self._processes.clear()

    def _kill(self, name: str) -> bool:
        with self._lock:
            if p := self._processes.pop(name, None):
                self._kill_process(p)
                return True
            return False

    def _kill_process(self, p: multiprocessing.Process):
        try:
            if p.is_alive():
                p.kill()
                p.join()
            p.close()
        except Exception as e:
            self._logger.exception(e)

    def execute(
        self,
        name: str,
        fn: Callable[..., Any],
        args: Args = tuple(),
        kwargs: KwArgs | None = None,
        timeout: dt.timedelta = dt.timedelta(days=1),
    ) -> Result:
        if self._stopped:
            return Error(ExecutorStopError(name))

        with self._lock:
            assert name not in self._processes

        self._logger.debug(f"execute: {name}")

        try:
            out = self._manager.dict()

            kwargs = kwargs or {}
            # NOTE: We use multiprocessing.Process to be able to cancel(kill) running tasks.
            # With concurrent.futures.ProcessPoolExecutor we can only cancel tasks that have not yet started.
            p = multiprocessing.Process(target=wrap(fn, out), args=args, kwargs=kwargs)
            p.start()

            with self._lock:
                self._processes[name] = p

            p.join(timeout.total_seconds())

            if self._stopped:
                return Error(ExecutorStopError(name))

            if "result" in out:
                return out["result"]

            with self._lock:
                if name in self._processes:
                    return Error(ProcessTimeoutError(name, timeout))

            return Error(ProcessKillError(name))

        except BrokenPipeError as e:
            return Error(ExecutorStopError(name))
        except Exception as e:
            self._logger.exception(e)
            return Error(e)
        finally:
            self._kill(name)
