import multiprocessing
import threading
import time
import pytest
from datetime import timedelta
from python_task_queue.executor import ExecutorStopError, ProcessExecutor, ProcessKillError, ProcessTimeoutError, wrap
from python_task_queue.result import Error, Ok


def test_wrap():
    def f(a, b):
        return a + b

    out = {}
    wrapped_f = wrap(f, out)
    wrapped_f(1, 2)
    assert out["result"] == Ok(3)

    def f(a, b):
        raise Exception("error")

    out = {}
    wrapped_f = wrap(f, out)
    wrapped_f(1, 2)
    assert isinstance(out["result"], Error)
    assert type(out["result"].value) is Exception
    assert out["result"].value.args == ("error",)


def test_execute_ok():
    def f(a, b):
        return a + b

    executor = ProcessExecutor()
    out = executor.execute("test", f, args=(1, 2))
    assert out == Ok(3)


def test_execute_raise_exception():
    def f(a, b):
        raise Exception("error")

    executor = ProcessExecutor()
    out = executor.execute("test", f, args=(1, 2))
    assert isinstance(out, Error)
    assert type(out.value) is Exception
    assert out.value.args == ("error",)


def test_execute_timeout():
    def f():
        time.sleep(10)

    executor = ProcessExecutor()
    out = executor.execute("test", f, timeout=timedelta(milliseconds=10))
    assert out == Error(ProcessTimeoutError("test", timedelta(milliseconds=10)))


def test_execute_kill():
    def f(a, b):
        time.sleep(10)

    executor = ProcessExecutor()
    threading.Timer(0.1, lambda: executor.kill("test")).start()
    out = executor.execute("test", f, args=(1, 2), timeout=timedelta(seconds=2))
    assert out == Error(ProcessKillError("test"))


def test_execute_stop_executor():
    def f(a, b):
        time.sleep(10)

    executor = ProcessExecutor()
    threading.Timer(0.1, lambda: executor.stop()).start()
    out = executor.execute("test", f, args=(1, 2), timeout=timedelta(seconds=2))
    assert out == Error(ExecutorStopError("test"))


def test_execute_stopped_executor():
    def f(a, b):
        return a + b

    executor = ProcessExecutor()
    executor.stop()
    out = executor.execute("test", f, args=(1, 2))
    assert out == Error(ExecutorStopError("test"))


def test_stop():
    executor = ProcessExecutor()

    executor._processes["test_1"] = multiprocessing.Process()
    executor._processes["test_2"] = multiprocessing.Process()
    executor._processes["test_3"] = multiprocessing.Process()
    assert not executor._stopped

    executor.stop()
    assert executor._stopped
    assert len(executor._processes) == 0


def test_kill():
    executor = ProcessExecutor()

    executor._processes["test_1"] = multiprocessing.Process()
    executor._processes["test_2"] = multiprocessing.Process()

    assert executor.kill("test_1")

    # NOTE: should remove test_1 from processes, but not test_2
    assert "test_1" not in executor._processes
    assert "test_2" in executor._processes

    # NOTE: should return False for an already killed process
    assert not executor.kill("test_1")


def test_kill_nonexistent():
    executor = ProcessExecutor()

    # NOTE: should return False for a nonexistent process
    assert not executor.kill("nonexistent")
