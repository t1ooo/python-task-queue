import pytest
import datetime as dt
from datetime import timedelta
from unittest.mock import MagicMock, call, patch
from python_task_queue.broker import Broker, Message
from python_task_queue.executor import ExecutorStopError, ProcessExecutor, ProcessKillError
from python_task_queue.result import Error, Result
from python_task_queue.shared import datetime_now
from python_task_queue.state import State
from python_task_queue.storage import StorageWrapper
from python_task_queue.task import Options, Task
from python_task_queue.worker import ConsumerWorker, RunningTasks


@pytest.fixture
def worker():
    return ConsumerWorker(
        broker=MagicMock(spec=Broker),
        storage=MagicMock(spec=StorageWrapper),
        executor=MagicMock(spec=ProcessExecutor),
        polling_delay=timedelta(seconds=1),
        running_tasks=MagicMock(spec=RunningTasks),
    )


def test_on_already_running(worker: ConsumerWorker):
    message = Message("test")

    worker._on_already_running(message)

    worker._broker.assert_not_called()
    worker._storage.assert_not_called()


def test_on_not_found(worker: ConsumerWorker):
    message = Message("test")

    worker._on_not_found(message)

    worker._broker.done.assert_called_with(message)
    worker._storage.assert_not_called()


def test_on_bad_state(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None, result=Result("result"), opts=Options(id="test"))

    worker._on_bad_state(message, task)

    worker._broker.assert_not_called()
    worker._storage.assert_not_called()


def test_on_expired(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None, opts=Options(id="test"))

    worker._on_expired(message, task)

    worker._broker.done.assert_called_with(message)
    worker._storage.set.assert_called_with(
        task.id, task.update(state=State.EXPIRED), task.opts.result_ttl
    )


def test_on_execute(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None, opts=Options(id="test"))
    result = Result("result")
    worker._execute = MagicMock(return_value=result)

    now = dt.datetime(1, 1, 1)
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        assert datetime_now() == now

        worker._on_execute(message, task)

        assert worker._storage.set.call_count == 2

        task = task.update(state=State.RUNNING)
        assert worker._storage.set.call_args_list[0] == call(
            task.id, task, task.opts.result_ttl
        )

        task = task.update(
            started_at=now, ended_at=now, result=result, state=State.SUCCESS
        )
        assert worker._storage.set.call_args_list[1] == call(
            task.id, task, task.opts.result_ttl
        )

        worker._broker.done.assert_called_with(message)


def test_on_execute_stop_error(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None, opts=Options(id="test"))
    result = Error(ExecutorStopError("test"))
    worker._execute = MagicMock(return_value=result)

    worker._on_execute(message, task)

    assert worker._storage.set.call_count == 1

    task = task.update(state=State.RUNNING)
    assert worker._storage.set.call_args_list[0] == call(
        task.id, task, task.opts.result_ttl
    )

    worker._broker.done.assert_not_called()


def test_on_execute_kill_error(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None, opts=Options(id="test"))
    result = Error(ProcessKillError("test"))
    worker._execute = MagicMock(return_value=result)

    now = dt.datetime(1, 1, 1)
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        assert datetime_now() == now

        worker._on_execute(message, task)

        assert worker._storage.set.call_count == 2

        task = task.update(state=State.RUNNING)
        assert worker._storage.set.call_args_list[0] == call(
            task.id, task, task.opts.result_ttl
        )

        task = task.update(
            started_at=now, ended_at=now, result=result, state=State.CANCELLED
        )
        assert worker._storage.set.call_args_list[1] == call(
            task.id, task, task.opts.result_ttl
        )

        worker._broker.done.assert_called_with(message)


def test_run_next_already_running(worker: ConsumerWorker):
    message = Message("test")
    worker._broker.dequeue.return_value = message
    worker._running_tasks.register.return_value = False
    worker._on_already_running = MagicMock()

    worker.run_next()

    worker._on_already_running.assert_called_with(message)


def test_run_next_task_not_found(worker: ConsumerWorker):
    message = Message("test")
    worker._broker.dequeue.return_value = message
    worker._running_tasks.register.return_value = True
    worker._storage.get.return_value = None
    worker._on_not_found = MagicMock()

    worker.run_next()

    worker._on_not_found.assert_called_with(message)


def test_run_next_bad_state(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None, state=State.SUCCESS)

    worker._broker.dequeue.return_value = message
    worker._running_tasks.register.return_value = True
    worker._storage.get.return_value = task
    worker._on_bad_state = MagicMock()

    worker.run_next()

    worker._on_bad_state.assert_called_with(message, task)


def test_run_next_expired(worker: ConsumerWorker):
    message = Message("test")
    opts = MagicMock(spec=Options)
    opts.ttl = -dt.timedelta(seconds=1)
    task = Task(fn=lambda: None, opts=opts)
    assert task.is_expired()

    worker._broker.dequeue.return_value = message
    worker._running_tasks.register.return_value = True
    worker._storage.get.return_value = task
    worker._on_expired = MagicMock()

    worker.run_next()

    worker._on_expired.assert_called_with(message, task)


def test_run_next_execute(worker: ConsumerWorker):
    message = Message("test")
    task = Task(fn=lambda: None)

    worker._broker.dequeue.return_value = message
    worker._running_tasks.register.return_value = True
    worker._storage.get.return_value = task
    worker._on_execute = MagicMock()

    worker.run_next()

    worker._on_execute.assert_called_with(message, task)
