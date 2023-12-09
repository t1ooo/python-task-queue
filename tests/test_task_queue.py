import pytest
from unittest.mock import MagicMock, patch
import datetime as dt
from python_task_queue.result import Error, Ok
from python_task_queue.shared import datetime_now
from python_task_queue.state import State
from python_task_queue.task import Options, Task
from python_task_queue.task_queue import CanceledError, ExpiredError, TaskQueue, TimeoutError


@pytest.fixture
def task_queue():
    broker = MagicMock()
    storage = MagicMock()
    worker = MagicMock()
    task_queue = TaskQueue(broker, storage, worker)
    task_queue.storage = MagicMock()
    return task_queue


def _test_fn():
    return 5


def test_submit(task_queue: TaskQueue):
    args = (1, 2, 3)
    kwargs = dict(a=1, b="test")
    opts = Options(id="test")

    now = dt.datetime(1, 1, 1)
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        assert datetime_now() == now

        task_id = task_queue.submit(_test_fn, args=args, kwargs=kwargs, opts=opts)
        assert task_id == "test"

        task = Task(_test_fn, args=args, kwargs=kwargs, opts=opts)

        task_queue.storage.set.assert_called_with(task.id, task, task.opts.result_ttl)
        task_queue.broker.enqueue.assert_called_with("test")


def test_close(task_queue: TaskQueue):
    task_queue.close()
    task_queue.broker.close.assert_called_with()
    task_queue.storage.close.assert_called_with()
    task_queue.worker.stop.assert_called_with()


def test_cancel(task_queue: TaskQueue):
    task = Task(_test_fn, opts=Options(id="test"))
    task_queue.storage.get.return_value = task

    task_queue.cancel("test")

    task_queue.storage.get.assert_called_with("test")
    task_queue.worker.kill_task.assert_called_with("test")
    task = task.update(state=State.CANCELLED)
    task_queue.storage.set.assert_called_with("test", task, task.opts.result_ttl)


def test_task(task_queue: TaskQueue):
    task_queue.task("test")
    task_queue.storage.get.assert_called_with("test")


def test_state(task_queue: TaskQueue):
    task_queue.state("test")
    task_queue.storage.get.assert_called_with("test")


def test_result(task_queue: TaskQueue):
    task_queue.worker.running_tasks.is_registered.return_value = False
    task_queue.storage.get.return_value = Task(_test_fn, result=Ok("result"))

    out = task_queue.result("test", dt.timedelta(milliseconds=1))
    assert out == Ok("result")
    task_queue.worker.running_tasks.is_registered.assert_called_with("test")
    task_queue.storage.get.assert_called_with("test")


def test_result_canceled(task_queue: TaskQueue):
    task_queue.worker.running_tasks.is_registered.return_value = False
    task_queue.storage.get.return_value = Task(
        _test_fn, result=Ok("result"), state=State.CANCELLED
    )

    out = task_queue.result("test", dt.timedelta(milliseconds=1))
    assert isinstance(out, Error)
    assert isinstance(out.value, CanceledError)


def test_result_expired(task_queue: TaskQueue):
    task_queue.worker.running_tasks.is_registered.return_value = False
    task_queue.storage.get.return_value = Task(
        _test_fn, result=Ok("result"), state=State.EXPIRED
    )

    out = task_queue.result("test", dt.timedelta(milliseconds=1))
    assert isinstance(out, Error)
    assert isinstance(out.value, ExpiredError)


def test_result_timeout(task_queue: TaskQueue):
    task_queue.worker.running_tasks.is_registered.return_value = False
    task_queue.storage.get.return_value = None

    out = task_queue.result("test", dt.timedelta(milliseconds=1))
    assert isinstance(out, Error)
    assert isinstance(out.value, TimeoutError)
