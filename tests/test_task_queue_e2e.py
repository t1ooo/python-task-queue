from datetime import timedelta
import time
import pytest
from redis.client import Redis
import pika
from python_task_queue.broker import (
    PeeweeBroker,
    RabbitmqBroker,
    RedisBroker,
    SimpleBroker,
)
from python_task_queue.peewee_db import peewee_sqlite_database
from python_task_queue.result import Error, Ok
from python_task_queue.state import State
from python_task_queue.storage import PeeweeStorage, RedisStorage, SimpleStorage
from python_task_queue.task import Options, Task
from python_task_queue.task_queue import CanceledError, TaskQueue, TimeoutError
from python_task_queue.worker import Worker

delay = 0.1


def ok(name: str):
    time.sleep(delay)
    return f"ok, {name}"


def error(name: str):
    time.sleep(delay)
    raise Exception(f"error, {name}")


def timeout(name: str):
    time.sleep(delay * 100)
    return "timeout"


def cancel(name: str):
    time.sleep(delay * 100)
    return "cancel"


def _test_ok(task_queue: TaskQueue):
    task_id = "ok"
    task = Task(ok, args=(task_id,), opts=Options(id=task_id))
    task_queue.submit(task.fn, args=task.args, opts=task.opts)

    out_task = task_queue.task(task_id)
    assert out_task is not None
    assert out_task.id == task.id
    assert task_queue.state(task_id) == State.PENDING
    result = task_queue.result(task_id, timedelta(seconds=delay * 100))
    assert result == Ok(f"ok, {task_id}")


def _test_error(task_queue: TaskQueue):
    task_id = "error"
    task = Task(error, args=(task_id,), opts=Options(id=task_id))
    task_queue.submit(task.fn, args=task.args, opts=task.opts)

    out_task = task_queue.task(task_id)
    assert out_task is not None
    assert out_task.id == task.id
    assert task_queue.state(task_id) == State.PENDING
    result = task_queue.result(task_id, timedelta(seconds=delay * 100))
    assert isinstance(result, Error)
    assert isinstance(result.value, Exception)
    assert result.value.args == (f"error, {task_id}",)


def _test_timeout(task_queue: TaskQueue):
    task_id = "timeout"
    task = Task(error, args=(task_id,), opts=Options(id=task_id))
    task_queue.submit(task.fn, args=task.args, opts=task.opts)

    out_task = task_queue.task(task_id)
    assert out_task is not None
    assert out_task.id == task.id
    assert task_queue.state(task_id) == State.PENDING
    result = task_queue.result(task_id, timedelta(seconds=delay * 1))
    assert isinstance(result, Error)
    assert isinstance(result.value, TimeoutError)


def _test_cancel(task_queue: TaskQueue):
    task_id = "timeout"
    task = Task(error, args=(task_id,), opts=Options(id=task_id))
    task_queue.submit(task.fn, args=task.args, opts=task.opts)
    task_queue.cancel(task_id)

    out_task = task_queue.task(task_id)
    assert out_task is not None
    assert out_task.id == task.id
    assert task_queue.state(task_id) == State.CANCELLED
    result = task_queue.result(task_id, timedelta(seconds=delay * 1))
    assert isinstance(result, Error)
    assert isinstance(result.value, CanceledError)


def test_simple():
    broker = SimpleBroker()
    storage = SimpleStorage()
    worker = Worker(broker, storage)
    task_queue = TaskQueue(broker, storage, worker)
    task_queue.worker.start(block=False)
    try:
        _test_ok(task_queue)
        _test_error(task_queue)
        _test_timeout(task_queue)
        _test_cancel(task_queue)
    finally:
        task_queue.close()


@pytest.mark.redis
def test_redis():
    broker = RedisBroker(Redis("localhost"))
    storage = RedisStorage(Redis("localhost"))
    worker = Worker(broker, storage)
    task_queue = TaskQueue(broker, storage, worker)
    task_queue.worker.start(block=False)
    try:
        _test_ok(task_queue)
        _test_error(task_queue)
        _test_timeout(task_queue)
        _test_cancel(task_queue)
    finally:
        task_queue.close()


@pytest.mark.peewee
def test_peewee():
    broker = PeeweeBroker(
        peewee_sqlite_database("tests/test_data/peewee_test_broker.sqlite")
    )
    storage = PeeweeStorage(
        peewee_sqlite_database("tests/test_data/peewee_test_storage.sqlite")
    )
    worker = Worker(broker, storage)
    task_queue = TaskQueue(broker, storage, worker)
    task_queue.worker.start(block=False)
    try:
        _test_ok(task_queue)
        _test_error(task_queue)
        _test_timeout(task_queue)
        _test_cancel(task_queue)
    finally:
        task_queue.close()


@pytest.mark.rabbitmq
def test_rabbitmq():
    broker = RabbitmqBroker(pika.ConnectionParameters("localhost"))
    storage = SimpleStorage()
    worker = Worker(broker, storage)
    task_queue = TaskQueue(broker, storage, worker)
    task_queue.worker.start(block=False)
    try:
        _test_ok(task_queue)
        _test_error(task_queue)
        _test_timeout(task_queue)
        _test_cancel(task_queue)
    finally:
        task_queue.close()
