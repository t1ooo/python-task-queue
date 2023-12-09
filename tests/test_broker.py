import random
import time
import pytest
from redis.client import Redis
import pika
from python_task_queue.broker import (
    Broker,
    PeeweeBroker,
    RabbitmqBroker,
    RedisBroker,
    SimpleBroker,
)
from python_task_queue.peewee_db import peewee_sqlite_database


def _test_en_de_done(broker: Broker, sleep: float = 0.0):
    broker.clear()

    broker.enqueue("test")
    time.sleep(sleep)
    assert broker.size() == 1

    # NOTE: call dequeue should return Message if a broker queue is not empty
    m = broker.dequeue()
    assert m is not None
    assert m.task_id == "test"
    time.sleep(sleep)
    assert broker.size() == 0

    # NOTE: call dequeue should return None if a broker queue not empty
    # call dequeue should decrement size
    assert broker.dequeue() is None
    assert broker.size() == 0

    # NOTE: call done should not change size
    broker.done(m)
    assert broker.size() == 0


def _test_clear(broker: Broker, sleep: float = 0.0):
    # NOTE: call clear should reset size
    # call dequeue after clear should return None
    broker.clear()
    broker.enqueue("test")
    time.sleep(sleep)
    assert broker.size() == 1
    broker.clear()
    assert broker.size() == 0
    assert broker.dequeue() is None


def _test_requeue(broker: Broker):
    # NOTE: after call requeue we should able to dequeue undone tasks
    broker.clear()
    broker.enqueue("test")
    assert broker.dequeue() is not None
    assert broker.dequeue() is None
    broker.requeue()
    assert broker.size() == 1
    assert broker.dequeue() is not None
    assert broker.size() == 0

    # NOTE: after call requeue we should not able to dequeue done tasks
    broker.clear()
    broker.enqueue("test")
    broker.requeue()
    m = broker.dequeue()
    broker.done(m)
    assert broker.dequeue() is None
    assert broker.size() == 0


def _test_close(broker: Broker):
    # NOTE: after call requeue we should able to dequeue undone tasks
    broker.clear()
    broker.enqueue("test")
    broker.close()
    # NOTE: dequeue should return None after close
    assert broker.dequeue() is None

def _test_random(broker: Broker, n: int, requeue: bool = True):
    broker.clear()

    m = None
    size = 0
    enqueue = 0
    done = 0
    ops: list[str] = []
    for _ in range(n):
        op = random.choice(["enqueue", "dequeue", "requeue", "done", "clear"])
        ops.append(op)
        match op:
            case "enqueue":
                broker.enqueue("test")
                size += 1
                enqueue += 1
            case "dequeue":
                m = broker.dequeue()
                if size == 0:
                    assert m is None, ops
                if m:
                    size -= 1
            case "requeue":
                broker.requeue()
                size = enqueue - done
                m = 0
            case "done":
                if m:
                    broker.done(m)
                    done += 1
                    m = None
            case "clear":
                broker.clear()
                m = None
                size = 0
                enqueue = 0
                done = 0
            case _:
                assert False

        assert size >= 0, ops
        assert broker.size() == size, ops


def test_simple_broker():
    broker = SimpleBroker()
    try:
        _test_en_de_done(broker)
        _test_clear(broker)
        _test_requeue(broker)
        _test_random(broker, n=100)
        _test_close(broker)
    finally:
        broker.close()


@pytest.mark.redis
def test_redis_broker():
    broker = RedisBroker(Redis("localhost"))
    try:
        _test_en_de_done(broker)
        _test_clear(broker)
        _test_requeue(broker)
        _test_random(broker, n=100)
        _test_close(broker)
    finally:
        broker.close()


@pytest.mark.peewee
def test_peewee_broker():
    broker = PeeweeBroker(
        peewee_sqlite_database("tests/test_data/peewee_test_broker.sqlite")
    )
    try:
        _test_en_de_done(broker)
        _test_clear(broker)
        _test_requeue(broker)
        _test_random(broker, n=100)
        _test_close(broker)
    finally:
        broker.close()


@pytest.mark.rabbitmq
def test_rabbitmq_broker():
    broker = RabbitmqBroker(pika.ConnectionParameters("localhost"))
    try:
        _test_en_de_done(broker, 0.1)
        _test_clear(broker, 0.1)
        _test_close(broker)
    finally:
        broker.close()
