import time
import pytest
from redis.client import Redis
from datetime import timedelta
from python_task_queue.peewee_db import peewee_sqlite_database
from python_task_queue.storage import PeeweeStorage, RedisStorage, SimpleStorage, Storage


def _test_existing(storage: Storage):
    key = "test_key"
    value = b"test_value"
    expire = timedelta(milliseconds=10)
    storage.set(key, value, expire)

    out = storage.get(key)
    assert out == value


def _test_non_existing(storage: Storage):
    key = "non_existing_key"
    out = storage.get(key)
    assert out is None


def _test_expired(storage: Storage):
    key1 = "expired_key1"
    value1 = b"expired_value1"
    expire1 = timedelta(milliseconds=10)

    key2 = "expired_key2"
    value2 = b"expired_value2"
    expire2 = timedelta(milliseconds=100)

    storage.set(key1, value1, expire1)
    storage.set(key2, value2, expire2)

    time.sleep(timedelta(milliseconds=1000).total_seconds())

    storage.remove_expired()

    out1 = storage.get(key1)
    assert out1 is None

    out2 = storage.get(key2)
    assert out2 is None


def test_simple_storage():
    storage = SimpleStorage()
    try:
        _test_existing(storage)
        _test_non_existing(storage)
        _test_expired(storage)
    finally:
        storage.close()


@pytest.mark.redis
def test_redis_storage():
    storage = RedisStorage(Redis("localhost"))
    try:
        _test_existing(storage)
        _test_non_existing(storage)
        _test_expired(storage)
    finally:
        storage.close()


@pytest.mark.peewee
def test_peewee_storage():
    storage = PeeweeStorage(
        peewee_sqlite_database("tests/test_data/peewee_test_storage.sqlite")
    )
    try:
        _test_existing(storage)
        _test_non_existing(storage)
        _test_expired(storage)
    finally:
        storage.close()
