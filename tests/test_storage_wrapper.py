from unittest.mock import MagicMock
import pytest
from datetime import timedelta
from python_task_queue.storage import StorageWrapper
from python_task_queue.task import Task


@pytest.fixture
def storage():
    storage = MagicMock()
    encoder = MagicMock()
    return StorageWrapper(storage, encoder)


def test_set(storage: StorageWrapper):
    key = "test"
    value = b"value"
    task = Task(fn=lambda: None)

    storage._storage.get.return_value = value

    storage._encoder.decode.return_value = task

    assert storage.get("test") == task
    storage._storage.get.assert_called_once_with(key)
    storage._encoder.decode.assert_called_once_with(type(task), value)


def test_get(storage: StorageWrapper):
    key = "test"
    value = b"value"
    task = Task(fn=lambda: None)
    expire = timedelta(seconds=1)

    storage._encoder.encode.return_value = value

    storage.set(key, task, expire)

    storage._encoder.encode.assert_called_once_with(task)
    storage._storage.set.assert_called_once_with(key, value, expire)


def test_remove_expired(storage: StorageWrapper):
    storage.remove_expired()
    storage._storage.remove_expired.assert_called_once_with()


def test_close(storage: StorageWrapper):
    storage.close()
    storage._storage.close.assert_called_once_with()
