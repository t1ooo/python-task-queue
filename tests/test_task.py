import pytest
from datetime import timedelta
from python_task_queue.shared import datetime_now
from python_task_queue.task import Options, Task


def test_is_expired():
    task = Task(
        fn=lambda: None,
        enqueued_at=datetime_now() - timedelta(seconds=1),
        opts=Options(ttl=timedelta(seconds=1)),
    )
    assert task.is_expired()

    task = Task(
        fn=lambda: None,
        enqueued_at=datetime_now(),
        opts=Options(ttl=timedelta(seconds=1)),
    )
    assert not task.is_expired()


def test_update():
    task = Task(fn=lambda: None)
    task2 = task.update()
    assert task == task2
    assert task is not task2
