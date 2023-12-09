import pytest
from python_task_queue.worker import RunningTasks


def test_register():
    running_tasks = RunningTasks()

    assert not running_tasks.is_registered("test")
    assert running_tasks.register("test")
    assert running_tasks.is_registered("test")


def test_register_twice():
    running_tasks = RunningTasks()

    assert running_tasks.register("test")
    assert not running_tasks.register("test")


def test_remove():
    running_tasks = RunningTasks()

    assert running_tasks.register("test")
    running_tasks.remove("test")
    assert not running_tasks.is_registered("test")


def test_remove_twice():
    running_tasks = RunningTasks()

    assert running_tasks.register("test")
    running_tasks.remove("test")
    with pytest.raises(KeyError):
        assert running_tasks.remove("test")
