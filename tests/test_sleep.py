from datetime import timedelta
import math
import time
from python_task_queue.shared import sleep


def test_time():
    def f(delay, step):
        return sum(1 for _ in sleep(delay, step))

    data = [
        (10, timedelta(seconds=1), timedelta(milliseconds=100)),
        (2, timedelta(seconds=2), timedelta(seconds=1)),
        (1, timedelta(seconds=1), timedelta(seconds=5)),
    ]

    for count, delay, step in data:
        t1 = time.monotonic()
        n = f(delay, step)
        t2 = time.monotonic()
        assert count == n
        assert math.isclose(t2 - t1, delay.total_seconds(), abs_tol=0.01)


def test_time_single_iteration():
    delay = timedelta(seconds=1)

    steps = [
        timedelta(milliseconds=1000),
        timedelta(milliseconds=100),
        timedelta(milliseconds=10),
    ]

    for step in steps:
        t1 = time.monotonic()
        next(sleep(delay, step))
        t2 = time.monotonic()
        assert math.isclose(t2 - t1, step.total_seconds(), abs_tol=0.01)
