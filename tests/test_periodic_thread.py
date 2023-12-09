import time
import pytest
from datetime import timedelta
from unittest.mock import MagicMock
from python_task_queue.worker import PeriodicThread


@pytest.fixture
def thread():
    delay = timedelta(milliseconds=10)
    thread = PeriodicThread(delay)
    thread.run_next = MagicMock()
    return thread


def test_stop(thread: PeriodicThread):
    assert not thread._stopped

    thread.start()
    thread.join()

    assert not thread.is_alive()
    assert thread._stopped


def test_run_next(thread: PeriodicThread):
    n = 10

    thread.start()
    time.sleep((thread._delay * n).total_seconds())
    thread.join()

    assert thread.run_next.called
    assert thread.run_next.call_count in (n-2,n-1,n)
