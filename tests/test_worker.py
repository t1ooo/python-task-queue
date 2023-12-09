from unittest.mock import MagicMock
import pytest
from datetime import timedelta
from python_task_queue.worker import ConsumerWorker, DeadMessagesWorker, ExpiredResultsWorker, Worker


def test_worker():
    workers_count = 2
    polling_delay = timedelta(milliseconds=100)
    failed_polling_delay = timedelta(minutes=1)
    expired_polling_delay = timedelta(minutes=10)

    worker = Worker(
        broker=MagicMock(),
        storage=MagicMock(),
        workers_count=workers_count,
        polling_delay=polling_delay,
        failed_polling_delay=failed_polling_delay,
        expired_polling_delay=expired_polling_delay,
    )

    # NOTE: check workers
    assert len(worker._workers) == workers_count + 2

    i = 0
    for i in range(workers_count):
        assert isinstance(worker._workers[i], ConsumerWorker)
        assert worker._workers[i]._delay == polling_delay

    i += 1
    assert isinstance(worker._workers[i], DeadMessagesWorker)
    assert worker._workers[i]._delay == failed_polling_delay

    i += 1
    assert isinstance(worker._workers[i], ExpiredResultsWorker)
    assert worker._workers[i]._delay == expired_polling_delay

    worker._workers = [MagicMock() for _ in worker._workers]
    worker._storage = MagicMock()
    worker._executor = MagicMock()
    worker.running_tasks = MagicMock()

    # NOTE: start
    worker.start(block=False)
    for w in worker._workers:
        w.start.assert_called_once_with()

    # NOTE: stop
    worker.stop()
    worker._executor.stop.assert_called_once_with()
    for w in worker._workers:
        w.join.assert_called_once_with()


def test_stop():
    workers_count = 2
    polling_delay = timedelta(milliseconds=100)
    failed_polling_delay = timedelta(minutes=1)
    expired_polling_delay = timedelta(minutes=10)

    worker = Worker(
        broker=MagicMock(),
        storage=MagicMock(),
        workers_count=workers_count,
        polling_delay=polling_delay,
        failed_polling_delay=failed_polling_delay,
        expired_polling_delay=expired_polling_delay,
    )
    worker.stop()
    worker.stop()