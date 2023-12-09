import pytest
from datetime import timedelta
from python_task_queue.task import Options


def test_options():
    Options(
        id="test",
        tries=1,
        tries_delay=timedelta(seconds=1),
        timeout=timedelta(minutes=1),
        ttl=timedelta(minutes=1),
        result_ttl=timedelta(minutes=1),
    )


def test_id():
    with pytest.raises(ValueError):
        Options(id="")


def test_tries():
    with pytest.raises(ValueError):
        Options(tries=0)

    with pytest.raises(ValueError):
        Options(tries=-1)


def test_tiomeout():
    with pytest.raises(ValueError):
        Options(timeout=timedelta(seconds=0))

    with pytest.raises(ValueError):
        Options(timeout=timedelta(seconds=-1))


def test_ttl():
    with pytest.raises(ValueError):
        Options(ttl=timedelta(seconds=0))

    with pytest.raises(ValueError):
        Options(ttl=timedelta(seconds=-1))


def test_result_ttl():
    with pytest.raises(ValueError):
        Options(result_ttl=timedelta(seconds=0))

    with pytest.raises(ValueError):
        Options(result_ttl=timedelta(seconds=-1))
