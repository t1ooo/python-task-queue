from argparse import ArgumentParser
from datetime import timedelta
import time
from redis.client import Redis
import pika
import logging
from python_task_queue.task import Options
from python_task_queue.worker import Worker
from python_task_queue.task_queue import TaskQueue
from python_task_queue.peewee_db import peewee_sqlite_database
from python_task_queue.storage import PeeweeStorage, RedisStorage
from python_task_queue.broker import PeeweeBroker, RabbitmqBroker, RedisBroker

# setup logger
logging.basicConfig(level=logging.WARNING)
logging.getLogger("python_task_queue").setLevel(logging.INFO)


# define brokers builders
brokers = {
    "peewee-sqlite": lambda: PeeweeBroker(
        peewee_sqlite_database("data/pewee_broker.sqlite")
    ),
    "redis": lambda: RedisBroker(Redis("localhost")),
    "rabbitmq": lambda: RabbitmqBroker(pika.ConnectionParameters("localhost")),
}

# define storage builders
storages = {
    "peewee-sqlite": lambda: PeeweeStorage(
        peewee_sqlite_database("data/pewee_storage.sqlite")
    ),
    "redis": lambda: RedisStorage(Redis("localhost")),
}


# parse cmd arguments
def first(it):
    return next(iter(it))


ap = ArgumentParser()
ap.add_argument(
    "--broker",
    choices=brokers.keys(),
    default=first(brokers.keys()),
)
ap.add_argument(
    "--storage",
    choices=storages.keys(),
    default=first(storages.keys()),
)
args = ap.parse_args()


# define task functions
def hello(name: str):
    time.sleep(1)
    return f"hello, {name}"


def error(name: str):
    time.sleep(1)
    raise Exception(f"error, {name}")


def timeout(name: str):
    time.sleep(10)
    return "timeout"


def kill(name: str):
    time.sleep(10)
    return "kill"


# setup and run TaskQueue
print("broker:", args.broker)
print("storage:", args.storage)
broker = brokers[args.broker]()
storage = storages[args.storage]()

broker.clear()
worker = Worker(broker, storage)
task_queue = TaskQueue(broker, storage, worker)

task_queue.worker.start(block=False)


try:
    # submit some tasks to TaskQueue
    task_id_hello = task_queue.submit(
        hello,
        args=("name",),
        opts=Options(
            id="hello",
            tries=3,
            tries_delay=timedelta(seconds=1),
            timeout=timedelta(minutes=1),
            ttl=timedelta(days=1),
            result_ttl=timedelta(days=1),
        ),
    )
    task_id_error = task_queue.submit(
        error,
        args=("name",),
        opts=Options(timeout=timedelta(seconds=1)),
    )
    task_id_tout = task_queue.submit(
        timeout, args=("name",), opts=Options(timeout=timedelta(seconds=1))
    )
    task_id_kill = task_queue.submit(kill, args=("name",))

    # print task ids
    print("task_id_hello", task_id_hello)
    print("task_id_error", task_id_error)
    print("task_id_tout", task_id_tout)
    print("task_id_kill", task_id_kill)

    time.sleep(1)
    # cancel one of tasks
    print(task_queue.cancel(task_id_kill))

    # print task results with timeout
    print("result", task_queue.result(task_id_hello, timeout=timedelta(seconds=5)))
    print("result", task_queue.result(task_id_error, timeout=timedelta(seconds=5)))
    print("result", task_queue.result(task_id_tout, timeout=timedelta(seconds=5)))
    print("result", task_queue.result(task_id_kill, timeout=timedelta(seconds=5)))
finally:
    # close TaskQueue
    task_queue.close()
