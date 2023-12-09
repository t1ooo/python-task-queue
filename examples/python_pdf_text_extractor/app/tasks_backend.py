from python_task_queue.task_queue import TaskQueue
from redis.client import Redis

from python_task_queue.broker import RedisBroker
from python_task_queue.storage import RedisStorage
from python_task_queue.worker import Worker

broker = RedisBroker(Redis("localhost"))
storage = RedisStorage(Redis("localhost"))

worker = Worker(broker, storage)
task_queue = TaskQueue(broker, storage, worker)
