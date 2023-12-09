from abc import abstractmethod
import dataclasses as dc
import threading
import time
from typing import Any, Protocol
import peewee as pw
import pika
import pika.spec
import pika.frame
from redis.client import Redis
import tenacity as tn

from .shared import checked_cast
from .encoder import Encoder
from .logger import cls_logger
from .shared import build_key


def gen_delivery_tag() -> int:
    return time.time_ns()


@dc.dataclass(frozen=True)
class Message:
    task_id: str
    delivery_tag: int = dc.field(default_factory=gen_delivery_tag)


class Broker(Protocol):
    def enqueue(self, task_id: str):
        ...

    def dequeue(self) -> Message | None:
        ...

    def done(self, message: Message):
        ...

    # TODO: pass a list of already running tasks to filter them and not add them to the queue again
    def requeue(self):
        """Requeue failed tasks."""
        ...

    def close(self):
        ...

    def size(self) -> int:
        ...

    def clear(self):
        ...


class SimpleBroker(Broker):
    """For testing purposes only."""
    def __init__(self) -> None:
        # NOTE: We want to remove an item from the queue,
        # so we use a simple list instead of a queue.Queue.
        self._queue: list[Message] = []
        self._processing: list[Message] = []
        self._closed = False
        self._lock = threading.Lock()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._close()

    def _close(self):
        pass

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def enqueue(self, task_id: str):
        message = Message(task_id)
        with self._lock:
            self._queue.append(message)

    def dequeue(self) -> Message | None:
        if self._closed:
            return None
        with self._lock:
            if len(self._queue) == 0:
                return None
            message = self._queue.pop(0)
            assert message not in self._processing
            self._processing.append(message)
            return message

    def done(self, message: Message):
        with self._lock:
            try:
                self._processing.remove(message)
            except ValueError:
                pass
            # NOTE: requeue requeue all tasks from the processing queue,
            # including the tasks currently being processed.
            # So we are trying to remove the message from the main queue too.
            try:
                self._queue.remove(message)
            except ValueError:
                pass

    def requeue(self):
        with self._lock:
            for m in self._processing:
                self._queue.append(m)
            self._processing.clear()

    def clear(self):
        with self._lock:
            self._queue.clear()
            self._processing.clear()


class RedisBroker:
    def __init__(
        self,
        client: Redis,
        encoder: Encoder | None = None,
        key_prefix: str = "python_task_queue",
        close_connections: bool = True,
    ) -> None:
        self._client = client
        self._queue_key = build_key(key_prefix, "queue")
        self._processing_queue_key = build_key(key_prefix, "processing_queue")
        self._encoder = encoder or Encoder()
        self._close_connections = close_connections
        self._logger = cls_logger(self)
        self._closed = False
        self._lock = threading.Lock()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._close()

    def _close(self):
        if self._close_connections:
            self._client.close()

    def size(self) -> int:
        with self._lock:
            return self._client.llen(self._queue_key)

    def enqueue(self, task_id: str):
        message = Message(task_id)
        value = self._encoder.encode(message)
        with self._lock:
            self._client.lpush(self._queue_key, value)

    def dequeue(self) -> Message | None:
        if self._closed:
            return None

        with self._lock:
            value = self._client.rpoplpush(self._queue_key, self._processing_queue_key)
            return self._encoder.decode(Message, value)

    def done(self, message: Message):
        value = self._encoder.encode(message)
        with self._lock:
            r1 = self._client.lrem(self._processing_queue_key, 0, value)
            # NOTE: requeue requeue all tasks from the processing queue,
            # including the tasks currently being processed.
            # So we are trying to remove the message from the main queue too.
            r2 = self._client.lrem(self._queue_key, 0, value)
            assert 1 == r1 + r2

    def requeue(self):
        with self._lock:
            while True:
                r = self._client.rpoplpush(self._processing_queue_key, self._queue_key)
                if r is None:
                    break

    def clear(self):
        with self._lock:
            self._client.delete(self._queue_key, self._processing_queue_key)


class RabbitmqBroker(Broker):
    def __init__(
        self, params: pika.ConnectionParameters, queue_name: str = "python_task_queue"
    ) -> None:
        self._params = params
        self._queue_name = queue_name
        self._closed = False
        self._lock = threading.Lock()
        self._logger = cls_logger(self)

        self._connect()

    def _connect(self):
        self._logger.info("connect")
        with self._lock:
            self.connection = pika.BlockingConnection(self._params)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self._queue_name)
        self._logger.info("connected")

    def _reconnect(self):
        if self._closed:
            return
        self._logger.info("reconnect")
        self._close()
        self._connect()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._close()

    def _close(self):
        self._logger.info("close")
        try:
            with self._lock:
                if self.channel.is_open:
                    self.channel.cancel()
                    self.channel.close()
                if self.connection.is_open:
                    self.connection.close()
        except Exception as e:
            self._logger.exception(e)
        self._logger.info("closed")

    def size(self) -> int:
        def f() -> pika.frame.Method:
            with self._lock:
                return self.channel.queue_declare(queue=self._queue_name, passive=True)

        method = self._reconnect_retrier()(f)
        return int(method.method.message_count)

    def enqueue(self, task_id: str):
        def f():
            with self._lock:
                self.channel.basic_publish(
                    exchange="", routing_key=self._queue_name, body=task_id
                )

        self._reconnect_retrier()(f)

    def dequeue(self) -> Message | None:
        if self._closed:
            return None

        def f() -> tuple[Any, Any, Any]:
            with self._lock:
                return self.channel.basic_get(queue=self._queue_name)

        method, _, body = self._reconnect_retrier()(f)

        if method is None:
            return None

        method = checked_cast(pika.spec.Basic.GetOk, method)

        if type(body) is str:
            task_id = body
        elif type(body) is bytes:
            task_id = body.decode()
        else:
            # if not isinstance(body, str):
            raise Exception("invalid body type", type(body))

        return Message(
            task_id,
            method.delivery_tag,
            # method.redelivered,
        )

    def done(self, message: Message):
        def f():
            with self._lock:
                self.channel.basic_ack(message.delivery_tag)

        self._reconnect_retrier()(f)

    def clear(self):
        def f():
            with self._lock:
                self.channel.queue_purge(queue=self._queue_name)

        self._reconnect_retrier()(f)

    def requeue(self):
        # NOTE: rabbitmq will do it for us
        pass

    def _reconnect_retrier(self) -> tn.Retrying:
        def after(state: tn.RetryCallState):
            self._logger.warning(state)
            self._reconnect()

        def is_closed(state: tn.RetryCallState):
            return self._closed

        return tn.Retrying(
            stop=(tn.stop_after_attempt(3) | is_closed),
            wait=tn.wait_fixed(1),
            retry=tn.retry_if_exception_type(Exception),
            reraise=True,
            after=after,
        )


broker_database_proxy = pw.DatabaseProxy()


class MessageModel(pw.Model):
    delivery_tag = pw.IntegerField(primary_key=True)
    task_id = pw.TextField()

    class Meta:
        database = broker_database_proxy
        table_name = "queue"

    def to_dict(self):
        return dict(delivery_tag=self.delivery_tag, task_id=self.task_id)


class ProcessingMessageModel(MessageModel):
    class Meta:
        table_name = "processing_queue"


class PeeweeBroker(Broker):
    def __init__(self, db: pw.SqliteDatabase, encoder: Encoder | None = None):
        broker_database_proxy.initialize(db)
        db.connect()
        db.create_tables([MessageModel, ProcessingMessageModel])
        self._db = db
        self._encoder = encoder or Encoder()
        self._closed = False
        self._logger = cls_logger(self)

    def enqueue(self, task_id: str):
        MessageModel.create(delivery_tag=gen_delivery_tag(), task_id=task_id)

    def dequeue(self) -> Message | None:
        if self._closed:
            return None

        with self._db.atomic():
            message = MessageModel.get_or_none()
            if message is None:
                return None
            d = message.to_dict()
            ProcessingMessageModel.create(**d)
            message.delete_instance()
            return Message(**d)

    def size(self) -> int:
        with self._db.atomic():
            return MessageModel.select().count()

    def done(self, message: Message):
        with self._db.atomic():
            r1 = ProcessingMessageModel.delete_by_id(message.delivery_tag)
            r2 = MessageModel.delete_by_id(message.delivery_tag)
            assert 1 == r1 + r2

    def requeue(self):
        with self._db.atomic():
            MessageModel.insert_from(
                ProcessingMessageModel.select(),
                (MessageModel.delivery_tag, MessageModel.task_id),
            ).execute()
            ProcessingMessageModel.delete().execute()

    def clear(self):
        with self._db.atomic():
            MessageModel.delete().execute()
            ProcessingMessageModel.delete().execute()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._close()

    def _close(self):
        self._db.close()
