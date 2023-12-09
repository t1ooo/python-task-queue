import datetime as dt
import threading
from typing import Protocol
import peewee as pw
from redis.client import Redis

from .shared import datetime_now

from .task import Task
from .encoder import Encoder
from .logger import cls_logger
from .shared import build_key


class Storage(Protocol):
    def get(self, key: str) -> bytes | None:
        ...

    def set(self, key: str, value: bytes, expire: dt.timedelta):
        ...

    def close(self):
        ...

    def remove_expired(self):
        ...


class SimpleStorage(Storage):
    """For testing purposes only."""
    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, dt.datetime, dt.timedelta]] = {}
        self._datetime = datetime_now
        self._lock = threading.Lock()

    def get(self, key: str) -> bytes | None:
        try:
            with self._lock:
                return self._data[key][0]
        except KeyError:
            return None

    def set(self, key: str, value: bytes, expire: dt.timedelta):
        with self._lock:
            self._data[key] = (value, self._datetime(), expire)

    def close(self):
        pass

    def remove_expired(self):
        with self._lock:
            for key in list(self._data.keys()):
                _, dt, expire = self._data[key]
                if dt + expire < self._datetime():
                    self._data.pop(key, None)


class RedisStorage(Storage):
    # TODO: handle lost connection
    def __init__(self, client: Redis, key_prefix: str = "python_task_queue") -> None:
        self._client = client
        self._key_prefix = key_prefix

    def get(self, key: str) -> bytes | None:
        key = build_key(self._key_prefix, key)
        # NOTE: redis hash does not support the expiration time of the child key,
        # so we use simple key
        return self._client.get(key)

    def set(self, key: str, value: bytes, expire: dt.timedelta):
        key = build_key(self._key_prefix, key)
        self._client.set(key, value, px=expire)

    def close(self):
        pass

    def remove_expired(self):
        # NOTE: redis will do it for us
        pass


storage_database_proxy = pw.DatabaseProxy()


class KVModel(pw.Model):
    key = pw.TextField(primary_key=True)
    value = pw.BlobField()
    created = pw.DateTimeField()
    expired = pw.DateTimeField()

    class Meta:
        database = storage_database_proxy
        table_name = "KW"


class PeeweeStorage(Storage):
    def __init__(self, db: pw.SqliteDatabase, encoder: Encoder | None = None):
        storage_database_proxy.initialize(db)
        db.connect()
        db.create_tables([KVModel])
        self._db = db
        self._encoder = encoder or Encoder()
        self._logger = cls_logger(self)
        self._datetime = datetime_now

    def get(self, key: str) -> bytes | None:
        try:
            return KVModel.get_by_id(key).value
        except pw.DoesNotExist:
            return None

    def set(self, key: str, value: bytes, expire: dt.timedelta):
        created = self._datetime()
        expired = created + expire
        with self._db.atomic():
            KVModel.replace(
                key=key,
                value=value,
                created=created,
                expired=expired,
            ).execute()

    def remove_expired(self):
        dt = self._datetime()
        with self._db.atomic():
            KVModel.delete().where(KVModel.expired < dt).execute()

    def close(self):
        self._db.close()


class StorageWrapper:
    """Storage wrapper that encodes/decodes a value."""

    def __init__(self, storage: Storage, encoder: Encoder | None = None) -> None:
        self._storage = storage
        self._encoder = encoder or Encoder()
        self._logger = cls_logger(self)

    def get(self, task_id: str) -> Task | None:
        value = self._storage.get(task_id)
        task = self._encoder.decode(Task, value)
        return task

    def set(self, task_id: str, task: Task, expire: dt.timedelta):
        value = self._encoder.encode(task)
        self._storage.set(task_id, value, expire)

    def remove_expired(self):
        self._storage.remove_expired()

    def close(self):
        self._storage.close()
