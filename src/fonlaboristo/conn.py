import os

import redis


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class RedisClient(metaclass=Singleton):
    def __init__(self):
        self.pool = redis.ConnectionPool(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=os.environ.get("REDIS_PORT", 6379),
            password=os.environ.get("REDIS_PASSWORD", ""),
            decode_responses=True
        )

    @property
    def conn(self) -> redis.Redis:
        if not hasattr(self, "_conn"):
            self._get_connection()
        return self._conn

    def _get_connection(self):
        self._conn = redis.Redis(connection_pool=self.pool)
