import importlib
import json
import time

from fonlaboristo.conn import RedisClient


PENDING = "PENDING"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
FAILED = "FAILED"


class Job:
    def __init__(self, id):
        self.id = id
        self._cached_state = None

    def __getattr__(self, item):
        if self._cached_state is None:
            self._cached_state = RedisClient().conn.hgetall(f"job:{self.id}")
        return self._cached_state[item]

    def run(self):
        redis = RedisClient().conn

        redis.hmset(
            f"job:{self.id}", {"state": RUNNING, "start_time": int(time.time())}
        )

        callback = getattr(importlib.import_module(self.callback_module), self.callback_name)
        input = json.loads(self.input)
        try:
            result = callback.run(*input['args'], **input['kwargs'])
        except Exception as exc:
            redis.hmset(f"job:{self.id}", {"state": FAILED, "exception": str(exc)})
        else:
            redis.hmset(f"job:{self.id}", {"state": COMPLETED, "result": result})
