import json
import time
from functools import partial

from fonlaboristo.job import Job, PENDING, RUNNING
from fonlaboristo.conn import RedisClient
from fonlaboristo.task import Task
from fonlaboristo.utils import gen_job_id
from redis import Redis
import logging

logger = logging.getLogger(__name__)


class Queue:
    def __init__(self, name="default", task_timeout=600, fetch_timeout=5):
        self.name = name
        self.task_timeout = task_timeout
        self.fetch_timeout = fetch_timeout

    def task(self):
        return partial(Task, queue=self)

    def put(self, callback_module, callback_name, args, kwargs) -> Job:
        job_id = gen_job_id()
        redis = RedisClient().conn
        redis.hmset(
            f"job:{job_id}",
            {
                "callback_module": callback_module,
                "callback_name": callback_name,
                "input": json.dumps({"args": args, "kwargs": kwargs}),
                "state": PENDING,
            },
        )
        redis.lpush(f"queue:{self.name}:pending", job_id)

        return Job(id=job_id)

    def trim_in_progress(self):
        """monitors in the progress for items that are in there too long"""
        logger.info(f"retrying stuck jobs")
        redis = RedisClient().conn
        job_id = redis.lindex(f"queue:{self.name}:pending", -1)
        if job_id is None:
            return
        job = Job(job_id)
        if (
            job.state == RUNNING
            and time.time() - job.start_time > self.task_timeout
        ):
            logger.info(
                f"job {job_id} was stuck for {time.time() - job_status['start_time']} seconds, retrying"
            )
            pipe = redis.pipeline()
            pipe.lrem(f"queue:{self.name}:running")
            pipe.lpush(f"queue:{self.name}:pending", job_id)
            pipe.hset(f"job:{job_id}")
            pipe.execute()

    def get(self) -> Job:
        job_id = RedisClient().conn.brpoplpush(
            f"queue:{self.name}:pending",
            f"queue:{self.name}:running",
            self.fetch_timeout,
        )
        logger.info(f"popped job {job_id} of queue")
        if job_id is None:
            return None

        return Job(job_id)
