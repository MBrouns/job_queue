import logging

logger = logging.getLogger(__name__)


class Task:
    def __init__(self, func, *, queue):
        self.queue = queue
        self.func = func

    @property
    def _func_name(self):
        return f"{self.func.__module__}.{self.func.__qualname__}"

    def __call__(self, *args, **kwargs):
        logger.info(f"task {self._func_name} called")
        return self.queue.put(self.func.__module__, self.func.__qualname__, args=args, kwargs=kwargs)

    def run(self, *args, **kwargs):
        return self.func(*args, **kwargs)

