from multiprocessing.context import Process
import logging

logger = logging.getLogger(__name__)


def worker(queues):
    processes = [Process(target=_queue_worker, args=(queue,)) for queue in queues]
    for p in processes:

        p.start()

    for p in processes:
        p.join()


def _queue_worker(queue):
    logger.info(f"starting worker listening for jobs on queue {queue}")
    while True:
        queue.trim_in_progress()
        job = queue.get()
        if not job:
            continue

        job.run()
