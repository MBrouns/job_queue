
import click
import logging

from fonlaboristo.bg_queue import Queue
from fonlaboristo.worker import worker

logger = logging.getLogger(__name__)


@click.group()
@click.option('--verbose', is_flag=True)
def main(verbose):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level)


@main.command()
def run_worker():
    q = Queue('default')
    worker([q])
