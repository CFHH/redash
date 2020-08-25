from datetime import timedelta
from functools import partial

from flask import current_app
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys

from rq import get_current_job
from rq.decorators import job as rq_job

from redash import (
    create_app,
    extensions,
    settings,
    redis_connection,
    rq_redis_connection,
)

job = partial(rq_job, connection=rq_redis_connection)


class CurrentJobFilter(logging.Filter):
    def filter(self, record):
        current_job = get_current_job()

        record.job_id = current_job.id if current_job else ""
        record.job_func = current_job.func_name if current_job else ""

        return True


def get_job_logger(name):
    logger = logging.getLogger("rq.job#" + name)

    '''
    formatter = logging.Formatter(settings.RQ_WORKER_JOB_LOG_FORMAT)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(CurrentJobFilter())
    logger.addHandler(stream_handler)

    current_path = os.path.dirname(os.path.abspath(__file__))
    redash_root_path = os.path.join(current_path, os.pardir)
    log_path = os.path.join(redash_root_path, 'log')
    file_name = os.path.join(log_path, 'redash.rq.log')

    file_handler = TimedRotatingFileHandler(filename=file_name, when='D', interval=1, backupCount=15)
    file_handler.suffix = '%Y%m%d.log'
    file_handler.setFormatter(formatter)
    stream_handler.addFilter(CurrentJobFilter())
    logger.addHandler(file_handler)

    logger.propagate = False
    '''

    return logger
