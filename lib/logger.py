"""Helper functions to include JSON representation of a row of data in logs

"""
import json
import logging
from . import settings

streamhandler = logging.StreamHandler()

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[streamhandler],
)

logger = logging.getLogger()


def log(row, level, msg, *args):
    msg = msg + " %s "
    args = args + (json.dumps(row),)
    getattr(logger, level)(msg, *args)


def log_warning(row, msg, *args):
    return log(row, "warning", msg, *args)


def log_info(row, msg, *args):
    return log(row, "info", msg, *args)


def log_error(row, msg, *args):
    return log(row, "error", msg, *args)
