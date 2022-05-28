import logging
import os

FORMAT = "[%(levelname)8s] %(name)s: %(message)s"

DEBUG = 'DEBUG'

levels = [
    'CRITICAL',
    'ERROR',
    'WARNING',
    'INFO',
    'DEBUG',
    'NOTSET',
]


def init_logger():
    _init_root_logger()
    _init_project_logger()


def _init_root_logger():
    root_logger = logging.getLogger()
    root_logger.setLevel("ERROR")


def _init_project_logger():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(_get_format())
    handler.setFormatter(formatter)

    project_logger = logging.getLogger("iomete_jdbc_sync")
    project_logger.setLevel(_get_level())
    project_logger.addHandler(handler)


def _get_level():
    env_level = os.getenv("LOG_LEVEL")
    if env_level is not None and env_level in levels:
        return env_level.upper()
    return DEBUG


def _get_format():
    env_format = os.getenv("LOG_FORMAT")
    if env_format is not None:
        return env_format
    return FORMAT
