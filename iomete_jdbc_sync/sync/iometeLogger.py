import logging
import os

FORMAT = "%(asctime)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"

ERROR = 'ERROR'

levels = [
    'CRITICAL',
    'ERROR',
    'WARNING',
    'INFO',
    'DEBUG',
    'NOTSET',
]


class iometeLogger:
    def __init__(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.getLevelName(self.get_level()))
        handler = logging.StreamHandler()
        formatter = logging.Formatter(self.get_format())
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger

    def get_logger(self):
        return self.logger

    @staticmethod
    def get_level():
        env_level = os.getenv("LOG_LEVEL")
        if env_level is not None and env_level in levels:
            return env_level.upper()
        return ERROR

    @staticmethod
    def get_format():
        env_format = os.getenv("LOG_FORMAT")
        if env_format is not None:
            return env_format
        return FORMAT
