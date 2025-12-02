"""
config is inherited through sub-classing Base.
"""
import logging

from pyconfs import Configuration
from service.config import app_conf as config


class Base:
    """
    base classes for config and logging
    """

    def __init__(self, conf: Configuration = config):
        self._app_config = conf
        self._logger = conf.init_logger(
            self, name=self.__class__.__name__,
        )
