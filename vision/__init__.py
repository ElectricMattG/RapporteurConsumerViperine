"""Init for module level config

Define module level config in a module namespace.
Importing the package will result in the config
being accessable within the audio_frontend
namespace: audio_frontend.app_conf, in addition
processing scripts are generated for the
entrypoint.sh file.

"""

import logging
import os
import sys
from typing import Optional

from pyconfs import Configuration


# config class for wrapping and accessing the config dictionary
class CustConf(Configuration):
    @staticmethod
    def init_logger(self, name: str, log_lib: logging) -> Optional[None]:
        """Initialize a logger with the logging configuration defined in the
        configuration dictionary.

        Args:
            name: The name of the logger.
            log_lib: The logging library to use.
        """
        root_logger = log_lib.getLogger(name)
        root_logger.handlers = []

        root_handler = logging.StreamHandler(stream=sys.stdout)
        root_handler.setFormatter(
            logging.Formatter(fmt=self._app_config.get("logging.format"))
        )
        root_logger.addHandler(root_handler)

        file_handler = logging.FileHandler(
            self._app_config.get("logging.output_full_path")
        )
        file_handler.setFormatter(
            logging.Formatter(fmt=self._app_config.get("logging.format"))
        )
        root_logger.addHandler(file_handler)

        root_logger.setLevel(self._app_config.get("logging.level"))
        return root_logger

    def get(self, key: str) -> Optional[None]:
        """Get the value corresponding to the given key string.

        Args:
            key (str): A string in the form "first_level_key.second_level_key".
        """
        return self.data[key.split(".")[0]][key.split(".")[1]]


# config dict (expand as needed)
defaults = {
    "admin": {
        "admin": "admin",
    },
    "logging": {
        "format": "| %(asctime)s | %(levelname)s - %(name)-10s - %(message)s",
        "level": 10,
        "output_full_path": "./logs/logfile.log",
    },
}

# config object for reuse
app_conf = CustConf.from_dict(defaults)

# working directory
work_dir = "./working_dir"
log_dir = "./logs"

# build directory
os.makedirs(work_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)

# init log file
log_filepath = app_conf.get("logging.output_full_path")
with open(log_filepath, "w") as fp:
    fp.write("--logs--")
