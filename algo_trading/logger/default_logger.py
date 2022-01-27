import logging
import sys
from typing import Tuple

from algo_trading.logger.controllers import LogLevelController, LogConfig


def get_main_logger(
    log_name: str,
    file_name: str,
    log_level: LogLevelController,
) -> Tuple[logging.Logger, LogConfig]:
    """Default settings for application logger. Automatically
    prints to stdout, but has the option to write to a file as well.
    Returns logger and LogConfig needed for downstream modules.

    Args:
        log_name (str): Name of main logger.
        file_name (str): File to write log.
        log_level (LogLevelController): Log level.

    Returns:
        Tuple[logging.Logger, LogConfig]: Log object and LogConfig object.
    """

    config = LogConfig(
        log_name=log_name,
        file_name=file_name,
        log_level=log_level,
    )

    _log_levels = {
        LogLevelController.info: logging.INFO,
        LogLevelController.debug: logging.DEBUG,
    }

    logger = logging.getLogger(config.log_name)
    logger.setLevel(_log_levels[config.log_level])

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(sh)

    if config.file_name:
        fh = logging.FileHandler(config.file_name)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger, config


def get_child_logger(log_name: str, child_name: str) -> logging.Logger:
    """Returns a child logger, which shows which module
    the log message is coming from. This consolidates all
    log messages into one file while still indicating
    where the code was called from.

    Args:
        log_name (str): Name of main log.
        child_name (str): Name of child logger.

    Returns:
        logging.Logger: Child log object.
    """
    return logging.getLogger(log_name).getChild(child_name)
