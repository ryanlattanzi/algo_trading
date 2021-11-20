import logging
import sys


def main_logger(
    log_name: str,
    file_name: str = None,
    log_level: str = "info",
) -> logging.Logger:
    """Default settings for application logger. Automatically
    prints to stdout, but has the option to write to a file as well.

    Args:
        logger_name (str): Name of logger.
        file_name (str, optional): File to output. Defaults to None.

    Returns:
        logging.Logger: Logger object with default settings.
    """

    _log_levels = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }

    logger = logging.getLogger(log_name)
    logger.setLevel(_log_levels[log_level])

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(sh)

    if file_name:
        fh = logging.FileHandler(file_name)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def child_logger(log_name: str, child_name: str):
    return logging.getLogger(log_name).getChild(child_name)
