import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger for the given name.
    Log level is controlled by the LOG_LEVEL env var (default: INFO).

    Usage:
        from core.logger import get_logger
        logger = get_logger(__name__)
        logger.info("broker started")
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # already configured — return as-is to avoid duplicate handlers
        return logger

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # prevent log records from bubbling up to the root logger
    logger.propagate = False

    return logger
