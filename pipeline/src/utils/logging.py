"""
Structured JSON logging — Wave 1 artifact.
All agents import get_logger() from here for consistent log formatting.
"""
import logging
import sys
from pythonjsonlogger import jsonlogger


def get_logger(name: str) -> logging.Logger:
    """Return a logger that emits JSON lines to stdout."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
