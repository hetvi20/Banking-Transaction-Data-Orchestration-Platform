"""
utils/logger.py — Structured logging with loguru
"""
import sys
from loguru import logger as _logger


def get_logger(name: str):
    _logger.remove()
    _logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
        level="INFO",
    )
    _logger.add(
        "logs/pipeline.log",
        rotation="100 MB",
        retention="30 days",
        level="DEBUG",
    )
    return _logger.bind(name=name)
