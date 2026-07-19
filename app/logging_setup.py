"""Centralized logging configuration."""

from __future__ import annotations

import logging

_CONFIGURED = False
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logging once for the whole application."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    logging.basicConfig(level=level, format=LOG_FORMAT)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a named logger, ensuring basic config is applied."""
    setup_logging()
    return logging.getLogger(name)
