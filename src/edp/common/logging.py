"""Logging setup shared by every pipeline step."""

from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_HANDLER_MARKER = "_edp_handler"


def setup_logging(level: str = "INFO") -> None:
    """Configure root logging once. Safe to call repeatedly (no duplicate handlers)."""
    numeric = logging.getLevelName(level.upper())
    if not isinstance(numeric, int):
        raise ValueError(f"Invalid log level: {level!r}")
    root = logging.getLogger()
    root.setLevel(numeric)
    if not any(getattr(h, _HANDLER_MARKER, False) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        setattr(handler, _HANDLER_MARKER, True)
        root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger, e.g. ``get_logger(__name__)``."""
    return logging.getLogger(name)
