"""Structured JSON-line logging with run_id support."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
import sys
from typing import Any, Dict, Optional

_DEFAULT_RUN_ID = os.environ.get("RUN_ID", "default-run")
_CURRENT_RUN_ID: str = _DEFAULT_RUN_ID


def set_run_id(run_id: str) -> None:
    """Set global run_id for logs."""
    global _CURRENT_RUN_ID
    _CURRENT_RUN_ID = run_id


def get_run_id() -> str:
    """Get current run_id."""
    return _CURRENT_RUN_ID


class JsonFormatter(logging.Formatter):
    """Formatter that outputs JSON lines."""

    def __init__(self, run_id: Optional[str] = None):
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", self.run_id or get_run_id()),
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Include custom extra fields if provided
        standard_attrs = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "run_id"
        }
        for k, v in record.__dict__.items():
            if k not in standard_attrs and not k.startswith("_"):
                log_entry[k] = v

        return json.dumps(log_entry, default=str)


def setup_logging(
    run_id: Optional[str] = None,
    level: str | int = "INFO",
    stream: Optional[Any] = None,
) -> logging.Logger:
    """Configure root logger with JSON formatting."""
    if run_id:
        set_run_id(run_id)

    root = logging.getLogger()
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter(run_id=get_run_id()))
    root.addHandler(handler)

    return logging.getLogger("qoe")


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured for the application."""
    return logging.getLogger(f"qoe.{name}" if not name.startswith("qoe") else name)
