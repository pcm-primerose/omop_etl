# infra/logging_setup.py
from __future__ import annotations
import json
import logging
import os
import sys
import multiprocessing as mp
import atexit
from pathlib import Path
from logging.config import dictConfig
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import Optional, Dict, Any, List

# global controller for multiprocessing logging
_mp_controller: Optional[MpLoggingController] = None

# track active file handlers for cleanup
_file_handlers: List[RotatingFileHandler] = []


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        payload = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # add extra fields if present
        if hasattr(record, "trial"):
            payload["trial"] = record.trial
        if hasattr(record, "run_id"):
            payload["run_id"] = record.run_id
        if hasattr(record, "timestamp"):
            payload["run_timestamp"] = record.timestamp

        # add any other extra fields
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "thread",
                "threadName",
                "exc_info",
                "exc_text",
                "stack_info",
            ):
                if key not in payload:
                    payload[key] = value

        # add exception info if present
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


class PlainFormatter(logging.Formatter):
    """Enhanced plain text formatter."""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s | %(levelname)-4s | %(name)-20s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class MpLoggingController:
    """Controller for multiprocessing logging setup."""

    def __init__(self, queue: mp.Queue, listener: QueueListener):
        self.queue = queue
        self.listener = listener

    def stop(self):
        """Stop the queue listener."""
        self.listener.stop()


def configure(
    *,
    level: Optional[int | str] = None,
    json_out: Optional[bool] = None,
    log_file: Optional[Path] = None,
) -> None:
    """
    Configure logging for the application.

    Args:
        level: Logging level (int or string like 'INFO', 'DEBUG')
        json_out: Whether to use JSON output format
        log_file: Optional path to log file
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")
    if isinstance(level, str):
        level = level.upper()

    if json_out is None:
        json_out = os.getenv("LOG_JSON", "1") == "1"

    # build config
    config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {"()": JsonFormatter},
            "plain": {"()": PlainFormatter},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "json" if json_out else "plain",
            },
        },
        "root": {
            "level": level,
            "handlers": ["console"],
        },
    }

    dictConfig(config)

    # add file handler if specified
    if log_file:
        add_file_handler(log_file, json_format=json_out, level=level)

    logging.getLogger(__name__).debug(
        "Logging configured",
        extra={
            "level": level,
            "json": json_out,
            "file": str(log_file) if log_file else None,
        },
    )


def add_file_handler(
    path: Path,
    *,
    max_bytes: int = 10_000_000,
    backup_count: int = 3,
    level: Optional[str | int] = None,
    json_format: Optional[bool] = None,
) -> RotatingFileHandler:
    """
    Add a rotating file handler to the root logger.

    Args:
        path: Path to log file
        max_bytes: Maximum bytes per file before rotation
        backup_count: Number of backup files to keep
        level: Optional logging level for this handler
        json_format: Whether to use JSON format (defaults to LOG_JSON env var)

    Returns:
        The created file handler
    """
    # ensure dir exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # create handler
    handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")

    # set formatter
    if json_format is None:
        json_format = os.getenv("LOG_JSON", "1") == "1"

    formatter = JsonFormatter() if json_format else PlainFormatter()
    handler.setFormatter(formatter)

    # set level if specified
    if level is not None:
        if isinstance(level, str):
            level = level.upper()
        handler.setLevel(level)

    # add to root logger
    logging.getLogger().addHandler(handler)

    # track for cleanup
    _file_handlers.append(handler)

    logging.getLogger(__name__).debug(f"Added file handler: {path}")

    return handler


def remove_file_handler(handler: RotatingFileHandler) -> None:
    logging.getLogger().removeHandler(handler)
    handler.close()

    if handler in _file_handlers:
        _file_handlers.remove(handler)


def cleanup_file_handlers() -> None:
    for handler in _file_handlers[:]:
        remove_file_handler(handler)


# register cleanup on exit
atexit.register(cleanup_file_handlers)


def start_mp_logging(
    ctx: Optional[mp.context.BaseContext] = None,
) -> tuple[mp.Queue, QueueListener]:
    """
    Set up multiprocessing logging in the parent process,
    call after configure() and before creating worker processes.

    Args:
        ctx: Multiprocessing context (uses default if None)

    Returns:
        Queue and listener for worker processes
    """
    global _mp_controller

    if _mp_controller is not None:
        return _mp_controller.queue, _mp_controller.listener

    queue = (ctx or mp).Queue(-1)
    root = logging.getLogger()

    # snapshot current handlers
    handlers = root.handlers[:]

    # create and start listener
    listener = QueueListener(queue, *handlers, respect_handler_level=True)
    listener.start()

    # ctore controller
    _mp_controller = MpLoggingController(queue, listener)
    atexit.register(_mp_controller.stop)

    logging.getLogger(__name__).debug("Multiprocessing logging started")

    return queue, listener


def configure_worker(queue: mp.Queue, *, level: Optional[str | int] = None) -> None:
    """
    Configure logging in a worker process,
    call at the start of each worker process.

    Args:
        queue: Queue from start_mp_logging()
        level: Optional logging level override
    """
    root = logging.getLogger()

    # clear existing handlers
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # add queue handler
    root.addHandler(QueueHandler(queue))

    # set level
    if level is not None:
        if isinstance(level, str):
            level = level.upper()
        root.setLevel(level)
    else:
        root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    logging.getLogger(__name__).debug("Worker logging configured")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    if name is None:
        # try to get caller's module name
        import inspect

        frame = inspect.currentframe()
        if frame and frame.f_back:
            name = frame.f_back.f_globals.get("__name__", __name__)
        else:
            name = __name__

    return logging.getLogger(name)
