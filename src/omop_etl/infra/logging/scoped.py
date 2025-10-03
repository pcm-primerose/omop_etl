from contextlib import contextmanager
from logging import Logger, getLogger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterator, Optional

from .logging_setup import add_file_handler, remove_file_handler


@contextmanager
def file_logging(
    path: Path,
    *,
    json_format: Optional[bool] = False,
    level: Optional[int | str] = None,
    logger: Optional[Logger] = None,
) -> Iterator[Logger]:
    """
    Temporarily add a file handler to logger, root by default.
    Ensures it is detached and closed, even on error.
    """
    log = logger or getLogger()
    handler: RotatingFileHandler | None = None
    try:
        handler = add_file_handler(path, json_format=json_format, level=level)
        yield log
    finally:
        if handler is not None:
            remove_file_handler(handler)
