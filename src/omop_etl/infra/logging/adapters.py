import logging
from typing import Mapping, Any

_RESERVED = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "process",
    "processName",
}


class ExtraAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.pop("extra", {}) or {}
        merged = {**self.extra, **extra}
        # drop reserved keys
        for k in list(merged.keys()):
            if k in _RESERVED:
                merged.pop(k)
        kwargs["extra"] = merged
        return msg, kwargs


def with_extra(logger: logging.Logger, extra: Mapping[str, Any]) -> ExtraAdapter:
    return ExtraAdapter(logger, dict(extra))
