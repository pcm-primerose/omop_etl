from functools import lru_cache
from typing import get_type_hints, get_origin, Union, get_args, TypeVar
from types import UnionType
from dataclasses import fields as dataclass_fields

T = TypeVar("T")


class _RowValidationError(Exception):
    """Raised when a row fails validation."""

    def __init__(self, row_type: str, field_name: str, message: str):
        self.row_type = row_type
        self.field_name = field_name
        super().__init__(f"{row_type}.{field_name}: {message}")


def _is_optional_type(type_hint) -> bool:
    """Check if a type hint is Optional: includes None."""
    origin = get_origin(type_hint)
    # handle None union type
    if origin is UnionType or origin is Union:
        return type(None) in get_args(type_hint)
    return False


@lru_cache(maxsize=None)
def _required_field_names(row_cls: type[T]) -> tuple[str, ...]:
    hints = get_type_hints(row_cls)

    required: list[str] = []
    for f in dataclass_fields(row_cls):
        if f.name.startswith("_"):
            continue

        t = hints.get(f.name)
        if t is None:
            continue

        if not _is_optional_type(t):
            required.append(f.name)

    return tuple(required)


def validate_required_fields(row: T) -> None:
    row_cls: type[T] = type(row)
    row_type = row_cls.__name__

    for name in _required_field_names(row_cls):
        if getattr(row, name) is None:
            raise _RowValidationError(row_type, name, "required field is None")
