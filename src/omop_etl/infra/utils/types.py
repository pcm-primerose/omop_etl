from typing import get_origin, Union, get_args, Sequence, MutableSequence, Any
import types as _types


def unwrap_optional(_type: Any) -> Any:
    """
    Optional[T] or T|None -> T, otherwise unchanged.
    """
    origin = get_origin(_type)
    if origin in (Union, getattr(_types, "UnionType", None)):
        args = [a for a in get_args(_type) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return _type


def is_sequence_origin(tp: Any) -> bool:
    origin = get_origin(tp)
    return origin in (list, tuple, Sequence, MutableSequence)
