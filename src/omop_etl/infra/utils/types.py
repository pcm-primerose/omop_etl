import typing as t
import types as _types


def unwrap_optional(tp: t.Any) -> t.Any:
    """
    Optional[T] or T|None -> T, otherwise unchanged.
    """
    origin = t.get_origin(tp)
    if origin in (t.Union, getattr(_types, "UnionType", None)):
        args = [a for a in t.get_args(tp) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return tp


def is_sequence_origin(tp: t.Any) -> bool:
    origin = t.get_origin(tp)
    return origin in (list, tuple, t.Sequence, t.MutableSequence)
