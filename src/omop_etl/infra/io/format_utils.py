from typing import Iterable, Sequence, List, Final, Mapping, cast, TypeVar, Optional
from types import MappingProxyType

from .types import ALIASES, WIDE_FORMATS, AnyFormatToken

_EXTENSIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "csv": ".csv",
        "tsv": ".tsv",
        "parquet": ".parquet",
        "json": ".json",
    },
)


def ext(fmt: AnyFormatToken) -> str:
    try:
        return _EXTENSIONS[fmt]
    except KeyError as e:
        raise ValueError(f"Unknown format: {fmt}. Allowed: {', '.join(_EXTENSIONS)}") from e


def _flatten(xs: Iterable) -> list:
    out: list = []
    for x in xs:
        if isinstance(x, (list, tuple)):
            out.extend(_flatten(x))
        else:
            out.append(x)
    return out


F = TypeVar("F", bound=str)  # element type of `allowed` (TabularFormat or WideFormat)


def expand_formats(
    formats: AnyFormatToken | Sequence[AnyFormatToken],
    allowed: Optional[Sequence[F]],
) -> List[F]:
    # normalize to a flat list of strings, services ensures not None
    raw = [formats] if isinstance(formats, str) else _flatten(formats)

    allowed_set = set(allowed)
    seen: set[str] = set()
    requested: list[str] = []

    for tok in raw:
        t = tok.lower()
        if t == "all":
            for f in allowed:
                if f not in seen:
                    seen.add(f)
                    requested.append(f)
            continue
        t = ALIASES.get(t, t)  # map aliases
        if t not in allowed_set:
            raise ValueError(f"Unsupported format: {tok}. Allowed: {', '.join(allowed)} or 'all'.")
        if t not in seen:
            seen.add(t)
            requested.append(t)

    return cast(List[F], requested)


def normalize_format(
    fmt: AnyFormatToken,
    allowed: Sequence[AnyFormatToken] = WIDE_FORMATS,
) -> AnyFormatToken:
    return expand_formats(fmt, allowed=allowed)[0]
