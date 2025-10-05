from typing import Collection, Iterable, Sequence, List, Final, Mapping, cast
from types import MappingProxyType

from .types import Format, NORMALIZED_FORMATS, ALIASES

_EXTENSIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "csv": ".csv",
        "tsv": ".tsv",
        "parquet": ".parquet",
        "json": ".json",
    },
)


def ext(fmt: Format) -> str:
    try:
        return _EXTENSIONS[fmt]
    except KeyError as e:
        raise ValueError(f"Unknown format '{fmt}'. Allowed: {', '.join(_EXTENSIONS)}") from e


def _flatten(xs: Iterable) -> list:
    out: list = []
    for x in xs:
        if isinstance(x, (list, tuple)):
            out.extend(_flatten(x))
        else:
            out.append(x)
    return out


def expand_formats(
    formats: str | Sequence[str] | None,
    allowed: Collection[str] = NORMALIZED_FORMATS,
    allow_all: bool = False,
    default: str = "csv",
) -> List[Format]:
    if formats is None:
        raw: list[str] = [default]
    elif isinstance(formats, str):
        raw = [formats]
    else:
        raw = _flatten(formats)

    out: list[str] = []
    seen: set[str] = set()
    for f in raw:
        if not isinstance(f, str):
            raise ValueError(f"Format entries must be strings, got {type(f).__name__}")
        x = ALIASES.get(f.lower(), f.lower())
        if allow_all and x == "all":
            for t in allowed:
                if t not in seen:
                    seen.add(t)
                    out.append(t)
            continue
        if x not in allowed:
            raise ValueError(f"Unsupported format '{f}'. Allowed: {', '.join(sorted(allowed))} or 'all'.")
        if x not in seen:
            seen.add(x)
            out.append(x)

    # narrow to List[Format]
    return cast(List[Format], out)


def normalize_format(
    fmt: str | None,
    default: str = "csv",
    allowed: Collection[str] = NORMALIZED_FORMATS,
) -> Format:
    return expand_formats(fmt, allowed=allowed, allow_all=False, default=default)[0]
