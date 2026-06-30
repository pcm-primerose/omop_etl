import hashlib
import json
import datetime as dt


def sha256_bigint(namespace: str, value: str) -> int:
    h = hashlib.sha256(f"{namespace}:{value}".encode("utf-8")).digest()
    n = int.from_bytes(h[:8], "big", signed=False)
    return n & ((1 << 63) - 1)


type RowIdPart = str | int | float | bool | None | dt.date | dt.datetime | tuple["RowIdPart", ...]


def normalize_row_id_part(part: RowIdPart) -> object:
    if part is None:
        return None

    if isinstance(part, dt.datetime):
        return {
            "__type__": "datetime",
            "value": part.isoformat(),
        }

    if isinstance(part, dt.date):
        return {
            "__type__": "date",
            "value": part.isoformat(),
        }

    if isinstance(part, tuple):
        return [normalize_row_id_part(p) for p in part]

    if isinstance(part, str | int | float | bool):
        return part

    raise TypeError(f"Unsupported row-id key part: {part!r} ({type(part).__name__})")


def row_id(namespace: str, *parts: RowIdPart) -> int:
    """
    Deterministic 63-bit row id from `(namespace, *parts)`.
    """
    payload = json.dumps(
        [normalize_row_id_part(p) for p in parts],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return sha256_bigint(namespace, payload)
