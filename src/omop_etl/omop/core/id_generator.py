import hashlib
import json
import datetime as dt
from dataclasses import dataclass

"""
Hardcoded source constant, not config/env-overridable at runtime.
Bump only for a deliberate, source-controlled identity-scheme change, (e.g. recovering from a genuine truncation collision).
A bump changes every generated id in the CDM.
"""
ROW_ID_SCHEME_VERSION = 0


@dataclass(frozen=True, slots=True)
class _RowIdHash:
    row_id: int
    digest: bytes


def _compute_row_id_hash(namespace: str, payload: str) -> _RowIdHash:
    """
    Pure and stateless sha256 hash,
    truncated 53-bit to be consumeable by downstream dependancies (OHDSI tools)
    while still being representable as BIGINT in postgres.
    """
    digest = hashlib.sha256(f"v{ROW_ID_SCHEME_VERSION}:{namespace}:{payload}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big", signed=False) & ((1 << 53) - 1)
    return _RowIdHash(row_id=value, digest=digest)


class RowIdTruncationCollision(RuntimeError):
    """
    Two genuinely different canonical inputs truncated to the same 53-bit row
    id within one namespace. SHA-256 hasn't collided but two distinct digests just
    share the same 53-bit projection, so someone must confirm the two digests
    really differ, bumps ROW_ID_SCHEME_VERSION, and rebuild the whole CDM.
    If they don't differ it's a bug in the NK definition of the domain model (see: harmonizaton/models).
    that produced the input, or a dedupe issue not caught by hydration (see `BaseHarmonizer`).
    """

    def __init__(self, *, namespace: str, row_id: int, first_digest: bytes, second_digest: bytes):
        self.namespace = namespace
        self.row_id = row_id
        self.first_digest = first_digest
        self.second_digest = second_digest
        super().__init__(
            f"Truncation collision in namespace {namespace!r} at row_id={row_id}: first_digest={first_digest.hex()}, second_digest={second_digest.hex()}"
        )


type RowIdPart = str | int | float | bool | None | dt.date | dt.datetime | tuple["RowIdPart", ...]


def _normalize_row_id_part(part: RowIdPart) -> object:
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
        return [_normalize_row_id_part(p) for p in part]

    if isinstance(part, str | int | float | bool):
        return part

    raise TypeError(f"Unsupported row-id key part: {part!r} ({type(part).__name__})")


def _canonicalize(parts: tuple[RowIdPart, ...]) -> str:
    return json.dumps(
        [_normalize_row_id_part(p) for p in parts],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def sha256_bigint(namespace: str, value: str) -> int:
    return _compute_row_id_hash(namespace, value).row_id


def row_id(namespace: str, *parts: RowIdPart) -> int:
    return _compute_row_id_hash(namespace, _canonicalize(parts)).row_id


class RowIdGenerator:
    """
    Run-scoped wrapper around `compute_row_id_hash` that adds truncation-
    collision tracking as a side effect. Constructed once per ETL run
    and shared across every builder that mints ids. Don't make this
    module-global: tests (and concurrent runs) would otherwise contaminate
    each other's namespaces with unrelated "collisions".

    Seeing the same `(namespace, row_id)` with the same digest again is not
    an error, it's the same logical identity being requested/referenced
    more than once (e.g. two builders independently computing the same FK).
    The post-build key duplication check this.
    """

    def __init__(self) -> None:
        self._seen: dict[tuple[str, int], bytes] = {}

    def generate(self, namespace: str, *parts: RowIdPart) -> int:
        result = _compute_row_id_hash(namespace, _canonicalize(parts))
        key = (namespace, result.row_id)
        previous = self._seen.get(key)
        if previous is None:
            self._seen[key] = result.digest
            return result.row_id

        if previous != result.digest:
            raise RowIdTruncationCollision(
                namespace=namespace,
                row_id=result.row_id,
                first_digest=previous,
                second_digest=result.digest,
            )
        return result.row_id
