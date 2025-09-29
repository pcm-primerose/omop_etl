import datetime as dt
import inspect
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Mapping, Tuple


def to_primitive(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple, set)):
        return [to_primitive(v) for v in value]
    if isinstance(value, dict):
        return {k: to_primitive(v) for k, v in value.items()}
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    if hasattr(value, "__dict__"):
        return {k.lstrip("_"): to_primitive(v) for k, v in value.__dict__.items() if not k.startswith("__")}
    return str(value)


@lru_cache(maxsize=256)
def _public_properties(cls: type) -> Dict[str, property]:
    return {name: prop for name, prop in inspect.getmembers(cls, lambda o: isinstance(o, property)) if not name.startswith("_")}


@dataclass(frozen=True, slots=True)
class ExportOptions:
    include_properties: Mapping[type, set[str]] | None = None
    exclude_properties: Mapping[type, set[str]] = field(default_factory=dict)
    rename_exported_property: Mapping[tuple[type, str], str] = field(default_factory=dict)
    identity_fields: Mapping[type, tuple[str, ...]] = field(default_factory=dict)


def _property_alias(opts: ExportOptions, cls: type, name: str) -> str:
    return opts.rename_exported_property.get((cls, name), name)


def _include(cls: type, name: str, opts: ExportOptions) -> bool:
    if opts.include_properties and cls in opts.include_properties:
        return name in opts.include_properties[cls]
    if cls in opts.exclude_properties and name in opts.exclude_properties[cls]:
        return False
    return True


def export_by_properties(obj: Any, opts: ExportOptions | None = None) -> Dict[str, Any]:
    opts = opts or ExportOptions()
    props = _public_properties(obj.__class__)
    out: Dict[str, Any] = {}
    for name, prop in props.items():
        if not prop.fget or not _include(obj.__class__, name, opts):
            continue
        out[_property_alias(opts, obj.__class__, name)] = to_primitive(getattr(obj, name))
    # identities
    for id_name in opts.identity_fields.get(obj.__class__, ()):
        out.setdefault(id_name, getattr(obj, id_name, None))
    return out


def classify_fields(
    obj: Any,
    include_collections: bool = True,
    opts: ExportOptions | None = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, List[Any]]]:
    """
    Returns (scalars, singletons, collections) using public properties only.
    """
    opts = opts or ExportOptions()
    scalars: Dict[str, Any] = {}
    singles: Dict[str, Any] = {}
    colls: Dict[str, List[Any]] = {}

    props = _public_properties(obj.__class__)
    for name in props:
        if not _include(obj.__class__, name, opts):
            continue
        val = getattr(obj, name)

        aliased = _property_alias(opts, obj.__class__, name)

        if val is None:
            scalars[aliased] = None
            continue

        if isinstance(val, (list, tuple)):
            if include_collections and val:
                colls[aliased] = list(val)
            continue

        if hasattr(val, "to_dict") or hasattr(val, "__dict__"):
            singles[aliased] = val
            continue

        scalars[aliased] = to_primitive(val)

    for id_name in opts.identity_fields.get(obj.__class__, ()):
        scalars.setdefault(id_name, getattr(obj, id_name, None))

    return scalars, singles, colls
