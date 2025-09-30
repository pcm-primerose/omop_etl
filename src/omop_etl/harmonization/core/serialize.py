# infra/serialize/polars_io.py
from __future__ import annotations
import datetime as dt
import inspect
import typing as t
from dataclasses import dataclass
import polars as pl
import polars.datatypes as _PLDT

NoneType = type(None)
IDENTITY_FIELDS = {"patient_id", "trial_id"}

# ---------- type mapping / reflection ----------


def _unwrap_optional(tp):
    if t.get_origin(tp) is t.Union:
        args = [a for a in t.get_args(tp) if a is not NoneType]
        if len(args) == 1:
            return args[0]
    return tp


def _py_to_pl(tp: t.Any) -> pl.DataType:
    tp = _unwrap_optional(tp)
    if tp is bool:
        return pl.Boolean
    if tp is int:
        return pl.Int64
    if tp is float:
        return pl.Float64
    if tp is str:
        return pl.Utf8
    if tp is dt.date:
        return pl.Date
    if tp is dt.datetime:
        return pl.Datetime
    # widen unknowns/enums
    return pl.Utf8


def _prop_ret_type(cls: type, name: str):
    prop = getattr(cls, name, None)
    if not isinstance(prop, property) or not prop.fget:
        return None
    return t.get_type_hints(prop.fget).get("return")


def _leaf_field_hints(leaf_cls: type) -> dict[str, t.Any]:
    hints: dict[str, t.Any] = {}
    hints.update(getattr(leaf_cls, "__annotations__", {}))
    for nm, prop in inspect.getmembers(leaf_cls, lambda o: isinstance(o, property)):
        if nm.startswith("_") or not prop.fget:
            continue
        rt = t.get_type_hints(prop.fget).get("return")
        if rt is not None:
            hints.setdefault(nm, rt)
    for k in list(hints.keys()):
        if k in IDENTITY_FIELDS:
            hints.pop(k)
    return hints


def _leaf_struct(leaf_cls: type) -> pl.Struct:
    fields = {fname: _py_to_pl(tp) for fname, tp in _leaf_field_hints(leaf_cls).items()}
    return pl.Struct(fields)


def patient_nested_schema(patient_cls: type) -> dict[str, _PLDT]:
    schema: dict[str, _PLDT] = {}
    for name, prop in inspect.getmembers(patient_cls, lambda o: isinstance(o, property)):
        if name.startswith("_") or not prop.fget:
            continue
        rt = _prop_ret_type(patient_cls, name)
        if rt is None:
            continue

        base = _unwrap_optional(rt)
        origin = t.get_origin(base)

        if origin in (list, tuple, t.Sequence, t.MutableSequence):
            args = t.get_args(base)
            if args:
                elem = _unwrap_optional(args[0])
                if isinstance(elem, type):
                    schema[name] = pl.List(_leaf_struct(elem))
            continue

        if isinstance(base, type):
            pl_dt = _py_to_pl(base)
            if pl_dt is pl.Utf8 and base not in (str, int, float, bool, dt.date, dt.datetime):
                schema[name] = _leaf_struct(base)
            else:
                schema[name] = pl_dt

    # identities (as strings)
    schema.setdefault("patient_id", pl.Utf8)
    schema.setdefault("trial_id", pl.Utf8)
    return schema


# ---------- value conversion (keep Polars-native types) ----------


def _to_polars_primitive(v):
    if v is None or isinstance(v, (str, int, float, bool, dt.date, dt.datetime)):
        return v
    if isinstance(v, (list, tuple)):
        return [_to_polars_primitive(x) for x in v]
    if isinstance(v, dict):
        return {k: _to_polars_primitive(x) for k, x in v.items()}
    if hasattr(v, "to_dict") and callable(v.to_dict):
        return _to_polars_primitive(v.to_dict())
    if hasattr(v, "__dict__"):
        return {k.lstrip("_"): _to_polars_primitive(x) for k, x in v.__dict__.items() if not k.startswith("__")}
    return str(v)


# ---------- row materialization (nested) ----------


def _public_properties(cls: type) -> dict[str, property]:
    return {name: prop for name, prop in inspect.getmembers(cls, lambda o: isinstance(o, property)) if not name.startswith("_")}


def _export_by_properties(obj: t.Any, *, exclude: set[str] = IDENTITY_FIELDS) -> dict[str, t.Any]:
    out: dict[str, t.Any] = {}
    for nm, prop in _public_properties(obj.__class__).items():
        if not prop.fget or nm in exclude:
            continue
        out[nm] = _to_polars_primitive(getattr(obj, nm))
    return out


def build_nested_df(patients: list, patient_cls: type) -> pl.DataFrame:
    schema = patient_nested_schema(patient_cls)
    rows: list[dict] = []
    for p in patients:
        row: dict[str, t.Any] = {}
        # primitive/scalar props
        for name, prop in _public_properties(patient_cls).items():
            if not prop.fget:
                continue
            val = getattr(p, name)
            rt = _prop_ret_type(patient_cls, name)
            if rt is None:
                continue
            base = _unwrap_optional(rt)
            origin = t.get_origin(base)

            if val is None:
                # primitives: leave None; singleton struct: None; collections: [] (keep shape)
                if origin in (list, tuple, t.Sequence, t.MutableSequence):
                    row[name] = []
                else:
                    row[name] = None
                continue

            # collections -> list of dicts
            if origin in (list, tuple, t.Sequence, t.MutableSequence):
                row[name] = [_export_by_properties(it) for it in list(val)]
                continue

            # singleton leaf -> dict
            if isinstance(base, type) and _py_to_pl(base) is pl.Utf8 and base not in (str, int, float, bool, dt.date, dt.datetime):
                row[name] = _export_by_properties(val)
                continue

            # primitive
            row[name] = _to_polars_primitive(val)

        rows.append(row)

    # explicit schema, no inference
    return pl.DataFrame(rows, schema=schema, strict=False, infer_schema_length=0)


# ---------- wide & normalized from nested ----------


def _unnest_singleton_into(df: pl.DataFrame, attr: str, ids: list[str], sep: str) -> pl.DataFrame:
    """
    Return a frame with only `ids` + prefixed fields from the singleton struct `attr`.
    """
    if attr not in df.columns or df.schema[attr] != pl.Struct:
        # nothing to emit
        return pl.DataFrame({k: df[k] for k in ids})
    # only ids + the struct
    sub = df.select([*ids, attr])
    before = set(sub.columns)
    sub2 = sub.unnest(attr)
    # fields added by unnest:
    fields = [c for c in sub2.columns if c not in before]
    if not fields:
        return pl.DataFrame({k: df[k] for k in ids})
    # prefix and select
    rename = {c: f"{attr}{sep}{c}" for c in fields}
    return sub2.rename(rename).select([*ids, *rename.values()])


def _explode_collection_into(df: pl.DataFrame, attr: str, ids: list[str], sep: str) -> pl.DataFrame | None:
    """
    Return a frame with only `ids` + row_index + prefixed fields from the list[struct] `attr`.
    """
    if attr not in df.columns or not isinstance(df.schema[attr], pl.List):
        return None
    sub = df.select([*ids, attr]).explode(attr).filter(pl.col(attr).is_not_null())
    # # derive row_index: prefer sequence_id; else row_number per patient_id
    # has_seq = sub.select(pl.col(attr).struct.field("sequence_id").is_not_null().any()).item()
    # if has_seq:
    #     sub = sub.with_columns(
    #         pl.col(attr).struct.field("sequence_id").cast(pl.Int64, strict=False).alias("row_index")
    #     )
    # else:
    #     # per-patient row_number if possible; fallback: global row_count
    sub = sub.with_columns(
        pl.int_range(0, pl.len()).over(ids[0]).alias("row_index"),
    )
    # sub = sub.with_row_index("row_index")

    before = set(sub.columns)
    sub2 = sub.unnest(attr)
    fields = [c for c in sub2.columns if c not in before]
    if not fields:
        return None
    rename = {c: f"{attr}{sep}{c}" for c in fields}
    return sub2.rename(rename).select([*ids, "row_index", *rename.values()])


def to_wide(df_nested: pl.DataFrame, prefix_sep: str = ".") -> pl.DataFrame:
    df = df_nested

    # Unnest ALL singletons on a snapshot of the schema (avoid iterator invalidation)
    for c, dtp in list(df.schema.items()):
        if dtp == pl.Struct:
            before = set(df.columns)
            df = df.unnest(c)
            new_cols = [x for x in df.columns if x not in before]
            if new_cols:
                df = df.rename({x: f"{c}{prefix_sep}{x}" for x in new_cols})

    # Base = non-nested only (scalars + flattened singletons)
    base_cols = [c for c, tp in df.schema.items() if tp not in (pl.Struct,) and not isinstance(tp, pl.List)]
    ids = ["patient_id", "trial_id"]
    df_base = df.select(base_cols)

    # One narrow part PER collection; keep ONLY base + that collection’s fields + metadata
    parts = []
    for c, tp in df.schema.items():
        if isinstance(tp, pl.List):
            part = _explode_collection_into(df, c, ids, prefix_sep)
            if part is not None:
                # add _collection tag and rejoin the base columns on ids (to keep scalars)
                part = part.join(df_base, on=ids, how="left").with_columns(pl.lit(c).alias("_collection"))
                parts.append(part)

    if parts:
        wide = pl.concat(parts, how="diagonal_relaxed")
        # Patients with no items in any collection -> emit one base row
        has_any = None
        for c, tp in df.schema.items():
            if isinstance(tp, pl.List):
                cond = pl.col(c).list.len().fill_null(0) > 0
                has_any = cond if has_any is None else (has_any | cond)
        base_rows = df.filter(~has_any if has_any is not None else pl.lit(True)).select(base_cols)
        if base_rows.height:
            base_rows = base_rows.with_columns(
                pl.lit(None, dtype=pl.Int64).alias("row_index"),
                pl.lit(None, dtype=pl.Utf8).alias("_collection"),
            )
            wide = pl.concat([wide, base_rows], how="diagonal_relaxed")
    else:
        wide = df_base.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("row_index"),
            pl.lit(None, dtype=pl.Utf8).alias("_collection"),
        )

    # Final safety: wide must be flat
    nest = [c for c, tp in wide.schema.items() if tp == pl.Struct or isinstance(tp, pl.List)]
    if nest:
        raise RuntimeError(f"wide contains nested columns: {nest}")
    return wide


def to_normalized(df_nested: pl.DataFrame) -> dict[str, pl.DataFrame]:
    out: dict[str, pl.DataFrame] = {}
    ids = ["patient_id", "trial_id"]

    # patients: primitive columns only (from nested df)
    base_cols = [c for c, tp in df_nested.schema.items() if tp not in (pl.Struct,) and not isinstance(tp, pl.List)]
    out["patients"] = df_nested.select([*ids, *[c for c in base_cols if c not in ids]])

    # singletons: one table per struct, prefixed fields only
    for c, tp in df_nested.schema.items():
        if tp == pl.Struct:
            tbl = _unnest_singleton_into(df_nested, c, ids, sep=".")
            # ensure flat
            if any(isinstance(dt, pl.List) or dt == pl.Struct for dt in tbl.schema.values()):
                tbl = tbl.select(ids)  # defensive fallback
            out[c] = tbl

    # collections: one table per list[struct], prefixed fields + row_index
    for c, tp in df_nested.schema.items():
        if isinstance(tp, pl.List):
            tbl = _explode_collection_into(df_nested, c, ids, sep=".")
            if tbl is None:
                continue
            # ensure flat
            if any(isinstance(dt, pl.List) or dt == pl.Struct for dt in tbl.schema.values()):
                raise RuntimeError(f"normalized '{c}' still nested")
            out[c] = tbl

    return out


# ---------- developer-facing introspection ----------


@dataclass(frozen=True)
class FieldInfo:
    owner: str  # class name
    name: str  # property name
    return_type: t.Any  # typing annotation (possibly Optional[...] / Sequence[...])
    category: str  # "scalar" | "singleton" | "collection"
    polars_dtype: pl.DataType


def iter_fields(obj: t.Any) -> list[FieldInfo]:
    cls = obj.__class__
    infos: list[FieldInfo] = []
    for name, prop in _public_properties(cls).items():
        if not prop.fget:
            continue
        rt = _prop_ret_type(cls, name)
        if rt is None:
            continue
        base = _unwrap_optional(rt)
        origin = t.get_origin(base)
        if origin in (list, tuple, t.Sequence, t.MutableSequence):
            args = t.get_args(base)
            elem = _unwrap_optional(args[0]) if args else t.Any
            dtype = pl.List(_leaf_struct(elem) if isinstance(elem, type) else pl.Utf8)
            infos.append(FieldInfo(cls.__name__, name, rt, "collection", dtype))
        elif isinstance(base, type) and _py_to_pl(base) is pl.Utf8 and base not in (str, int, float, bool, dt.date, dt.datetime):
            infos.append(FieldInfo(cls.__name__, name, rt, "singleton", _leaf_struct(base)))
        else:
            infos.append(FieldInfo(cls.__name__, name, rt, "scalar", _py_to_pl(base)))
    return infos


# import datetime as dt
# import inspect
# import typing
# from dataclasses import dataclass, field
# from functools import lru_cache
# from typing import Any, Dict, List, Mapping, Tuple
# from typing import Any, Dict, Literal, Tuple, Dict, Callable, Set
# import datetime as dt
# import inspect
# import polars as pl
# from mypy.checkexpr import defaultdict
#
# # # TODO: make as simple as possible:
# # #   - recurse
# # #   - don't need filtering options (ExportOptions)
# # #  [ ] need to include field names in to_dataframe
# # #  [ ] need to return property value, propery type, property name and class name so I can make things from the data
# # #       - currently just returning the propery value
# #
# # primitive_python_types: Set = {bool, int, float}
# # primitive_polars_types: Set = {pl.Int64, pl.Float64, pl.Null, pl.Utf8, pl.Boolean, pl.Date}
# #
# # @dataclass
# # class Property:
# #     name: str
# #     type: primitive_python_types
# #     value: Any
# #     class_name: str
# #
# # def flatten_class(obj: Any) -> List:
# #     public_properties = _public_properties(obj)
# #     flattened: List = [_to_primitive(public_properties)]
# #     return flattened
# #
# # def get_schema_from_flattened(obj: Any) -> pl.Schema:
# #     flattened = flatten_class(obj)
# #     return pl.Schema(flattened)
# #
# # def get_flattened_dataframe(obj: Any) -> pl.DataFrame:
# #     flat = flatten_class(obj)
# #     schema = get_schema_from_flattened(flat)
# #     data = get_dataframe_from_class(obj)
# #     return pl.DataFrame(flat, schema=schema)
# #
# # def _to_primitive(value: Any) -> Property:
# #     primitives: List[Property] = []
# #     if value is None or isinstance(value, (str, int, float, bool, None, dt.date, pl.Date)):
# #         primitives.append(
# #             Property(
# #                 value=value,
# #                 name=value.__name__,
# #                 type=type(value),
# #                 class_name=value.__class__.fset.__name__
# #             )
# #         )
# #
# #     if isinstance(value, (list, tuple, set)):
# #         [_to_primitive(v) for v in value]
# #     if isinstance(value, dict):
# #         {k: _to_primitive(v) for k, v in value.items()}
# #     if hasattr(value, "to_dict") and callable(value.to_dict):
# #         return value.to_dict()
# #     if hasattr(value, "__dict__"):
# #         return {k.lstrip("_"): _to_primitive(v) for k, v in value.__dict__.items() if not k.startswith("__")}
# #     return str(value)
# #
# #     return primitives
# #
# # @lru_cache(maxsize=256)
# # def _public_properties(cls: type) -> Dict[str, property]:
# #     return {name: prop for name, prop in inspect.getmembers(cls, lambda o: isinstance(o, property)) if not name.startswith("_")}
#
#
#
# ## # # # #
#
# def to_primitive_json(value: Any) -> Any:
#     # original behavior (for JSON/CSV)
#     if value is None or isinstance(value, (str, int, float, bool)):
#         return value
#     if isinstance(value, (dt.date, dt.datetime)):
#         return value.isoformat()
#     if isinstance(value, (list, tuple, set)):
#         return [to_primitive_json(v) for v in value]
#     if isinstance(value, dict):
#         return {k: to_primitive_json(v) for k, v in value.items()}
#     if hasattr(value, "to_dict") and callable(value.to_dict):
#         return value.to_dict()
#     if hasattr(value, "__dict__"):
#         return {k.lstrip("_"): to_primitive_json(v) for k, v in value.__dict__.items() if not k.startswith("__")}
#     return str(value)
#
# def to_primitive_polars(value: Any) -> Any:
#     # keep native types that Polars understands (especially dt.date, dt.datetime, bool, int, float)
#     if value is None or isinstance(value, (str, int, float, bool, dt.date, dt.datetime)):
#         return value
#     if isinstance(value, (list, tuple, set)):
#         return [to_primitive_polars(v) for v in value]
#     if isinstance(value, dict):
#         return {k: to_primitive_polars(v) for k, v in value.items()}
#     if hasattr(value, "to_dict") and callable(value.to_dict):
#         return to_primitive_polars(value.to_dict())
#     if hasattr(value, "__dict__"):
#         return {k.lstrip("_"): to_primitive_polars(v) for k, v in value.__dict__.items() if not k.startswith("__")}
#     return str(value)
#
# # utility if some values already came in as ISO strings:
# def coerce_iso_date(v):
#     if isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-":
#         try:
#             return dt.date.fromisoformat(v)
#         except ValueError:
#             return v
#     return v
#
#
# @lru_cache(maxsize=256)
# def _public_properties(cls: type) -> Dict[str, property]:
#     return {name: prop for name, prop in inspect.getmembers(cls, lambda o: isinstance(o, property)) if not name.startswith("_")}
#
#
# @dataclass(frozen=True, slots=True)
# class ExportOptions:
#     include_properties: Mapping[type, set[str]] | None = None
#     exclude_properties: Mapping[type, set[str]] = field(default_factory=dict)
#     rename_exported_property: Mapping[tuple[type, str], str] = field(default_factory=dict)
#     identity_fields: Mapping[type, tuple[str, ...]] = field(default_factory=dict)
#
#
# def _property_alias(opts: ExportOptions, cls: type, name: str) -> str:
#     return opts.rename_exported_property.get((cls, name), name)
#
# def _include(cls: type, name: str, opts: ExportOptions) -> bool:
#     if opts.include_properties and cls in opts.include_properties:
#         return name in opts.include_properties[cls]
#     if cls in opts.exclude_properties and name in opts.exclude_properties[cls]:
#         return False
#     return True
#
#
#
# def export_by_properties(obj: Any, opts: ExportOptions | None = None, conv: Callable[[Any], Any] = to_primitive_json) -> Dict[str, Any]:
#     opts = opts or ExportOptions()
#     props = _public_properties(obj.__class__)
#     out: Dict[str, Any] = {}
#     for name, prop in props.items():
#         if not prop.fget or not _include(obj.__class__, name, opts):
#             continue
#         out[_property_alias(opts, obj.__class__, name)] = conv(getattr(obj, name))
#     # identities
#     for id_name in opts.identity_fields.get(obj.__class__, ()):
#         out.setdefault(id_name, getattr(obj, id_name, None))
#     return out
#
# def classify_fields(
#     obj: Any,
#     include_collections: bool = True,
#     opts: ExportOptions | None = None,
#     conv: Callable[[Any], Any] = to_primitive_json,
# ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, List[Any]]]:
#     opts = opts or ExportOptions()
#     scalars: Dict[str, Any] = {}
#     singles: Dict[str, Any] = {}
#     colls: Dict[str, List[Any]] = {}
#
#     props = _public_properties(obj.__class__)
#     for name in props:
#         if not _include(obj.__class__, name, opts):
#             continue
#         val = getattr(obj, name)
#         aliased = _property_alias(opts, obj.__class__, name)
#
#         if val is None:
#             # do NOT emit placeholder None for complex props; only primitives
#             # (preserve previous tweak)
#             # primitive None stays as scalar; complex -> skip
#             if hasattr(val, "__dict__") or isinstance(val, (list, tuple)):
#                 continue
#             scalars[aliased] = None
#             continue
#
#         if isinstance(val, (list, tuple)):
#             if include_collections and val:
#                 colls[aliased] = list(val)
#             continue
#
#         if hasattr(val, "to_dict") or hasattr(val, "__dict__"):
#             singles[aliased] = val
#             continue
#
#         scalars[aliased] = conv(val)
#
#     for id_name in opts.identity_fields.get(obj.__class__, ()):
#         scalars.setdefault(id_name, getattr(obj, id_name, None))
#
#     return scalars, singles, colls
#
#
# def classify_fields(
#     obj: Any,
#     include_collections: bool = True,
#     opts: ExportOptions | None = None,
# ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, List[Any]]]:
#     """
#     Returns (scalars, singletons, collections) using public properties only.
#     """
#     opts = opts or ExportOptions()
#     scalars: Dict[str, Any] = {}
#     singles: Dict[str, Any] = {}
#     colls: Dict[str, List[Any]] = {}
#
#     props = _public_properties(obj.__class__)
#     for name in props:
#         if not _include(obj.__class__, name, opts):
#             continue
#         val = getattr(obj, name)
#
#         aliased = _property_alias(opts, obj.__class__, name)
#
#         if val is None:
#             scalars[aliased] = None
#             continue
#
#         if isinstance(val, (list, tuple)):
#             if include_collections and val:
#                 colls[aliased] = list(val)
#             continue
#
#         if hasattr(val, "to_dict") or hasattr(val, "__dict__"):
#             singles[aliased] = val
#             continue
#
#         scalars[aliased] = to_primitive_polars(val)
#
#     for id_name in opts.identity_fields.get(obj.__class__, ()):
#         scalars.setdefault(id_name, getattr(obj, id_name, None))
#
#     return scalars, singles, colls
#
# NoneType = type(None)
#
# def _unwrap_optional(tp):
#     if typing.get_origin(tp) is typing.Union:
#         args = [a for a in typing.get_args(tp) if a is not NoneType]
#         if len(args) == 1:
#             return args[0]
#     return tp
#
# def _py_to_pl(tp: typing.Any) -> pl.DataType:
#     tp = _unwrap_optional(tp)
#     if tp is bool: return pl.Boolean
#     if tp is int: return pl.Int64
#     if tp is float: return pl.Float64
#     if tp is str: return pl.Utf8
#     if tp is dt.date: return pl.Date
#     return pl.Utf8  # widen unknowns/enums
#
# def _prop_ret_type(cls: type, name: str):
#     prop = getattr(cls, name, None)
#     if not isinstance(prop, property) or not prop.fget:
#         return None
#     return typing.get_type_hints(prop.fget).get("return")
#
# def _leaf_field_hints(leaf_cls: type) -> dict[str, typing.Any]:
#     hints = {}
#     hints.update(getattr(leaf_cls, "__annotations__", {}))
#     for nm, prop in inspect.getmembers(leaf_cls, lambda o: isinstance(o, property)):
#         if nm.startswith("_") or not prop.fget:
#             continue
#         rt = typing.get_type_hints(prop.fget).get("return")
#         if rt is not None:
#             hints.setdefault(nm, rt)
#     return hints
#
# def leaf_struct(leaf_cls: type) -> pl.Struct:
#     # Build a Struct dtype from the leaf’s annotated/property fields
#     fields = {fname: _py_to_pl(tp) for fname, tp in _leaf_field_hints(leaf_cls).items()}
#     return pl.Struct(fields)
#
# def patient_nested_schema(patient_cls: type) -> dict[str, pl.DataType]:
#     """
#     Returns a Polars schema where:
#       - primitive properties -> primitive columns
#       - Optional[Leaf]       -> Struct column
#       - Sequence[Leaf]       -> List[Struct] column
#     """
#     schema: dict[str, pl.DataType] = {}
#     for name, prop in inspect.getmembers(patient_cls, lambda o: isinstance(o, property)):
#         if name.startswith("_") or not prop.fget:
#             continue
#         rt = _prop_ret_type(patient_cls, name)
#         if rt is None:
#             continue
#         base = _unwrap_optional(rt)
#         origin = typing.get_origin(base)
#
#         if origin in (list, tuple, typing.Sequence, typing.MutableSequence):
#             args = typing.get_args(base)
#             if args:
#                 elem = _unwrap_optional(args[0])
#                 if isinstance(elem, type):
#                     schema[name] = pl.List(leaf_struct(elem))
#             continue
#
#         if isinstance(base, type):
#             # primitive?
#             if _py_to_pl(base) is not pl.Utf8 or base in (str, int, float, bool, dt.date, dt.datetime):
#                 schema[name] = _py_to_pl(base)
#             else:
#                 # non-primitive -> Struct
#                 schema[name] = leaf_struct(base)
#
#     # patient_id & trial_id guaranteed
#     schema.setdefault("patient_id", pl.Utf8)
#     schema.setdefault("trial_id", pl.Utf8)
#     return schema
