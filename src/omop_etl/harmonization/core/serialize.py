import datetime as dt
import inspect
import typing as t
from functools import lru_cache
import polars as pl
from polars._typing import PolarsDataType as polars_data_type
from omop_etl.infra.io.types import SerializeTypes
from omop_etl.infra.utils.typing_utils import unwrap_optional


# TODO: refactor main three branches to separate modules:
# scalars
# singletons
# collections


def to_wide(df_nested: pl.DataFrame, prefix_sep: str = SerializeTypes.COL_SEP) -> pl.DataFrame:
    """
    Flatten a nested frame into a single wide frame:
      - one base row per patient (ids + scalars + singleton fields)
      - one row per item per collection (ids + row_index + collection fields)
      - row_type marks base or collection name
    """
    df = df_nested

    for collection, dtp in list(df.schema.items()):
        if dtp == pl.Struct:
            before = set(df.columns)
            df = df.unnest(collection)
            new_cols = [x for x in df.columns if x not in before]
            if new_cols:
                df = df.rename({x: f"{collection}{prefix_sep}{x}" for x in new_cols})

    base_cols = [c for c, tp in df.schema.items() if tp not in (pl.Struct,) and not isinstance(tp, pl.List)]
    base_only = [c for c in base_cols if c not in SerializeTypes.ID_COLUMNS]
    base = df.select([*SerializeTypes.ID_COLUMNS, *base_only]).with_columns(
        pl.lit(None, dtype=pl.Int64).alias("row_index"),
        pl.lit("base").alias("row_type"),
    )

    base = _reorder_wide_columns(df=base, ids=SerializeTypes.ID_COLUMNS)

    # collection parts: ids + row_index + fields + row_type with no scalar duplication
    parts: list[pl.DataFrame] = []
    for collection, schema in df.schema.items():
        if isinstance(schema, pl.List):
            part = _explode_collection_into(df, collection, SerializeTypes.ID_COLUMNS, sep=prefix_sep)
            if part is not None:
                part = part.with_columns(pl.lit(collection).alias("row_type"))
                parts.append(_reorder_wide_columns(part, SerializeTypes.ID_COLUMNS))

    wide = pl.concat([base, *parts], how="diagonal_relaxed") if parts else base

    # ensure flat
    nested = [c for c, tp in wide.schema.items() if tp == pl.Struct or isinstance(tp, pl.List)]
    if nested:
        raise RuntimeError(f"wide contains nested columns: {nested}")

    return _sort_wide(wide)


def to_normalized(df_nested: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Produce normalized tables:
      - patients: ids + primitive columns only
      - one singleton table per struct (ids + prefixed fields)
      - one collection table per list[struct] (ids + row_index + prefixed fields)
    """
    out: dict[str, pl.DataFrame] = {}

    # patients
    base_cols = [c for c, tp in df_nested.schema.items() if tp not in (pl.Struct,) and not isinstance(tp, pl.List)]
    out["patients"] = df_nested.select([*SerializeTypes.ID_COLUMNS, *[c for c in base_cols if c not in SerializeTypes.ID_COLUMNS]]).sort(
        pl.col("patient_id"),
    )

    # singletons
    for c, tp in df_nested.schema.items():
        if isinstance(tp, pl.Struct):
            tbl = _unnest_singleton_into(df_nested, c, SerializeTypes.ID_COLUMNS, sep=SerializeTypes.COL_SEP)
            if any(isinstance(dt, pl.List) or isinstance(dt, pl.Struct) for dt in tbl.schema.values()):
                tbl = tbl.select(SerializeTypes.ID_COLUMNS)  # defensive fallback
            out[c] = tbl

    # collections
    for c, tp in df_nested.schema.items():
        if isinstance(tp, pl.List):
            tbl = _explode_collection_into(df_nested, c, SerializeTypes.ID_COLUMNS, sep=SerializeTypes.COL_SEP)
            if tbl is None:
                continue
            if any(isinstance(dt, pl.List) or dt == pl.Struct for dt in tbl.schema.values()):
                raise RuntimeError(f"normalized '{c}' still nested")
            out[c] = tbl

    return out


def build_nested_schema(patients: list, patient_cls: type) -> dict[str, polars_data_type]:
    """Public: class schema & data enrichment (handles dynamic leaves)."""
    out = _enrich_schema_from_data(patients, patient_cls, _patient_class_schema(patient_cls))
    return out


def build_nested_df(patients: list, patient_cls: type) -> pl.DataFrame:
    """
    Build a nested DataFrame with:
      - scalar columns
      - singleton Struct columns
      - collection List[Struct] columns
    with a deterministic schema and no dtype inference.
    """
    schema = build_nested_schema(patients, patient_cls)
    rows: list[dict] = []

    for p in patients:
        row: dict[str, t.Any] = {}
        for name, prop in _public_properties(patient_cls).items():
            val = getattr(p, name, None)
            rt = _property_return_type(patient_cls, name)
            base = unwrap_optional(rt) if rt else None
            origin = t.get_origin(base) if base else None

            if val is None:
                row[name] = [] if origin in (list, tuple, t.Sequence, t.MutableSequence) else None
                continue

            if origin in (list, tuple, t.Sequence, t.MutableSequence):
                row[name] = [_export_leaf_object(it) for it in list(val)]
            elif isinstance(base, type) and schema.get(name) == pl.Struct:
                row[name] = _export_leaf_object(val)
            else:
                row[name] = _to_polars_primitive(val)

        rows.append(row)

    # rows contains all data (scalars + singletons + collections)
    out = pl.DataFrame(rows, schema=schema, strict=False, infer_schema_length=0)
    return out


def _py_to_pl(tp: t.Any) -> polars_data_type:
    tp = unwrap_optional(tp)
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
    return pl.Utf8


def _unify_dtypes(a: polars_data_type, b: polars_data_type) -> polars_data_type:
    if a == b:
        return a
    nums: set[polars_data_type] = {pl.Int64, pl.Float64}
    if a in nums and b in nums:
        return pl.Float64
    if pl.Utf8 in (a, b):
        return pl.Utf8
    if a == pl.Datetime or b == pl.Datetime:
        return pl.Datetime
    if a == pl.Date and b == pl.Date:
        return pl.Date
    if (a == pl.Boolean and b == pl.Int64) or (b == pl.Boolean and a == pl.Int64):
        return pl.Int64
    return pl.Utf8


@lru_cache(maxsize=512)
def _public_properties(cls: type) -> dict[str, property]:
    """Public @property descriptors on a class."""
    return {name: prop for name, prop in inspect.getmembers(cls, lambda o: isinstance(o, property)) if not name.startswith("_")}


def _property_return_type(cls: type, name: str) -> t.Any | None:
    """
    Return the property's annotated return type, if not present, fall back to
    the setter value param annotation.
    Returns None if neither exists.
    """
    prop = getattr(cls, name, None)
    if not isinstance(prop, property):
        return None

    if prop.fget:
        rt = t.get_type_hints(prop.fget).get("return")
        if rt is not None:
            return rt

    if prop.fset:
        sh = t.get_type_hints(prop.fset)
        if "value" in sh:
            return sh["value"]

    return None


def _leaf_field_hints(leaf_cls: type) -> dict[str, t.Any]:
    """
    Collect field type hints from @property returns and class __annotations__.
    Identity fields are excluded, undefined to not included.
    """
    hints: dict[str, t.Any] = {}
    hints.update(getattr(leaf_cls, "__annotations__", {}))
    for nm, prop in _public_properties(leaf_cls).items():
        if prop.fget:
            rt = t.get_type_hints(prop.fget).get("return")
            if rt is not None:
                hints.setdefault(nm, rt)
    for k in list(hints.keys()):
        if k in SerializeTypes.IDENTITY_FIELDS:
            hints.pop(k)

    return hints


def _leaf_struct_from_class(leaf_cls: type) -> pl.Struct:
    """Struct dtype composed from leaf class type hints only."""
    fields = {fname: _py_to_pl(tp) for fname, tp in _leaf_field_hints(leaf_cls).items()}
    return pl.Struct(fields)


def _to_polars_primitive(v: t.Any) -> t.Any:
    """
    Convert nested types to Polars-compatible Python values without stringifing dates.
    """
    if v is None or isinstance(v, (str, int, float, bool, dt.date, dt.datetime)):
        return v
    if isinstance(v, SerializeTypes.LIST_OR_TUPLE):
        return [_to_polars_primitive(x) for x in v]
    if isinstance(v, dict):
        return {k: _to_polars_primitive(x) for k, x in v.items()}
    if hasattr(v, "to_dict") and callable(v.to_dict):
        return _to_polars_primitive(v.to_dict())
    if hasattr(v, "__dict__"):
        return {k.lstrip("_"): _to_polars_primitive(x) for k, x in v.__dict__.items() if not k.startswith("__")}
    return str(v)


def _export_leaf_object(obj: t.Any, *, exclude: set[str] = SerializeTypes.IDENTITY_FIELDS) -> dict[str, t.Any]:
    """
    Export an object as a dict. Prefer public @properties, if none, fall back
    to __dict__ for dynamic leaves (like C30/EQ5D).
    Identity fields are dropped.
    """
    props = _public_properties(obj.__class__)
    if props:
        return {nm: _to_polars_primitive(getattr(obj, nm)) for nm, p in props.items() if p.fget and nm not in exclude}
    if hasattr(obj, "__dict__"):
        out: dict[str, t.Any] = {}
        for k, v in obj.__dict__.items():
            if k.startswith("_") or k in exclude:
                continue
            out[k] = _to_polars_primitive(v)
        return out
    return {}


def _patient_class_schema(patient_cls: type) -> dict[str, polars_data_type]:
    """
    Schema from class annotations & property returns:
      - scalars -> primitive dtype
      - Optional[Leaf] -> Struct
      - Sequence[Leaf] -> List[Struct]
    """
    schema: dict[str, polars_data_type] = {}
    for name, prop in _public_properties(patient_cls).items():
        rt = _property_return_type(patient_cls, name)
        if rt is None:
            # default Utf8 so if there are unannotated scalars they aren't dropped
            schema[name] = pl.Utf8
            continue

        base = unwrap_optional(rt)
        origin = t.get_origin(base)

        # collections
        if origin in (list, tuple, t.Sequence, t.MutableSequence):
            args = t.get_args(base)
            if args:
                elem = unwrap_optional(args[0])
                if isinstance(elem, type):
                    schema[name] = pl.List(_leaf_struct_from_class(elem))
            continue

        # singletons or scalars
        if isinstance(base, type):
            pl_dt = _py_to_pl(base)
            if pl_dt is pl.Utf8 and base not in (str, int, float, bool, dt.date, dt.datetime):
                schema[name] = _leaf_struct_from_class(base)  # leaf class
            else:
                schema[name] = pl_dt

    # identities
    schema.setdefault("patient_id", pl.Utf8)
    schema.setdefault("trial_id", pl.Utf8)
    return schema


def _enrich_schema_from_data(patients: list, patient_cls: type, schema: dict[str, polars_data_type]) -> dict[str, polars_data_type]:
    """
    If any leaf Struct/List(Struct) is empty, derive fields from actual data.
    """
    sch = dict(schema)

    for name, prop in _public_properties(patient_cls).items():
        return_type = _property_return_type(patient_cls, name)
        base = unwrap_optional(return_type) if return_type else None
        origin = t.get_origin(base) if base else None

        if isinstance(base, type) and sch.get(name) == pl.Struct:
            fields: dict[str, pl.DataType] = {}
            for p in patients:
                v = getattr(p, name, None)
                if v is None:
                    continue
                d = _export_leaf_object(v)
                for k, val in d.items():
                    dt = _py_to_pl(type(val)) if val is not None else pl.Null
                    fields[k] = _unify_dtypes(fields.get(k, dt), dt) if k in fields else dt

            if fields:
                sch[name] = pl.Struct({k: (pl.Utf8 if v == pl.Null else v) for k, v in fields.items()})

        # collections with empty inner struct: learn fields from data
        if origin in (list, tuple, t.Sequence, t.MutableSequence) and isinstance(sch.get(name), pl.List):
            inner = sch[name].inner
            if str(inner) == "Struct({})":
                fields: dict[str, pl.DataType] = {}
                for p in patients:
                    seq = getattr(p, name, ()) or ()
                    for it in seq:
                        d = _export_leaf_object(it)
                        for k, val in d.items():
                            dt = _py_to_pl(type(val)) if val is not None else pl.Null
                            fields[k] = _unify_dtypes(fields.get(k, dt), dt) if k in fields else dt
                if fields:
                    sch[name] = pl.List(pl.Struct({k: (pl.Utf8 if v == pl.Null else v) for k, v in fields.items()}))

    sch.setdefault("patient_id", pl.Utf8)
    sch.setdefault("trial_id", pl.Utf8)
    return sch


def _unnest_singleton_into(df: pl.DataFrame, attr: str, ids: t.Sequence[str], sep: str) -> pl.DataFrame:
    """Return a frame with only ids + prefixed fields from singleton struct attr."""
    if attr not in df.columns or not df.schema[attr] == pl.Struct:
        return df.select(ids)

    sub = df.select([*ids, attr])
    before = set(sub.columns)
    sub2 = sub.unnest(attr)
    fields = [c for c in sub2.columns if c not in before]
    if not fields:
        return df.select(ids)
    rename = {c: f"{attr}{sep}{c}" for c in fields}
    return sub2.rename(rename).select([*ids, *rename.values()])


def _explode_collection_into(df: pl.DataFrame, attr: str, ids: t.Sequence[str], sep: str) -> pl.DataFrame | None:
    """
    Return a frame with only ids & per-patient row_index & prefixed fields from list[struct] attr.
    """
    if attr not in df.columns or not isinstance(df.schema[attr], pl.List):
        return None
    sub = df.select([*ids, attr]).explode(attr).filter(pl.col(attr).is_not_null())

    # per-patient row_index
    sub = sub.with_columns(pl.int_range(0, pl.len()).over(ids).alias("row_index"))

    before = set(sub.columns)
    sub2 = sub.unnest(attr)
    new_fields = [c for c in sub2.columns if c not in before]
    if not new_fields:
        return None

    rename = {c: f"{attr}{sep}{c}" for c in new_fields}
    return sub2.rename(rename).select([*ids, "row_index", *rename.values()])


def _reorder_wide_columns(df: pl.DataFrame, ids: t.Sequence[str]) -> pl.DataFrame:
    """Place ids & row_index & row_type first"""
    front = [*ids, "row_index", "row_type"]
    rest = [c for c in df.columns if c not in front]
    return df.select([*front, *rest])


def _sort_wide(
    wide: pl.DataFrame,
    ids: t.Sequence[str] = SerializeTypes.ID_COLUMNS,
    collection_order: list[str] | None = None,
) -> pl.DataFrame:
    if collection_order:
        rank = {name: i + 1 for i, name in enumerate(collection_order)}
        wide = (
            wide.with_columns(
                pl.when(pl.col("row_type") == "base").then(0).otherwise(pl.col("row_type").replace(rank, default=len(rank) + 1)).alias("_rt_rank"),
            )
            .sort([*ids, "_rt_rank", "row_index"])
            .drop("_rt_rank")
        )
    else:
        wide = (
            wide.with_columns(
                pl.when(pl.col("row_type") == "base").then(0).otherwise(1).alias("_rt_grp"),
            )
            .sort([*ids, "_rt_grp", "row_type", "row_index"])
            .drop("_rt_grp")
        )
    return wide
