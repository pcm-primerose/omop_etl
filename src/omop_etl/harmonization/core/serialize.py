import datetime as dt
import inspect
import typing
from typing import Any, Sequence, MutableSequence
from functools import lru_cache
import polars as pl
from polars._typing import PolarsDataType as polars_data_type

from omop_etl.infra.io.types import SerializeTypes
from omop_etl.infra.utils.types import unwrap_optional


def to_wide(df_nested: pl.DataFrame, prefix_sep: str = SerializeTypes.COL_SEP) -> pl.DataFrame:
    """
    Flatten a nested frame into a single wide frame:
      - one base row per patient (ids + scalars + singleton fields)
      - one row per item per collection (ids + row_index + collection fields)
      - row_type marks base or collection name
    """
    df = df_nested

    # unnest singletons
    for col_name, col_dtype in list(df.schema.items()):
        if col_dtype == pl.Struct:
            before = set(df.columns)
            df = df.unnest(col_name)
            new_cols = [col for col in df.columns if col not in before]
            if new_cols:
                df = df.rename({col: f"{col_name}{prefix_sep}{col}" for col in new_cols})

    # base branch: IDs + primitives + flattened singletons
    base_cols = [col for col, dtype in df.schema.items() if dtype not in (pl.Struct,) and not isinstance(dtype, pl.List)]
    base_only = [col for col in base_cols if col not in SerializeTypes.ID_COLUMNS]

    # build base rows
    base_sel = df.select([*SerializeTypes.ID_COLUMNS, *base_only])
    base_sel = _filter_nonempty_rows(base_sel, base_only)

    base = base_sel.with_columns(
        pl.lit(None, dtype=pl.Int64).alias("row_index"),
        pl.lit("base").alias("row_type"),
    )
    base = _reorder_wide_columns(df=base, id_cols=SerializeTypes.ID_COLUMNS)

    # collection parts: ids + row_index + fields + row_type with no scalar duplication
    parts: list[pl.DataFrame] = []
    for col_name, col_dtype in df.schema.items():
        if isinstance(col_dtype, pl.List):
            part = _explode_collection_into(df, col_name, SerializeTypes.ID_COLUMNS, sep=prefix_sep)
            if part is not None:
                part = part.with_columns(pl.lit(col_name).alias("row_type"))
                parts.append(_reorder_wide_columns(part, SerializeTypes.ID_COLUMNS))

    wide = pl.concat([base, *parts], how="diagonal_relaxed") if parts else base

    # ensure flat
    nested = [col for col, dtype in wide.schema.items() if dtype == pl.Struct or isinstance(dtype, pl.List)]
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
    tables: dict[str, pl.DataFrame] = {}

    # patients
    base_cols = [col for col, dtype in df_nested.schema.items() if dtype not in (pl.Struct,) and not isinstance(dtype, pl.List)]
    tables["patients"] = df_nested.select(
        [*SerializeTypes.ID_COLUMNS, *[col for col in base_cols if col not in SerializeTypes.ID_COLUMNS]]
    ).sort(pl.col("patient_id"))

    # singletons
    for col_name, col_dtype in df_nested.schema.items():
        if isinstance(col_dtype, pl.Struct):
            table = _unnest_singleton_into(df_nested, col_name, SerializeTypes.ID_COLUMNS, sep=SerializeTypes.COL_SEP)
            data_cols = [col for col in table.columns if col not in SerializeTypes.ID_COLUMNS]
            table = _filter_nonempty_rows(table, data_cols) if data_cols else table
            if any(isinstance(schema_dtype, pl.List) or isinstance(schema_dtype, pl.Struct) for schema_dtype in table.schema.values()):
                table = table.select(SerializeTypes.ID_COLUMNS)
            tables[col_name] = table

    # collections
    for col_name, col_dtype in df_nested.schema.items():
        if isinstance(col_dtype, pl.List):
            table = _explode_collection_into(df_nested, col_name, SerializeTypes.ID_COLUMNS, sep=SerializeTypes.COL_SEP)
            if table is None:
                continue
            if any(isinstance(schema_dtype, pl.List) or schema_dtype == pl.Struct for schema_dtype in table.schema.values()):
                raise RuntimeError(f"normalized '{col_name}' still nested")
            tables[col_name] = table

    return tables


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

    for patient in patients:
        row: dict[str, Any] = {}
        for prop_name, prop in _public_properties(patient_cls).items():
            value = getattr(patient, prop_name, None)
            return_type = _property_return_type(patient_cls, prop_name)
            base_type = unwrap_optional(return_type) if return_type else None
            origin = typing.get_origin(base_type) if base_type else None

            if value is None:
                row[prop_name] = [] if origin in (list, tuple, Sequence, MutableSequence) else None
                continue

            if origin in (list, tuple, Sequence, MutableSequence):
                row[prop_name] = [_export_leaf_object(item) for item in list(value)]
            elif isinstance(base_type, type) and schema.get(prop_name) == pl.Struct:
                row[prop_name] = _export_leaf_object(value)
            else:
                row[prop_name] = _to_polars_primitive(value)

        rows.append(row)

    return pl.DataFrame(rows, schema=schema, strict=False, infer_schema_length=0)


def _filter_nonempty_rows(df: pl.DataFrame, data_columns: list[str]) -> pl.DataFrame:
    """Remove rows where all data columns are null or empty strings."""
    if not data_columns:
        return df.head(0)
    nonempty = pl.any_horizontal([pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8).str.strip_chars() != "") for col in data_columns])
    return df.filter(nonempty)


def _py_to_pl(tp: Any) -> polars_data_type:
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


def _unify_dtypes(dtype_a: polars_data_type, dtype_b: polars_data_type) -> polars_data_type:
    if dtype_a == dtype_b:
        return dtype_a
    numeric_types: set[polars_data_type] = {pl.Int64, pl.Float64}
    if dtype_a in numeric_types and dtype_b in numeric_types:
        return pl.Float64
    if pl.Utf8 in (dtype_a, dtype_b):
        return pl.Utf8
    if dtype_a == pl.Datetime or dtype_b == pl.Datetime:
        return pl.Datetime
    if dtype_a == pl.Date and dtype_b == pl.Date:
        return pl.Date
    if (dtype_a == pl.Boolean and dtype_b == pl.Int64) or (dtype_b == pl.Boolean and dtype_a == pl.Int64):
        return pl.Int64
    return pl.Utf8


@lru_cache(maxsize=512)
def _public_properties(cls: type) -> dict[str, property]:
    """Public @property descriptors on a class."""
    return {name: prop for name, prop in inspect.getmembers(cls, lambda o: isinstance(o, property)) if not name.startswith("_")}


def _property_return_type(cls: type, name: str) -> Any | None:
    """
    Return the property's annotated return type, if not present, fall back to
    the setter value param annotation.
    Returns None if neither exists.
    """
    prop = getattr(cls, name, None)
    if not isinstance(prop, property):
        return None

    if prop.fget:
        getter_return_type = typing.get_type_hints(prop.fget).get("return")
        if getter_return_type is not None:
            return getter_return_type

    if prop.fset:
        setter_hints = typing.get_type_hints(prop.fset)
        if "value" in setter_hints:
            return setter_hints["value"]

    return None


def _leaf_field_hints(leaf_cls: type) -> dict[str, Any]:
    """
    Collect field type hints from @property returns and class __annotations__.
    Identity fields are excluded, undefined to not included.
    """
    hints: dict[str, Any] = {}
    hints.update(getattr(leaf_cls, "__annotations__", {}))
    for prop_name, prop in _public_properties(leaf_cls).items():
        if prop.fget:
            return_type = typing.get_type_hints(prop.fget).get("return")
            if return_type is not None:
                hints.setdefault(prop_name, return_type)
    for field_name in list(hints.keys()):
        if field_name in SerializeTypes.IDENTITY_FIELDS:
            hints.pop(field_name)

    return hints


def _leaf_struct_from_class(leaf_cls: type) -> pl.Struct:
    """Struct dtype composed from leaf class type hints only."""
    fields = {fname: _py_to_pl(tp) for fname, tp in _leaf_field_hints(leaf_cls).items()}
    return pl.Struct(fields)


def _to_polars_primitive(value: Any) -> Any:
    """
    Convert nested types to Polars-compatible Python values without stringifing dates.
    """
    if value is None or isinstance(value, (str, int, float, bool, dt.date, dt.datetime)):
        return value
    if isinstance(value, SerializeTypes.LIST_OR_TUPLE):
        return [_to_polars_primitive(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_polars_primitive(val) for key, val in value.items()}
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _to_polars_primitive(value.to_dict())
    if hasattr(value, "__dict__"):
        return {key.lstrip("_"): _to_polars_primitive(val) for key, val in value.__dict__.items() if not key.startswith("__")}
    return str(value)


def _export_leaf_object(obj: Any, *, exclude: set[str] = SerializeTypes.IDENTITY_FIELDS) -> dict[str, Any]:
    """
    Export an object as a dict. Prefer public @properties, if none, fall back
    to __dict__ for dynamic leaves (like C30/EQ5D).
    Identity fields are dropped.
    """
    props = _public_properties(obj.__class__)
    if props:
        return {
            prop_name: _to_polars_primitive(getattr(obj, prop_name))
            for prop_name, prop in props.items()
            if prop.fget and prop_name not in exclude
        }
    if hasattr(obj, "__dict__"):
        result: dict[str, Any] = {}
        for attr_name, attr_value in obj.__dict__.items():
            if attr_name.startswith("_") or attr_name in exclude:
                continue
            result[attr_name] = _to_polars_primitive(attr_value)
        return result
    return {}


def _patient_class_schema(patient_cls: type) -> dict[str, polars_data_type]:
    """
    Schema from class annotations & property returns:
      - scalars -> primitive dtype
      - Optional[Leaf] -> Struct
      - Sequence[Leaf] -> List[Struct]
    """
    schema: dict[str, polars_data_type] = {}
    for prop_name, prop in _public_properties(patient_cls).items():
        return_type = _property_return_type(patient_cls, prop_name)
        if return_type is None:
            # default Utf8 so if there are unannotated scalars they aren't dropped
            schema[prop_name] = pl.Utf8
            continue

        base_type = unwrap_optional(return_type)
        origin = typing.get_origin(base_type)

        # collections
        if origin in (list, tuple, Sequence, MutableSequence):
            type_args = typing.get_args(base_type)
            if type_args:
                element_type = unwrap_optional(type_args[0])
                if isinstance(element_type, type):
                    schema[prop_name] = pl.List(_leaf_struct_from_class(element_type))
            continue

        # singletons or scalars
        if isinstance(base_type, type):
            polars_dtype = _py_to_pl(base_type)
            if polars_dtype is pl.Utf8 and base_type not in (str, int, float, bool, dt.date, dt.datetime):
                schema[prop_name] = _leaf_struct_from_class(base_type)  # leaf class
            else:
                schema[prop_name] = polars_dtype

    # identities
    schema.setdefault("patient_id", pl.Utf8)
    schema.setdefault("trial_id", pl.Utf8)
    return schema


def _enrich_schema_from_data(patients: list, patient_cls: type, schema: dict[str, polars_data_type]) -> dict[str, polars_data_type]:
    """
    If any leaf Struct/List(Struct) is empty, derive fields from actual data.
    """
    enriched_schema = dict(schema)

    for prop_name, prop in _public_properties(patient_cls).items():
        return_type = _property_return_type(patient_cls, prop_name)
        base_type = unwrap_optional(return_type) if return_type else None
        origin = typing.get_origin(base_type) if base_type else None

        if isinstance(base_type, type) and enriched_schema.get(prop_name) == pl.Struct:
            fields: dict[str, pl.DataType] = {}
            for patient in patients:
                value = getattr(patient, prop_name, None)
                if value is None:
                    continue
                exported = _export_leaf_object(value)
                for field_name, field_value in exported.items():
                    inferred_dtype = _py_to_pl(type(field_value)) if field_value is not None else pl.Null
                    fields[field_name] = (
                        _unify_dtypes(fields.get(field_name, inferred_dtype), inferred_dtype) if field_name in fields else inferred_dtype
                    )

            if fields:
                enriched_schema[prop_name] = pl.Struct(
                    {field_name: (pl.Utf8 if field_dtype == pl.Null else field_dtype) for field_name, field_dtype in fields.items()}
                )

        # collections with empty inner struct: learn fields from data
        current_dtype = enriched_schema.get(prop_name)
        if origin in (list, tuple, Sequence, MutableSequence) and isinstance(current_dtype, pl.List):
            inner = current_dtype.inner
            if str(inner) == "Struct({})":
                fields: dict[str, pl.DataType] = {}
                for patient in patients:
                    sequence = getattr(patient, prop_name, ()) or ()
                    for item in sequence:
                        exported = _export_leaf_object(item)
                        for field_name, field_value in exported.items():
                            inferred_dtype = _py_to_pl(type(field_value)) if field_value is not None else pl.Null
                            fields[field_name] = (
                                _unify_dtypes(fields.get(field_name, inferred_dtype), inferred_dtype)
                                if field_name in fields
                                else inferred_dtype
                            )
                if fields:
                    enriched_schema[prop_name] = pl.List(
                        pl.Struct(
                            {field_name: (pl.Utf8 if field_dtype == pl.Null else field_dtype) for field_name, field_dtype in fields.items()}
                        )
                    )

    enriched_schema.setdefault("patient_id", pl.Utf8)
    enriched_schema.setdefault("trial_id", pl.Utf8)
    return enriched_schema


def _unnest_singleton_into(df: pl.DataFrame, struct_col: str, id_cols: Sequence[str], sep: str) -> pl.DataFrame:
    """Return a frame with only ids + prefixed fields from singleton struct attr."""
    if struct_col not in df.columns or not df.schema[struct_col] == pl.Struct:
        return df.select(id_cols)

    selected = df.select([*id_cols, struct_col])
    cols_before = set(selected.columns)
    unnested = selected.unnest(struct_col)
    new_fields = [col for col in unnested.columns if col not in cols_before]
    if not new_fields:
        return df.select(id_cols)
    rename_map = {col: f"{struct_col}{sep}{col}" for col in new_fields}
    return unnested.rename(rename_map).select([*id_cols, *rename_map.values()])


def _explode_collection_into(df: pl.DataFrame, list_col: str, id_cols: Sequence[str], sep: str) -> pl.DataFrame | None:
    """
    Return a frame with only ids & per-patient row_index & prefixed fields from list[struct] attr.
    """
    if list_col not in df.columns or not isinstance(df.schema[list_col], pl.List):
        return None
    exploded = df.select([*id_cols, list_col]).explode(list_col).filter(pl.col(list_col).is_not_null())

    # per-patient row_index
    exploded = exploded.with_columns(pl.int_range(0, pl.len()).over(id_cols).alias("row_index"))

    cols_before = set(exploded.columns)
    unnested = exploded.unnest(list_col)
    new_fields = [col for col in unnested.columns if col not in cols_before]
    if not new_fields:
        return None

    rename_map = {col: f"{list_col}{sep}{col}" for col in new_fields}
    return unnested.rename(rename_map).select([*id_cols, "row_index", *rename_map.values()])


def _reorder_wide_columns(df: pl.DataFrame, id_cols: Sequence[str]) -> pl.DataFrame:
    """Place ids & row_index & row_type first"""
    front = [*id_cols, "row_index", "row_type"]
    rest = [col for col in df.columns if col not in front]
    return df.select([*front, *rest])


def _sort_wide(
    wide: pl.DataFrame,
    id_cols: Sequence[str] = SerializeTypes.ID_COLUMNS,
    collection_order: list[str] | None = None,
) -> pl.DataFrame:
    if collection_order:
        rank = {name: idx + 1 for idx, name in enumerate(collection_order)}
        wide = (
            wide.with_columns(
                pl.when(pl.col("row_type") == "base")
                .then(0)
                .otherwise(pl.col("row_type").replace(rank, default=len(rank) + 1))
                .alias("_rt_rank"),
            )
            .sort([*id_cols, "_rt_rank", "row_index"])
            .drop("_rt_rank")
        )
    else:
        wide = (
            wide.with_columns(
                pl.when(pl.col("row_type") == "base").then(0).otherwise(1).alias("_rt_grp"),
            )
            .sort([*id_cols, "_rt_grp", "row_type", "row_index"])
            .drop("_rt_grp")
        )
    return wide
