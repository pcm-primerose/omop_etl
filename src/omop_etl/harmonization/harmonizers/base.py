from abc import ABC, abstractmethod
from collections import Counter
from logging import getLogger
import polars as pl
from dataclasses import dataclass
from typing import (
    Literal,
    Callable,
    Sequence,
    Mapping,
    Any,
    ClassVar,
    TypeVar,
)

from omop_etl.harmonization.models.domain.base import DomainBase
from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.harmonization.models.patient import Patient

log = getLogger(__name__)

# processor function type: takes harmonizer instance, returns DataFrame or None
type ProcessorFn = Callable[["BaseHarmonizer"], pl.DataFrame | None]

# lets @scalar/@singleton/@collection decorators return the original function type
_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass(frozen=True)
class SpecBase:
    """Base class for all processor specs with common fields."""

    name: str
    process: ProcessorFn
    strict_schema: bool | None = None
    skip_missing_patients: bool = False
    subject_col: str = "SubjectId"


@dataclass(frozen=True)
class ScalarSpec(SpecBase):
    """Spec for scalar patient attributes (e.g., cohort_name, sex, age)."""

    kind: Literal["scalar"] = "scalar"
    target_attr: str = ""
    value_col: str = ""
    on_duplicate: Literal["error", "first", "last"] = "error"


@dataclass(frozen=True, kw_only=True)
class SingletonSpec(SpecBase):
    """Spec for singleton domain objects (one per patient)."""

    target_domain: type[DomainBase]
    kind: Literal["singleton"] = "singleton"
    on_duplicate: Literal["error", "first", "last"] = "error"


@dataclass(frozen=True, kw_only=True)
class CollectionSpec(SpecBase):
    """Spec for collection domain objects (multiple per patient)."""

    target_domain: type[DomainBase]
    kind: Literal["collection"] = "collection"
    mode: Literal["replace", "extend"] = "replace"
    order_by: tuple[str, ...] = ()
    require_order_by: bool = False
    items_col: str = "items"
    on_natural_key_conflict: Literal["error", "warn"] = "warn"


# union type for all specs
ProcessorSpec = ScalarSpec | SingletonSpec | CollectionSpec


# sentinel attribute name used to attach a spec to a decorated processor method.
# __init_subclass__ on BaseHarmonizer scans class attributes for this marker and
# collects the specs into the subclass's SPECS tuple in declaration order.
_SPEC_ATTR = "__processor_spec__"


def _derived_name(fn: Callable[..., Any]) -> str:
    """Strip the conventional `_process_` prefix to get the logical spec name."""
    name: str = getattr(fn, "__name__")
    return name.removeprefix("_process_")


def _check_natural_key_conflicts(
    objs: list[DomainBase],
    *,
    patient_id: str,
    item_type: type[DomainBase],
    policy: Literal["error", "warn"],
) -> None:
    """
    Detect natural-key collisions where the rows have differing data.

    Identical duplicates (same NK, same data) are assumed to be deduplicated
    upstream by the collection processor, so this only flags conflicts.
    Keeps the first occurrence.
    """
    seen: dict[tuple, DomainBase] = {}
    fields = item_type.data_fields()
    for obj in objs:
        nk = obj.natural_key()
        prior = seen.get(nk)
        if prior is None:
            seen[nk] = obj
            continue
        if all(getattr(prior, f) == getattr(obj, f) for f in fields):
            continue
        diffs = {f: (getattr(prior, f), getattr(obj, f)) for f in fields if getattr(prior, f) != getattr(obj, f)}
        msg = f"{item_type.__name__} natural-key conflict for patient {patient_id}: NK={nk} has conflicting values: {diffs}"
        if policy == "error":
            raise ValueError(msg)
        log.warning(msg)


def scalar(
    *,
    name: str | None = None,
    target_attr: str | None = None,
    value_col: str | None = None,
    on_duplicate: Literal["error", "first", "last"] = "error",
    skip_missing_patients: bool = False,
    subject_col: str = "SubjectId",
    strict_schema: bool | None = None,
) -> Callable[[_F], _F]:
    """
    Decorator: register a method as a scalar processor.

    By default, `name`, `target_attr`, and `value_col` are derived from the method
    name with the `_process_` prefix stripped, so `_process_sex` produces a spec
    with name="sex" / target_attr="sex" / value_col="sex". Provide explicit kwargs
    only when the method name doesn't match the desired Patient attribute or value
    column.
    """

    def decorator(fn: _F) -> _F:
        derived = _derived_name(fn)
        spec = ScalarSpec(
            name=name or derived,
            process=fn,
            target_attr=target_attr or derived,
            value_col=value_col or derived,
            on_duplicate=on_duplicate,
            skip_missing_patients=skip_missing_patients,
            subject_col=subject_col,
            strict_schema=strict_schema,
        )
        setattr(fn, _SPEC_ATTR, spec)
        return fn

    return decorator


def singleton(
    target_domain: type[DomainBase],
    *,
    name: str | None = None,
    on_duplicate: Literal["error", "first", "last"] = "error",
    skip_missing_patients: bool = False,
    subject_col: str = "SubjectId",
    strict_schema: bool | None = None,
) -> Callable[[_F], _F]:
    """
    Decorator: register a method as a singleton-domain processor.

    `target_domain` is required. `name` defaults to the method name with the
    `_process_` prefix stripped.
    """

    def decorator(fn: _F) -> _F:
        derived = _derived_name(fn)
        spec = SingletonSpec(
            name=name or derived,
            process=fn,
            target_domain=target_domain,
            on_duplicate=on_duplicate,
            skip_missing_patients=skip_missing_patients,
            subject_col=subject_col,
            strict_schema=strict_schema,
        )
        setattr(fn, _SPEC_ATTR, spec)
        return fn

    return decorator


def collection(
    target_domain: type[DomainBase],
    *,
    name: str | None = None,
    mode: Literal["replace", "extend"] = "replace",
    order_by: tuple[str, ...] = (),
    require_order_by: bool = False,
    items_col: str = "items",
    skip_missing_patients: bool = False,
    subject_col: str = "SubjectId",
    strict_schema: bool | None = None,
    on_natural_key_conflict: Literal["error", "warn"] = "warn",
) -> Callable[[_F], _F]:
    """
    Decorator: register a method as a collection-domain processor.

    `target_domain` is required. `name` defaults to the method name with the
    `_process_` prefix stripped.
    """

    def decorator(fn: _F) -> _F:
        derived = _derived_name(fn)
        spec = CollectionSpec(
            name=name or derived,
            process=fn,
            target_domain=target_domain,
            mode=mode,
            order_by=order_by,
            require_order_by=require_order_by,
            items_col=items_col,
            skip_missing_patients=skip_missing_patients,
            subject_col=subject_col,
            strict_schema=strict_schema,
            on_natural_key_conflict=on_natural_key_conflict,
        )
        setattr(fn, _SPEC_ATTR, spec)
        return fn

    return decorator


class BaseHarmonizer(ABC):
    """
    Abstract base class for harmonizing source data into domain models.

    Subclasses define a SPECS registry of ProcessorSpec entries.
    Each spec maps a processor method (_process_{name}) to a target domain class
    and hydration strategy (singleton vs collection).

    Workflow is enforced by run() template method:
        - _create_patients() creates Patient instances (subclass implements)
        - _run_processors() iterates SPECS, calling each processor
        - Processor output is validated and conformed to target schema
        - Domain objects are hydrated onto Patient instances

    Processors return DataFrames with a subset of data_fields().
    Unknown columns are errors, missing columns are filled with null.
    """

    # subclasses define process registry
    SPECS: ClassVar[tuple[ProcessorSpec, ...]] = ()
    strict_schema: bool = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Build SPECS from decorated processor methods at class-definition time.

        Walks the class's own attributes (not inherited) for callables tagged with
        a `__processor_spec__` marker (set by the @scalar/@singleton/@collection
        decorators) and collects them into `cls.SPECS` in declaration order.

        If no decorated methods are present, `cls.SPECS` is left as inherited (or
        as whatever was set manually on the class). Validation runs against the
        final SPECS, so typos and conflicts surface at import time rather than
        instance construction.
        """
        super().__init_subclass__(**kwargs)

        decorated_specs = tuple(getattr(attr, _SPEC_ATTR) for attr in vars(cls).values() if callable(attr) and hasattr(attr, _SPEC_ATTR))
        if decorated_specs:
            cls.SPECS = decorated_specs

        cls._validate_specs(cls.SPECS)

    def __init__(self, data: pl.DataFrame, trial_id: str):
        self.data = data
        self.trial_id = trial_id
        self.patient_data: dict[str, Patient] = {}

    def _has_columns(self, *cols: str) -> bool:
        """Check if all specified columns exist in self.data."""
        return all(col in self.data.columns for col in cols)

    def run(self) -> None:
        """
        Template method: executes harmonization pipeline in correct order.

        Creates Patient instances and run spec-based processors.
        Subclasses should not override this method, override the hooks instead.
        """
        self._create_patients()
        self._run_processors()

    @abstractmethod
    def _create_patients(self) -> None:
        """
        Create Patient instances and populate patient_data.
        Subclass must implement this to create Patient instances with at minimum patient_id.
        """
        ...

    def process(self) -> HarmonizedData:
        """Run harmonization and return the harmonized output. Subclasses override."""
        raise NotImplementedError(f"{type(self).__name__} must implement process()")

    @classmethod
    def _validate_specs(cls, specs: tuple[ProcessorSpec, ...]) -> None:
        """
        Validate a ProcessorSpec registry (typed variants).

        Called from __init_subclass__ at class-definition time, so misconfigured
        SPECS surface at import rather than at first instantiation.
        """
        name_counts = Counter(s.name for s in specs)
        dupes = [n for n, count in name_counts.items() if count > 1]
        if dupes:
            raise ValueError(f"Duplicate processor names in SPECS: {dupes}")

        for spec in specs:
            if not spec.subject_col:
                raise ValueError(f"{spec.name}: subject_col cannot be empty")

            if isinstance(spec, ScalarSpec):
                if not spec.target_attr:
                    raise ValueError(f"{spec.name}: scalar requires target_attr")
                if not spec.value_col:
                    raise ValueError(f"{spec.name}: scalar requires value_col")
                if not hasattr(Patient, spec.target_attr):
                    raise ValueError(f"{spec.name}: Patient has no attribute '{spec.target_attr}'")

            elif isinstance(spec, SingletonSpec):
                try:
                    Patient.get_attr_for_type(spec.target_domain)
                except KeyError as e:
                    raise ValueError(f"{spec.name}: {spec.target_domain.__name__} does not map to any Patient attribute") from e
                if Patient.get_kind_for_type(spec.target_domain) != "singleton":
                    raise ValueError(f"{spec.name}: @singleton used with {spec.target_domain.__name__}, but Patient maps it to a collection attribute")

            elif isinstance(spec, CollectionSpec):
                if spec.order_by:
                    canonical = set(spec.target_domain.data_fields())
                    invalid = set(spec.order_by) - canonical
                    if invalid:
                        raise ValueError(f"{spec.name}: order_by contains columns not in {spec.target_domain.__name__}.data_fields(): {invalid}")
                if Patient.get_kind_for_type(spec.target_domain) != "collection":
                    raise ValueError(f"{spec.name}: @collection used with {spec.target_domain.__name__}, but Patient maps it to a singleton attribute")

                try:
                    Patient.get_attr_for_type(spec.target_domain)
                except KeyError as e:
                    raise ValueError(f"{spec.name}: {spec.target_domain.__name__} does not map to any Patient attribute") from e

        # two specs should not map to same Patient attr,
        # unless all are collections with mode="extend"
        attr_to_specs: dict[str, list[ProcessorSpec]] = {}
        for spec in specs:
            if isinstance(spec, ScalarSpec):
                attr = spec.target_attr
            else:
                attr = Patient.get_attr_for_type(spec.target_domain)
            attr_to_specs.setdefault(attr, []).append(spec)

        for attr, mapped in attr_to_specs.items():
            if len(mapped) > 1:
                all_extend = all(isinstance(s, CollectionSpec) and s.mode == "extend" for s in mapped)
                if not all_extend:
                    spec_names = [s.name for s in mapped]
                    raise ValueError(
                        f"Multiple specs map to same Patient attribute '{attr}': {spec_names}. "
                        f"This is only allowed when all are CollectionSpec with mode='extend'."
                    )

    def _run_processors(self) -> None:
        """
        Run all registered processors with metrics logging.
        Uses callable dispatch: spec.process(self) instead of getattr.
        """
        self._ensure_patients_initialized()

        for spec in self.SPECS:
            self._run_spec(spec)

    def run_one(self, spec_name: str) -> None:
        """
        Execute a single spec by name.

        Args:
            spec_name: The name of the spec to run.

        Raises:
            ValueError: If the spec name is not found in SPECS.
        """
        self._ensure_patients_initialized()
        spec = next((s for s in self.SPECS if s.name == spec_name), None)
        if spec is None:
            raise ValueError(f"Unknown spec: {spec_name}")
        self._run_spec(spec)

    def _run_spec(self, spec: ProcessorSpec) -> None:
        """
        Execute a single processor spec: call processor, validate, conform, hydrate.
        Instantiates Patient object with data processed by processors in ProcessorSpec(s) provided.

        Args:
            spec: The ProcessorSpec to execute.
        """
        df = spec.process(self)
        if df is None or df.is_empty():
            log.debug(f"{spec.name}: no data")
            return

        try:
            if isinstance(spec, ScalarSpec):
                # scalar: minimal validation, direct attribute assignment
                if spec.subject_col not in df.columns:
                    raise ValueError(f"Missing {spec.subject_col} in scalar processor output")
                if spec.value_col not in df.columns:
                    raise ValueError(f"Missing {spec.value_col} in scalar processor output")

                log.info(f"{spec.name}: {df.height} rows (scalar -> {spec.target_attr})")

                self.hydrate_scalar(
                    df,
                    attr=spec.target_attr,
                    value_col=spec.value_col,
                    subject_col=spec.subject_col,
                    skip_missing_patients=spec.skip_missing_patients,
                    on_duplicate=spec.on_duplicate,
                )

            elif isinstance(spec, CollectionSpec):
                # collection: validate, conform, pack, hydrate
                strict = self._get_strictness(spec)
                self.validate_schema_subset(df, spec.target_domain, subject_col=spec.subject_col, strict_unknown=strict)
                df = self.conform_schema(df, spec.target_domain, subject_col=spec.subject_col)
                self._log_processor_metrics(spec, df)

                if spec.require_order_by and not spec.order_by:
                    raise ValueError(f"{spec.name}: require_order_by=True but order_by is empty")

                packed = self.pack_structs(
                    df,
                    subject_col=spec.subject_col,
                    value_cols=spec.target_domain.data_fields(),
                    order_by_cols=spec.order_by or None,
                    items_col=spec.items_col,
                )
                self.hydrate_collection_field(
                    packed,
                    item_type=spec.target_domain,
                    patients=self.patient_data,
                    subject_col=spec.subject_col,
                    items_col=spec.items_col,
                    skip_missing_patients=spec.skip_missing_patients,
                    mode=spec.mode,
                    on_natural_key_conflict=spec.on_natural_key_conflict,
                )

            elif isinstance(spec, SingletonSpec):
                # singleton: validate, conform, hydrate
                strict = self._get_strictness(spec)
                self.validate_schema_subset(df, spec.target_domain, subject_col=spec.subject_col, strict_unknown=strict)
                df = self.conform_schema(df, spec.target_domain, subject_col=spec.subject_col)
                self._log_processor_metrics(spec, df)

                self.hydrate_singleton(
                    df,
                    skip_missing_patients=spec.skip_missing_patients,
                    subject_col=spec.subject_col,
                    item_type=spec.target_domain,
                    patients=self.patient_data,
                    on_duplicate=spec.on_duplicate,
                )

        except Exception as e:
            target = spec.target_attr if isinstance(spec, ScalarSpec) else spec.target_domain.__name__
            raise ValueError(f"{spec.name}: hydration failed for {target}: {e}") from e

    def _get_strictness(self, spec: ProcessorSpec) -> bool:
        """Provided spec overrides harmonizer default."""
        return spec.strict_schema if spec.strict_schema is not None else self.strict_schema

    @staticmethod
    def validate_schema_subset(
        frame: pl.DataFrame,
        item_type: type[DomainBase],
        *,
        subject_col: str = "SubjectId",
        strict_unknown: bool = True,
    ) -> None:
        """
        DataFrame columns from processors must be valid subset of target domain schema.

        Args:
           frame (pl.DataFrame): DataFrame to validate.
           item_type (type[DomainBase]): Target domain model to validate against.
           subject_col (str): Subject column name.
           strict_unknown (bool): Raise ValueError if unknown columns, else log warning.
        """
        cols = frame.columns

        if subject_col not in cols:
            raise ValueError(f"Missing subject_col {subject_col!r} for {item_type.__name__}: {cols}")

        counts = Counter(cols)
        dupes = [c for c, n in counts.items() if n > 1]
        if dupes:
            raise ValueError(f"Duplicate columns for {item_type.__name__}: {dupes}")

        data_fields = set(item_type.data_fields())
        if subject_col in data_fields:
            raise ValueError(f"{item_type.__name__}: subject_col {subject_col!r} must not be in data_fields()")

        actual = set(cols) - {subject_col}
        unknown = actual - data_fields
        if unknown:
            msg = f"{item_type.__name__}: unknown columns {sorted(unknown)}"
            if strict_unknown:
                raise ValueError(msg)
            log.warning(msg)

    @staticmethod
    def conform_schema(
        frame: pl.DataFrame,
        item_type: type[DomainBase],
        *,
        subject_col: str = "SubjectId",
    ) -> pl.DataFrame:
        """
        Conform DataFrame to the full schema for a DomainBase datamodel.
        Columns not matching target domain are filled with pl.Null.
        Selects columns in order: [subject_col, *data_fields()].
        Extra columns (if not caught by validate_schema_subset) are dropped.

        Args:
           frame (pl.DataFrame): DataFrame emitted from processor to conform.
           item_type (type[DomainBase]): Target domain model to conform schema to.
           subject_col (str): Subject column name.

        Returns:
           pl.DataFrame[subject_col, *data_fields()]
        """
        fields = list(item_type.data_fields())
        missing = [f for f in fields if f not in frame.columns]
        if missing:
            frame = frame.with_columns([pl.lit(None).alias(f) for f in missing])
        return frame.select([subject_col, *fields])

    @staticmethod
    def pack_structs(
        df: pl.DataFrame,
        *,
        subject_col: str = "SubjectId",
        value_cols: Sequence[str],
        order_by_cols: Sequence[str] | None = None,
        items_col: str = "items",
        require_order_by: bool = False,
    ) -> pl.DataFrame:
        """
        Group rows by subject_col and collects value_cols per subject into a list of structs.

        Args:
          df (pl.DataFrame): DataFrame containing at least [subject_col] + value_cols.
          subject_col (str): Subject id column name.
          value_cols (Sequence[str]): Columns to pack into the struct per row.
          order_by_cols (Sequence[str]): Optional additional columns to sort by within each subject.
          items_col (str): Name of the output list-of-structs column.
          require_order_by (bool): If True and order_by_cols is None, raises ValueError.

        Returns:
            pl.DataFrame[subject_col, items_col]
        """
        if require_order_by and not order_by_cols:
            raise ValueError("order_by_cols is required when require_order_by=True")

        if order_by_cols:
            df = df.sort([subject_col, *order_by_cols])

        out = df.group_by(subject_col, maintain_order=True).agg(pl.struct(list(value_cols)).alias(items_col)).select(subject_col, items_col)
        return out

    def hydrate_scalar(
        self,
        frame: pl.DataFrame,
        *,
        attr: str,
        value_col: str,
        subject_col: str = "SubjectId",
        skip_missing_patients: bool = False,
        on_duplicate: Literal["error", "first", "last"] = "error",
    ) -> None:
        """
        Assign scalar values directly to Patient attributes.

        Args:
            frame: DataFrame with [subject_col, value_col].
            attr: Patient attribute name to set.
            value_col: Column containing the scalar value.
            subject_col: Column containing patient identifier.
            skip_missing_patients: If True, skip subjects not in patient_data.
            on_duplicate: "error" raises, "first" keeps first, "last" keeps last.
        """
        seen: set[str] = set()

        for row in frame.select(subject_col, value_col).iter_rows(named=True):
            sid = row[subject_col]

            if sid in seen:
                if on_duplicate == "error":
                    raise ValueError(f"Duplicate scalar for patient {sid} in {attr}")
                elif on_duplicate == "first":
                    continue
            seen.add(sid)

            patient = self.patient_data.get(sid)
            if patient is None:
                if skip_missing_patients:
                    continue
                raise KeyError(f"Patient {sid} not found")

            setattr(patient, attr, row[value_col])

    @staticmethod
    def hydrate_collection_field(
        packed: pl.DataFrame,
        *,
        builder: Callable[[str, Mapping[str, Any]], Any] | None = None,
        skip_missing_patients: bool = False,
        subject_col: str = "SubjectId",
        items_col: str = "items",
        item_type: type[DomainBase],
        patients: dict[str, Patient],
        mode: Literal["replace", "extend"] = "replace",
        on_natural_key_conflict: Literal["error", "warn"] = "warn",
    ) -> None:
        """
        Instantiate collection domain models onto Patient after schema validation.

        Hydrates a collection field from a packed List[Struct] column (multiple instances per patient).
        For each subject, iterates the list of structs and builds domain objects.

        Args:
           packed: DataFrame with [subject_col, items_col] where items_col is List[Struct].
           builder: Optional custom builder; defaults to item_type.from_row.
           skip_missing_patients: If True, skip subjects not in patients dict.
           subject_col: Column name for subject identifier.
           items_col: Column name for the packed list of structs.
           item_type: Target domain class (used to resolve Patient attribute).
           patients: Map of patient_id to Patient instance.
           mode: "replace" overwrites, "extend" appends to existing collection.
           on_natural_key_conflict: "warn" logs a warning when two instances share a natural key
               but differ in other field values; "error" raises ValueError. Identical duplicates
               (same NK, same data) are assumed to be deduplicated upstream.
        """
        target_attr = Patient.get_attr_for_type(item_type)
        build = builder or item_type.from_row

        for sid, items in packed.select(subject_col, items_col).iter_rows():
            patient = patients.get(sid)
            if patient is None:
                if skip_missing_patients:
                    continue
                raise KeyError(f"Patient {sid} not found")

            try:
                objs = [build(sid, s) for s in items]
            except Exception as e:
                raise ValueError(f"{item_type.__name__} collection hydration failed for {sid=}") from e

            if item_type.NATURAL_KEY_FIELDS:
                _check_natural_key_conflicts(
                    objs,
                    patient_id=sid,
                    item_type=item_type,
                    policy=on_natural_key_conflict,
                )

            if mode == "extend":
                existing = getattr(patient, target_attr, ()) or ()
                objs = list(existing) + objs

            setattr(patient, target_attr, objs)

    @staticmethod
    def hydrate_singleton(
        frame: pl.DataFrame,
        *,
        builder: Callable[[str, Mapping[str, Any]], Any] | None = None,
        skip_missing_patients: bool = False,
        subject_col: str = "SubjectId",
        item_type: type[DomainBase],
        patients: dict[str, Patient],
        on_duplicate: Literal["error", "first", "last"] = "error",
    ) -> None:
        """
        Instantiate singleton domain model onto Patient after schema validation.

        Hydrates a single-value field from a flat DataFrame (one instance per patient).
        Each row is built into a domain object and assigned to the patient.

        Args:
           frame: DataFrame with [subject_col, ...domain_cols].
           builder: Optional custom builder; defaults to item_type.from_row.
           skip_missing_patients: If True, skip subjects not in patients dict.
           subject_col: Column name for subject identifier.
           item_type: Target domain class (used to resolve Patient attribute).
           patients: Map of patient_id to Patient instance.
           on_duplicate: "error" raises, "first" keeps first, "last" keeps last.
        """
        target_attr = Patient.get_attr_for_type(item_type)
        build = builder or item_type.from_row

        seen_patients: set[str] = set()

        for row in frame.iter_rows(named=True):
            sid = row[subject_col]

            # duplicate handling, fall-through overwrites
            if sid in seen_patients:
                if on_duplicate == "error":
                    raise ValueError(f"Duplicate singleton for patient {sid} in {item_type.__name__}")
                elif on_duplicate == "first":
                    continue
            seen_patients.add(sid)

            patient = patients.get(sid)
            if patient is None:
                if skip_missing_patients:
                    continue
                raise KeyError(f"Patient {sid} not found")

            try:
                obj = build(sid, row)
            except Exception as e:
                raise ValueError(f"{item_type.__name__} hydration failed for {sid=}") from e

            setattr(patient, target_attr, obj)

    def _ensure_patients_initialized(self) -> None:
        """patient_data must be populated before processors run."""
        if not self.patient_data:
            raise RuntimeError("patient_data is empty. _create_patients() must populate it.")

    @staticmethod
    def _log_processor_metrics(spec: SingletonSpec | CollectionSpec, df: pl.DataFrame) -> None:
        """
        Log basic observability metrics for a processor result.

        Args:
            spec: Singleton or Collection spec to log (scalar specs have no target_domain).
            df (pl.DataFrame): Dataframe for that processor spec.
        """
        row_count = df.height
        patient_count = df.select(spec.subject_col).n_unique()
        log.info(f"{spec.name}: {row_count} rows, {patient_count} patients")

        # null rates for soft-required columns
        for col in spec.target_domain.data_fields():
            if col in df.columns:
                null_count = df.select(pl.col(col).is_null().sum()).item()
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0
                if null_pct > 0:
                    log.warning(f"{spec.name}.{col}: {null_pct:.1f}% null ({null_count}/{row_count})")
