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
)

from omop_etl.harmonization.models.domain.base import DomainBase
from omop_etl.harmonization.models.patient import Patient

log = getLogger(__name__)


@dataclass(frozen=True)
class ProcessorSpec:
    """
    Metadata for a processor method:

    Fields:
    - name: processor spec method name suffix
    - kind: singleton, collection, or scalar
    - target_domain: target domain class (for singleton/collection)
    - target_attr: Patient attribute name (for scalar)
    - value_col: DataFrame column containing scalar value (for scalar)
    - on_duplicate: duplication handling (singleton/scalar)
    - order_by: order by key for collections
    - require_order_by: flag to require collection ordering
    - strict_schema: inherits from harmonizer
    - skip_missing_patients: flag to skip missing patients before instantiating
    - subject_col: subject column name
    - items_col: name of data columns for packed collections
    - mode: replace or extend (for collections)
    """

    name: str
    kind: Literal["singleton", "collection", "scalar"]
    target_domain: type[DomainBase] | None = None  # Required for singleton/collection
    target_attr: str | None = None  # Required for scalar (Patient attribute name)
    value_col: str | None = None  # Required for scalar (DataFrame column with value)
    on_duplicate: Literal["error", "first", "last"] = "error"
    mode: Literal["replace", "extend"] = "replace"
    order_by: tuple[str, ...] = ()
    require_order_by: bool = False
    strict_schema: bool | None = None
    skip_missing_patients: bool = False
    subject_col: str = "SubjectId"
    items_col: str = "items"


class BaseHarmonizer(ABC):
    """
    Abstract base class for harmonizing source data into domain models.

    Subclasses define a SPECS registry of ProcessorSpec entries.
    Each spec maps a processor method (_process_{name}) to a target domain class
    and hydration strategy (singleton vs collection).

    Workflow (enforced by run() template method):
    1. _create_patients() creates Patient instances (subclass implements)
    2. _run_legacy_processors() runs old-style processors (hook, default no-op)
    3. _run_processors() iterates SPECS, calling each processor
    4. Processor output is validated and conformed to target schema
    5. Domain objects are hydrated onto Patient instances

    Processors return DataFrames with a subset of CANONICAL_COLS.
    Unknown columns are errors, missing columns are filled with null.
    """

    # subclasses define process registry
    SPECS: ClassVar[tuple[ProcessorSpec, ...]] = ()
    strict_schema: bool = True

    def __init__(self, data: pl.DataFrame, trial_id: str):
        self.data = data
        self.trial_id = trial_id
        self.patient_data: dict[str, Patient] = {}
        self._validate_specs()

    def run(self) -> None:
        """
        Template method: executes harmonization pipeline in correct order.

        1. _create_patients() - creates Patient instances
        2. _run_legacy_processors() - runs old-style processors (hook, default no-op)
        3. _run_processors() - runs SPECS-based processors

        Subclasses should not override this method. Override the hooks instead.
        """
        self._create_patients()
        self._run_legacy_processors()
        self._run_processors()

    def _run_legacy_processors(self) -> None:
        """
        Hook for old-style processors not yet migrated to SPECS.

        Override in subclass to call legacy _process_* methods.
        Called after _create_patients() and before _run_processors().
        """
        pass

    @abstractmethod
    def _create_patients(self) -> None:
        """
        Create Patient instances and populate patient_data.

        Subclass must implement this to create Patient instances with at minimum
        patient_id. Called by run() before _run_legacy_processors().

        Example:
            for row in self.data.select("SubjectId").unique().iter_rows(named=True):
                pid = row["SubjectId"]
                self.patient_data[pid] = Patient(patient_id=pid, trial_id=self.trial_id)
        """
        ...

    def _validate_specs(self) -> None:
        """
        Validate ProcessorSpec registry at init time.
        """
        name_counts = Counter(s.name for s in self.SPECS)
        dupes = [n for n, count in name_counts.items() if count > 1]
        if dupes:
            raise ValueError(f"Duplicate processor names in SPECS: {dupes}")

        for spec in self.SPECS:
            if not spec.subject_col:
                raise ValueError(f"{spec.name}: subject_col cannot be empty")

            if spec.kind == "scalar":
                # Scalar validation
                if not spec.target_attr:
                    raise ValueError(f"{spec.name}: scalar requires target_attr")
                if not spec.value_col:
                    raise ValueError(f"{spec.name}: scalar requires value_col")
                if not hasattr(Patient, spec.target_attr):
                    raise ValueError(f"{spec.name}: Patient has no attribute '{spec.target_attr}'")
            else:
                # Singleton/collection validation
                if not spec.target_domain:
                    raise ValueError(f"{spec.name}: {spec.kind} requires target_domain")

                if spec.order_by:
                    canonical = set(spec.target_domain.CANONICAL_COLS)
                    invalid = set(spec.order_by) - canonical
                    if invalid:
                        raise ValueError(
                            f"{spec.name}: order_by contains columns not in {spec.target_domain.__name__}.CANONICAL_COLS: {invalid}"
                        )

                if spec.kind == "singleton" and spec.mode != "replace":
                    raise ValueError(f"{spec.name}: mode={spec.mode!r} is invalid for singleton (only 'replace' is meaningful)")

                try:
                    Patient.get_attr_for_type(spec.target_domain)
                except KeyError as e:
                    raise ValueError(f"{spec.name}: {spec.target_domain.__name__} does not map to any Patient attribute") from e

        # two specs should not map to same Patient attr,
        # unless all are collections with mode="extend"
        attr_to_specs: dict[str, list[ProcessorSpec]] = {}
        for spec in self.SPECS:
            if spec.kind == "scalar":
                attr = spec.target_attr
            else:
                attr = Patient.get_attr_for_type(spec.target_domain)
            attr_to_specs.setdefault(attr, []).append(spec)

        for attr, specs in attr_to_specs.items():
            if len(specs) > 1:
                all_extend = all(s.kind == "collection" and s.mode == "extend" for s in specs)
                if not all_extend:
                    spec_names = [s.name for s in specs]
                    raise ValueError(
                        f"Multiple specs map to same Patient attribute '{attr}': {spec_names}. "
                        f"This is only allowed when all are kind='collection' with mode='extend'."
                    )

    def _run_processors(self) -> None:
        """
        Run all registered processors with metrics logging.
        """

        self._ensure_patients_initialized()

        for spec in self.SPECS:
            processor = getattr(self, f"_process_{spec.name}", None)
            if processor is None:
                raise AttributeError(f"Processor _process_{spec.name} not found")

            df = processor()
            if df is None or df.is_empty():
                log.debug(f"{spec.name}: no data")
                continue

            try:
                if spec.kind == "scalar":
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
                elif spec.kind == "collection":
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
                        value_cols=spec.target_domain.CANONICAL_COLS,
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
                    )
                else:
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
                target = spec.target_attr if spec.kind == "scalar" else spec.target_domain.__name__
                raise ValueError(f"{spec.name}: hydration failed for {target}") from e

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

        canonical = set(item_type.CANONICAL_COLS)
        if subject_col in canonical:
            raise ValueError(f"{item_type.__name__}: subject_col {subject_col!r} must not be in CANONICAL_COLS")

        actual = set(cols) - {subject_col}
        unknown = actual - canonical
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
        Conform DataFrame to the full canonical schema for a DomainBase datamodel.
        Columns not matching target domain are filled with pl.Null.
        Selects columns in order: [subject_col, *`target_domain_canonical_cols`].
        Extra columns (if not caught by validate_schema_subset) are dropped.

        Args:
           frame (pl.DataFrame): DataFrame emitted from processor to conform.
           item_type (type[DomainBase]): Target domain model to conform schema to.
           subject_col (str): Subject column name.

        Returns:
           pl.DataFrame[subject_col, *CANONICAL_COLS]
        """
        canonical = list(item_type.CANONICAL_COLS)
        missing = [c for c in canonical if c not in frame.columns]
        if missing:
            frame = frame.with_columns([pl.lit(None).alias(c) for c in missing])
        return frame.select([subject_col, *canonical])

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
    def _log_processor_metrics(spec: ProcessorSpec, df: pl.DataFrame) -> None:
        """Log basic observability metrics for a processor result."""
        row_count = df.height
        patient_count = df.select(spec.subject_col).n_unique()
        log.info(f"{spec.name}: {row_count} rows, {patient_count} patients")

        # null rates for soft-required columns
        for col in spec.target_domain.CANONICAL_COLS:
            if col in df.columns:
                null_count = df.select(pl.col(col).is_null().sum()).item()
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0
                if null_pct > 0:
                    log.warning(f"{spec.name}.{col}: {null_pct:.1f}% null ({null_count}/{row_count})")
