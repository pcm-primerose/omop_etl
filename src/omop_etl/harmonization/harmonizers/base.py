from abc import ABC
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
    - target_domain: target_domain of processor
    - kind: target_domain singleton or collection
    - on_duplicate: singleton duplication handling
    - order_by: order by key for collections
    - require_order_by: flag to require collection ordering
    - strict_schema: inherits from harmonizer
    - skip_missing_patients: flag to skip missing patients before instantiating
    - subject_col: subject column name
    - items_col: name of data columns for packed collections
    """

    name: str
    target_domain: type[DomainBase]
    kind: Literal["singleton", "collection"]
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

    Workflow:
    1. Subclass populates patient_data via _process_patient_id()
    2. _run_processors() iterates SPECS, calling each processor
    3. Processor output is validated and conformed to target schema
    4. Domain objects are hydrated onto Patient instances

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

    def _validate_specs(self) -> None:
        """
        Validate ProcessorSpec registry at init time.
        """
        name_counts = Counter(s.name for s in self.SPECS)
        dupes = [n for n, count in name_counts.items() if count > 1]
        if dupes:
            raise ValueError(f"Duplicate processor names in SPECS: {dupes}")

        for spec in self.SPECS:
            # order_by subset of CANONICAL_COLS
            if spec.order_by:
                canonical = set(spec.target_domain.CANONICAL_COLS)
                invalid = set(spec.order_by) - canonical
                if invalid:
                    raise ValueError(
                        f"{spec.name}: order_by contains columns not in {spec.target_domain.__name__}.CANONICAL_COLS: {invalid}"
                    )

            if not spec.subject_col:
                raise ValueError(f"{spec.name}: subject_col cannot be empty")

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

            # validate subset schema and conform to full schema
            strict = self._get_strictness(spec)
            self.validate_schema_subset(df, spec.target_domain, subject_col=spec.subject_col, strict_unknown=strict)
            df = self.conform_schema(df, spec.target_domain, subject_col=spec.subject_col)

            self._log_processor_metrics(spec, df)

            try:
                if spec.kind == "collection":
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
                    self.hydrate_singleton(
                        df,
                        skip_missing_patients=spec.skip_missing_patients,
                        subject_col=spec.subject_col,
                        item_type=spec.target_domain,
                        patients=self.patient_data,
                        on_duplicate=spec.on_duplicate,
                    )

            except Exception as e:
                raise ValueError(f"{spec.name}: hydration failed for {spec.target_domain.__name__}") from e

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

        out = (
            df.group_by(by=subject_col, maintain_order=True)
            .agg(pl.struct(list(value_cols)).alias(items_col))
            .select(subject_col, items_col)
        )
        return out

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
        """patient_data must be instantiated for processors to run"""
        if not self.patient_data:
            raise RuntimeError("patient_data is empty. Run _process_patient_id() first.")

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
