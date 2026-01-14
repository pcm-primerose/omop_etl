from abc import ABC
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
    Counter,
)

from omop_etl.harmonization.models.domain.base import DomainBase
from omop_etl.harmonization.models.patient import Patient

log = getLogger(__name__)


@dataclass(frozen=True)
class ProcessorSpec:
    """
    Metadata for a processor method
    """

    name: str  # method name suffix
    item_type: type[DomainBase]  # target domain
    kind: Literal["singleton", "collection"]

    # singleton duplicate handling
    on_duplicate: Literal["error", "first", "last"] = "error"

    # collection mode
    mode: Literal["replace", "extend"] = "replace"

    # ordering for collections
    order_by: tuple[str, ...] = ()
    require_order_by: bool = False

    # schema strictness (None inherits from harmonizer)
    strict_schema: bool | None = None

    # skip patients not in patient_data
    skip_missing_patients: bool = False

    # col name overrides
    subject_col: str = "SubjectId"
    items_col: str = "items"


class BaseHarmonizer(ABC):
    # Subclasses define their processor registry
    SPECS: ClassVar[tuple[ProcessorSpec, ...]] = ()

    # Default strictness (can be overridden per-spec)
    strict_schema: bool = True

    def __init__(self, data: pl.DataFrame, trial_id: str):
        self.data = data
        self.trial_id = trial_id
        self.patient_data: dict[str, Patient] = {}
        self._validate_specs()  # Validate on init
        # ... existing init

    def _validate_specs(self) -> None:
        """Validate ProcessorSpec registry at init time."""
        # Check for duplicate processor names (O(n) with Counter)
        name_counts = Counter(s.name for s in self.SPECS)
        dupes = [n for n, count in name_counts.items() if count > 1]
        if dupes:
            raise ValueError(f"Duplicate processor names in SPECS: {dupes}")

        for spec in self.SPECS:
            # Validate order_by ⊆ CANONICAL_COLS
            if spec.order_by:
                canonical = set(spec.item_type.CANONICAL_COLS)
                invalid = set(spec.order_by) - canonical
                if invalid:
                    raise ValueError(f"{spec.name}: order_by contains columns not in {spec.item_type.__name__}.CANONICAL_COLS: {invalid}")

            # Validate subject_col is non-empty
            if not spec.subject_col:
                raise ValueError(f"{spec.name}: subject_col cannot be empty")

            # Validate mode is only meaningful for collections
            if spec.kind == "singleton" and spec.mode != "replace":
                raise ValueError(f"{spec.name}: mode={spec.mode!r} is invalid for singleton (only 'replace' is meaningful)")

            # Validate item_type resolves to a Patient attribute
            try:
                Patient.get_attr_for_type(spec.item_type)
            except KeyError as e:
                raise ValueError(f"{spec.name}: {spec.item_type.__name__} does not map to any Patient attribute") from e

        # Validate no two specs map to the same Patient attribute
        # Exception: multiple specs can share an attribute if ALL are collections with mode="extend"
        attr_to_specs: dict[str, list[ProcessorSpec]] = {}
        for spec in self.SPECS:
            attr = Patient.get_attr_for_type(spec.item_type)
            attr_to_specs.setdefault(attr, []).append(spec)

        for attr, specs in attr_to_specs.items():
            if len(specs) > 1:
                # Multiple specs targeting same attr - only valid if all are collection+extend
                all_extend = all(s.kind == "collection" and s.mode == "extend" for s in specs)
                if not all_extend:
                    spec_names = [s.name for s in specs]
                    raise ValueError(
                        f"Multiple specs map to same Patient attribute '{attr}': {spec_names}. "
                        f"This is only allowed when all are kind='collection' with mode='extend'."
                    )

    def _get_strictness(self, spec: ProcessorSpec) -> bool:
        """Resolve strictness: spec override > harmonizer default."""
        return spec.strict_schema if spec.strict_schema is not None else self.strict_schema

    @staticmethod
    def validate_schema(
        frame: pl.DataFrame,
        item_type: type[DomainBase],
        *,
        strict: bool = True,
        subject_col: str = "SubjectId",
    ) -> None:
        """Validate DataFrame columns match domain class schema exactly."""
        cols = frame.columns

        # Check subject_col is present
        if subject_col not in cols:
            raise ValueError(f"Missing subject_col {subject_col!r} for {item_type.__name__}: {cols}")

        # Check for duplicate columns (O(n) with Counter)
        counts = Counter(cols)
        dupes = [c for c, n in counts.items() if n > 1]
        if dupes:
            raise ValueError(f"Duplicate columns for {item_type.__name__}: {dupes}")

        # Ensure subject_col is not in CANONICAL_COLS (catches contract bugs)
        canonical = item_type.CANONICAL_COLS
        if subject_col in canonical:
            raise ValueError(f"{item_type.__name__}: subject_col {subject_col!r} must not be in CANONICAL_COLS")

        # Enforce exact column count: subject_col + CANONICAL_COLS, nothing else
        expected_count = 1 + len(canonical)
        if len(cols) != expected_count:
            raise ValueError(
                f"{item_type.__name__}: expected {expected_count} cols ({subject_col}+{len(canonical)}), got {len(cols)}: {cols}"
            )

        # Since no dupes + correct count, set equality is sufficient
        expected_set = set(canonical)
        actual_set = set(cols) - {subject_col}

        if actual_set != expected_set:
            msg = (
                f"{item_type.__name__} schema mismatch: "
                f"missing={sorted(expected_set - actual_set)}, "
                f"unknown={sorted(actual_set - expected_set)}"
            )
            if strict:
                raise ValueError(msg)
            else:
                log.warning(msg)

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

        If order_by_cols: globally sort by [subject_col, *order_by_cols] before grouping.
        If not order_by_cols: Maintain original inpur order.

        Args:
           df: Polars DataFrame containing at least [subject_col] + value_cols
           subject_col: Subject identifier column
           value_cols: Columns to pack into the struct per row
           order_by_cols: Optional additional columns to sort by within each subject
           items_col: Name of the output list-of-structs column
           require_order_by: If True and order_by_cols is None, raises ValueError

          Returns:
              pl.Dateframe[subject_col, items_col]
        """
        if require_order_by and not order_by_cols:
            raise ValueError("order_by_cols is required when require_order_by=True")

        if order_by_cols:
            df = df.sort([subject_col, *order_by_cols])

        out = df.group_by(subject_col, maintain_order=True).agg(pl.struct(list(value_cols)).alias(items_col)).select(subject_col, items_col)
        return out

    @staticmethod
    def hydrate_singleton(
        frame: pl.DataFrame,
        *,
        item_type: type[DomainBase],
        patients: dict[str, Patient],
        builder: Callable[[str, Mapping[str, Any]], Any] | None = None,
        subject_col: str = "SubjectId",
        skip_missing_patients: bool = False,
        on_duplicate: Literal["error", "first", "last"] = "error",
    ) -> None:
        """
        Hydrate a singleton field from DataFrame.

        Note: Schema validation should be done before calling this method.
        """
        target_attr = Patient.get_attr_for_type(item_type)
        build = builder or item_type.from_row

        # Track which patients we've seen (for duplicate detection)
        seen: set[str] = set()

        for row in frame.iter_rows(named=True):
            sid = row[subject_col]

            # Duplicate handling
            if sid in seen:
                if on_duplicate == "error":
                    raise ValueError(f"Duplicate singleton for patient {sid} in {item_type.__name__}")
                elif on_duplicate == "first":
                    continue  # Skip subsequent rows
                # else "last": fall through and overwrite
            seen.add(sid)

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

    @staticmethod
    def hydrate_collection_field(
        packed: pl.DataFrame,
        *,
        item_type: type[DomainBase],
        patients: dict[str, Patient],
        builder: Callable[[str, Mapping[str, Any]], Any] | None = None,
        subject_col: str = "SubjectId",
        items_col: str = "items",
        skip_missing_patients: bool = False,
        mode: Literal["replace", "extend"] = "replace",
    ) -> None:
        """
        Hydrate a collection field from packed DataFrame.

        Note: Schema validation should be done on the unpacked DataFrame
        before packing and calling this method.
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
            # Note: Patient.validate_collection coerces list -> tuple

            setattr(patient, target_attr, objs)

    def _ensure_patients_initialized(self) -> None:
        """Guard: ensure patient_data is populated before running processors."""
        if not self.patient_data:
            raise RuntimeError("patient_data is empty. Run _process_patient_id() first.")

    def _run_processors(self) -> None:
        """Run all registered processors with metrics logging."""
        self._ensure_patients_initialized()

        for spec in self.SPECS:
            processor = getattr(self, f"_process_{spec.name}", None)
            if processor is None:
                raise AttributeError(f"Processor _process_{spec.name} not found")

            df = processor()
            if df is None or df.is_empty():
                log.debug(f"{spec.name}: no data")
                continue

            # Validate schema BEFORE packing (applies to both singleton and collection)
            strict = self._get_strictness(spec)
            self.validate_schema(df, spec.item_type, strict=strict, subject_col=spec.subject_col)

            # Observability: log basic metrics
            self._log_processor_metrics(spec, df)

            # Wrap hydration in try/except to include spec.name in error context
            try:
                if spec.kind == "collection":
                    if spec.require_order_by and not spec.order_by:
                        raise ValueError(f"{spec.name}: require_order_by=True but order_by is empty")

                    packed = self.pack_structs(
                        df,
                        subject_col=spec.subject_col,
                        value_cols=spec.item_type.CANONICAL_COLS,
                        order_by_cols=spec.order_by or None,
                        items_col=spec.items_col,
                    )
                    self.hydrate_collection_field(
                        packed,
                        item_type=spec.item_type,
                        patients=self.patient_data,
                        subject_col=spec.subject_col,
                        items_col=spec.items_col,
                        skip_missing_patients=spec.skip_missing_patients,
                        mode=spec.mode,
                    )
                else:
                    self.hydrate_singleton(
                        df,
                        item_type=spec.item_type,
                        patients=self.patient_data,
                        subject_col=spec.subject_col,
                        skip_missing_patients=spec.skip_missing_patients,
                        on_duplicate=spec.on_duplicate,
                    )
            except Exception as e:
                raise ValueError(f"{spec.name}: hydration failed for {spec.item_type.__name__}") from e

    def _log_processor_metrics(self, spec: ProcessorSpec, df: pl.DataFrame) -> None:
        """Log basic observability metrics for a processor result."""
        row_count = df.height
        patient_count = df.select(spec.subject_col).n_unique()
        log.info(f"{spec.name}: {row_count} rows, {patient_count} patients")

        # Log null rates for soft-required columns
        for col in spec.item_type.SOFT_REQUIRED_COLS:
            if col in df.columns:
                null_count = df.select(pl.col(col).is_null().sum()).item()
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0
                if null_pct > 0:
                    log.warning(f"{spec.name}.{col}: {null_pct:.1f}% null ({null_count}/{row_count})")


# class BaseHarmonizer(ABC):
#     """
#     Abstract base class that defines the methods needed to produce
#     each final output variable. The idea is that each output variable
#     is computed by a dedicated method, which may pull data from different
#     sheets (i.e. from differently prefixed columns in the combined DataFrame).
#
#     Each processing methods updates the corresponding instance attributes.
#     The return objects are iteratively built during processing and returned
#     as one instance of the HarmonizedData class storing the harmonized data.
#     """
#
#     def __init__(self, data: pl.DataFrame, trial_id: str):
#         self.data = data
#         self.trial_id = trial_id
#         self.patient_data: Dict[str, Patient] = {}
#         self.medical_histories: List | None = []
#         self.previous_treatment_lines: List | None = []
#         self.ecog_assessments: List | None = []
#         self.adverse_events: List | None = []
#         self.clinical_benefits: List | None = []
#         self.quality_of_life_assessment: List | None = []
#
#     # main processing method
#     @abstractmethod
#     def process(self) -> HarmonizedData:
#         """Processes all data and returns a complete, harmonized structure"""
#         pass
#
#     # scalars
#     @abstractmethod
#     def _process_patient_id(self) -> None:
#         """Updates Patient object and uses patient ID as key, Dict[str, Patient]"""
#         pass
#
#     @abstractmethod
#     def _process_cohort_name(self) -> None:
#         """Process cohort name and instantiate Patient.cohort_name"""
#         pass
#
#     @abstractmethod
#     def _process_gender(self) -> None:
#         """Process gender and instantiate Patient.gender"""
#         pass
#
#     @abstractmethod
#     def _process_age(self) -> None:
#         """Process age and instantiate Patient.age"""
#         pass
#
#     @abstractmethod
#     def _process_date_of_death(self) -> None:
#         """Process date of death and instantiate Patient.date_of_death"""
#         pass
#
#     @abstractmethod
#     def _process_treatment_start_date(self) -> None:
#         """Process tumor type and hydrate to Patient.treatment_start_date"""
#         pass
#
#     @abstractmethod
#     def _process_evaluability(self) -> None:
#         """Process evaluability and hydrate to Patient.evaluability"""
#         pass
#
#     # singletons
#     @abstractmethod
#     def _process_date_lost_to_followup(self) -> None:
#         """Process date lost to followup and instantiate FollowUp singleton"""
#         pass
#
#     @abstractmethod
#     def _process_tumor_type(self) -> None:
#         """Process tumor type and instantiate to TumorType singleton"""
#         pass
#
#     @abstractmethod
#     def _process_ecog_baseline(self) -> None:
#         """Process ecog and instantiate to Ecog singleton"""
#         pass
#
#     @abstractmethod
#     def _process_study_drugs(self) -> None:
#         """Process study drugs and instantiate to StudyDrugs singleton"""
#         pass
#
#     @abstractmethod
#     def _process_biomarkers(self) -> None:
#         """Process biomarkers and instantiate to Biomarker singleton"""
#         pass
#
#     # collections
#     @abstractmethod
#     def _process_medical_histories(self) -> None:
#         """Process medical histories and instantiate List[MedicalHistory]"""
#         pass
#
#     @abstractmethod
#     def _process_previous_treatments(self) -> None:
#         """Process medical histories and instantiate List[PreviousTreatments]"""
#         pass
#
# @staticmethod
# def pack_structs(
#     df: pl.DataFrame,
#     *,
#     subject_col: str = "SubjectId",
#     value_cols: Sequence[str],
#     order_by_cols: Sequence[str] | None = None,
#     items_col: str = "items",
#     require_order_by: bool = False,
# ) -> pl.DataFrame:
#     """
#     Group rows by subject_col and collects value_cols per subject into a list of structs.
#
#     If order_by_cols: globally sort by [subject_col, *order_by_cols] before grouping.
#     If not order_by_cols: Maintain original inpur order.
#
#     Args:
#       df: Polars DataFrame containing at least [subject_col] + value_cols
#       subject_col: Subject identifier column
#       value_cols: Columns to pack into the struct per row
#       order_by_cols: Optional additional columns to sort by within each subject
#       items_col: Name of the output list-of-structs column
#       require_order_by: If True and order_by_cols is None, raises ValueError
#
#
#     Returns:
#         - pl.Dateframe[subject_col, items_col]
#     """
#     if require_order_by and not order_by_cols:
#         raise ValueError("order_by_cols is required when require_order_by=True")
#
#     if order_by_cols:
#         df = df.sort([subject_col, *order_by_cols])
#
#     out = df.group_by(subject_col, maintain_order=True).agg(pl.struct(list(value_cols)).alias(items_col)).select(subject_col, items_col)
#     return out
#
#     @staticmethod
#     def hydrate_collection_field(
#         packed: pl.DataFrame,
#         *,
#         builder: Callable[[str, Mapping[str, Any]], Any] | None = None,
#         skip_missing: bool | None = False,
#         subject_col: str = "SubjectId",
#         items_col: str = "items",
#         item_type: type,
#         patients: Dict[str, Patient],
#     ) -> None:
#         """
#         Hydrate a collection-valued patient field from a packed List[Struct] col: multiple instances per patient.
#         Iterates each subject row from packed: [subject_col, items_col], for each struct in items, builds a Python model object.
#         Allows instantiation of many-to-one fields in collection models.
#
#         The item_type is used to look up the attribute name via Patient.get_attr_for_type(),
#         preventing typos in string literals.
#
#         Defaults to raising error for missing patients.
#         """
#         target_attr = Patient.get_attr_for_type(item_type)
#
#         if builder is None:
#             raise ValueError("Provide builder")
#
#         for sid, items in packed.select(subject_col, items_col).iter_rows():
#             patient = patients.get(sid)
#             if patient is None:
#                 if skip_missing:
#                     continue
#                 raise KeyError(f"Patient {sid} not found in patients mapping")
#
#             objs: List[Any] = [builder(sid, s) for s in items]
#             setattr(patient, target_attr, objs)
#
#     @staticmethod
#     def hydrate_singleton(
#         frame: pl.DataFrame,
#         *,
#         builder: Callable[[str, Mapping[str, Any]], Any] | None = None,
#         skip_missing_patients: bool | None = False,
#         subject_col: str = "SubjectId",
#         item_type: type,
#         patients: Dict[str, Patient],
#     ) -> None:
#         """
#         Hydrate a singleton patient field from a DataFrame: single instance per patient.
#         Iterates each row, builds a Python model object using the builder.
#
#         The item_type is used to look up the attribute name via Patient.get_attr_for_type(),
#         preventing typos in string literals.
#
#         Defaults to raising error for missing patients.
#         """
#         target_attr = Patient.get_attr_for_type(item_type)
#
#         for row in frame.iter_rows(named=True):
#             sid = row[subject_col]
#             patient = patients.get(sid)
#             if patient is None:
#                 if skip_missing_patients:
#                     continue
#                 raise KeyError(f"Patient {sid} not found")
#
#             obj = builder(sid, row)
#             setattr(patient, target_attr, obj)
