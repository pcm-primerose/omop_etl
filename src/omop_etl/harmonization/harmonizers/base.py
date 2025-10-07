from abc import ABC, abstractmethod
import polars as pl
from typing import (
    Optional,
    List,
    Dict,
    Callable,
    Sequence,
    Mapping,
    Any,
)
from omop_etl.harmonization.datamodels import Patient
from omop_etl.harmonization.datamodels import HarmonizedData


class BaseHarmonizer(ABC):
    """
    Abstract base class that defines the methods needed to produce
    each final output variable. The idea is that each output variable
    is computed by a dedicated method, which may pull data from different
    sheets (i.e. from differently prefixed columns in the combined DataFrame).

    Each processing methods updates the corresponding instance attributes.
    The return objects are iteratively built during processing and returned
    as one instance of the HarmonizedData class storing the harmonized data.
    """

    def __init__(self, data: pl.DataFrame, trial_id: str):
        self.data = data
        self.trial_id = trial_id
        self.patient_data: Dict[str, Patient] = {}
        self.medical_histories: Optional[List] = []
        self.previous_treatment_lines: Optional[List] = []
        self.ecog_assessments: Optional[List] = []
        self.adverse_events: Optional[List] = []
        self.clinical_benefits: Optional[List] = []
        self.quality_of_life_assessment: Optional[List] = []

    # main processing method
    @abstractmethod
    def process(self) -> HarmonizedData:
        """Processes all data and returns a complete, harmonized structure"""
        pass

    # scalars
    @abstractmethod
    def _process_patient_id(self) -> None:
        """Updates Patient object and uses patient ID as key, Dict[str, Patient]"""
        pass

    @abstractmethod
    def _process_cohort_name(self) -> None:
        """Process cohort name and instantiate Patient.cohort_name"""
        pass

    @abstractmethod
    def _process_gender(self) -> None:
        """Process gender and instantiate Patient.gender"""
        pass

    @abstractmethod
    def _process_age(self) -> None:
        """Process age and instantiate Patient.age"""
        pass

    @abstractmethod
    def _process_date_of_death(self) -> None:
        """Process date of death and instantiate Patient.date_of_death"""
        pass

    @abstractmethod
    def _process_treatment_start_date(self) -> None:
        """Process tumor type and hydrate to Patient.treatment_start_date"""
        pass

    @abstractmethod
    def _process_evaluability(self) -> None:
        """Process evaluability and hydrate to Patient.evaluability"""
        pass

    # singletons
    @abstractmethod
    def _process_date_lost_to_followup(self) -> None:
        """Process date lost to followup and instantiate FollowUp singleton"""
        pass

    @abstractmethod
    def _process_tumor_type(self) -> None:
        """Process tumor type and instantiate to TumorType singleton"""
        pass

    @abstractmethod
    def _process_ecog_baseline(self) -> None:
        """Process ecog and instantiate to Ecog singleton"""
        pass

    @abstractmethod
    def _process_study_drugs(self) -> None:
        """Process study drugs and instantiate to StudyDrugs singleton"""
        pass

    @abstractmethod
    def _process_biomarkers(self) -> None:
        """Process biomarkers and instantiate to Biomarker singleton"""
        pass

    # collections
    @abstractmethod
    def _process_medical_histories(self) -> None:
        """Process medical histories and instantiate List[MedicalHistory]"""
        pass

    @abstractmethod
    def _process_previous_treatments(self) -> None:
        """Process medical histories and instantiate List[PreviousTreatments]"""
        pass

    @staticmethod
    def pack_structs(
        df: pl.DataFrame,
        *,
        subject_col: str = "SubjectId",
        value_cols: Sequence[str],
        order_by_cols: Optional[Sequence[str]] = None,
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
            - pl.Dateframe[subject_col, items_col]
        """
        if require_order_by and not order_by_cols:
            raise ValueError("order_by_cols is required when require_order_by=True")

        if order_by_cols:
            df = df.sort([subject_col, *order_by_cols])

        out = df.group_by(subject_col, maintain_order=True).agg(pl.struct(list(value_cols)).alias(items_col)).select(subject_col, items_col)
        return out

    @staticmethod
    def hydrate_list_field(
        packed: pl.DataFrame,
        *,
        builder: Optional[Callable[[str, Mapping[str, Any]], Any]] = None,
        skip_missing: Optional[bool] = False,
        subject_col: str = "SubjectId",
        items_col: str = "items",
        target_attr: Optional[str] = None,
        patients: Dict[str, Any],
    ) -> None:
        """
        Hydrate a list-valued patient field from a packed List[Struct] col: multiple instances per patient.
        Iterates each subject row from packed: [subject_col, items_col], for each struct in items, builds a Python model object.
        Allows instantiation of many-to-one fields in collection models.

        Defaults to raising error for missing patients.
        """
        if target_attr is None:
            raise ValueError("Provide either target_attr to attach objects to the patient")

        if builder is None:
            raise ValueError("Provide builder")

        for sid, items in packed.select(subject_col, items_col).iter_rows():
            patient = patients.get(sid)
            if patient is None:
                if skip_missing is True:
                    continue
                raise KeyError(f"Patient {sid} not found in patients mapping")

            objs: List[Any] = [builder(sid, s) for s in items]
            setattr(patient, target_attr, objs)

    @staticmethod
    def hydrate_singleton(
        frame: pl.DataFrame,
        *,
        builder: Optional[Callable[[str, Mapping[str, Any]], Any]] = None,
        skip_missing_patients: Optional[bool] = False,
        subject_col: str = "SubjectId",
        target_attr: Optional[str] = None,
        patients: Dict[str, Any],
    ) -> None:
        """
        Hydrate a singleton patient field from a packed List[Struct] col: single instance per patient.
        Iterates each subject row from packed: [subject_col, items_col], for each struct in items, builds a Python model object.
        Allows instantiation of many-to-one fields in collection models.

        Defaults to raising error for missing patients.
        """
        if not target_attr:
            raise ValueError("target_attr is required")

        for row in frame.iter_rows(named=True):
            sid = row[subject_col]
            patient = patients.get(sid)
            if patient is None:
                if skip_missing_patients is True:
                    continue
                raise KeyError(f"Patient {sid} not found")

            obj = builder(sid, row)
            setattr(patient, target_attr, obj)
