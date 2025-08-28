# harmonization/harmonizers/base.py
from abc import ABC, abstractmethod
import polars as pl
from typing import Optional, List, Dict, Callable, Sequence, NewType, TypeVar
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
    def _process_ecog(self) -> None:
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
        subject: str = "SubjectId",
        cols: list[str],
        order_by: list[str] | None = None,
        items_col: str = "items",
    ) -> pl.DataFrame:
        if order_by:
            df = df.sort([subject] + order_by)
        return df.group_by(subject, maintain_order=True).agg(
            pl.struct(cols).alias(items_col)
        )  # auto-collect into list per group

    @staticmethod
    def hydrate_list_field(
        packed: pl.DataFrame,
        *,
        items_col: str,
        model_cls,
        attr_map: dict[str, str],  # struct_key -> model_attr
        assign: callable,  # (patient: Patient, objs: list[model_cls]) to None
        patients: dict[str, "Patient"],
    ) -> None:
        for sid, items in packed.iter_rows():
            objs = []
            for s in items:  # s is dict-like
                obj = model_cls(patient_id=sid)
                for key, attr in attr_map.items():
                    setattr(obj, attr, s.get(key))
                objs.append(obj)
            assign(patients[sid], objs)

    @staticmethod
    def latest_per_subject(
        df: pl.DataFrame,
        *,
        subject: str = "SubjectId",
        date_col: str,  # required: pick latest by this col
        cols: Sequence[str],  # columns to keep (besides subject)
        any_non_null: Sequence[str] | None = None,  # drop rows where all are null
        ascending: bool = True,  # sort order for date
    ) -> pl.DataFrame:
        if any_non_null is None:
            any_non_null = cols
        out = (
            df.filter(
                pl.any_horizontal([pl.col(c).is_not_null() for c in any_non_null])
            )
            .select([subject, *cols])
            .sort([subject, date_col], descending=[False, not ascending])
            .group_by(subject)
            .tail(1)
        )
        return out

    @staticmethod
    def hydrate_singleton(
        latest: pl.DataFrame,
        *,
        subject: str,
        model_cls,
        field_map: dict[str, str],  # df_col to model_attr
        assign,  # (patient, obj) to None, e.g. lambda p,o: setattr(p,"ecog",o)
        patients: dict[str, "Patient"],
    ) -> None:
        for row in latest.iter_rows(named=True):
            sid = row[subject]
            obj = model_cls(patient_id=sid)
            for src, attr in field_map.items():
                setattr(obj, attr, row[src])
            assign(patients[sid], obj)
