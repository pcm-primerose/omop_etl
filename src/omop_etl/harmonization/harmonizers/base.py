# harmonization/harmonizers/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass

import polars as pl
from typing import Optional, List, Dict
from omop_etl.harmonization.datamodels import Patient
from omop_etl.harmonization.datamodels import HarmonizedData

PatientDict = Dict[str, Patient]


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
        self.patient_data: PatientDict = {}
        self.medical_histories: Optional[List] = []
        self.previous_treatment_lines: Optional[List] = []
        self.ecog_assessments: Optional[List] = []
        self.adverse_events: Optional[List] = []
        self.clinical_benefits: Optional[List] = []
        self.quality_of_life_assessment: Optional[List] = []

    @abstractmethod
    def process(self) -> HarmonizedData:
        """Processes all data and returns a complete, harmonized structure"""
        pass

    @abstractmethod
    def _process_patient_id(self) -> None:
        """Updated Patient object and uses patient ID as key, Dict[str, Patient]"""
        pass

    @abstractmethod
    def _process_cohort_name(self) -> None:
        pass

    @abstractmethod
    def _process_gender(self) -> None:
        pass

    @abstractmethod
    def _process_age(self) -> None:
        pass

    @abstractmethod
    def _process_tumor_type(self) -> None:
        pass

    @abstractmethod
    def _process_study_drugs(self) -> None:
        pass

    @abstractmethod
    def _process_biomarkers(self) -> None:
        pass
