from abc import ABC, abstractmethod
import polars as pl
from typing import Optional, List, Dict
from datamodels import Patient


class BaseHarmonizer(ABC):
    """
    Abstract base class that defines the methods needed to produce
    each final output variable. The idea is that each output variable
    is computed by a dedicated method, which may pull data from different
    sheets (i.e. from differently prefixed columns in the combined DataFrame).
    """

    def __init__(self, data: pl.DataFrame, trial_id: str):
        self.data = data
        self.trial_id = trial_id
        self.patient_data: Optional[Dict[str, Patient]] = {}
        self.medical_histories: Optional[List] = []
        self.previous_treatment_lines: List = []
        self.ecog_assessments: List = []
        self.adverse_events: List = []
        self.clinical_benefits: List = []
        self.quality_of_life_assessment: List = []

    @abstractmethod
    def process(self):
        """Processes all data and returns a complete, harmonized structure"""
        pass

    @abstractmethod
    def _process_patient_id(self):
        pass

    @abstractmethod
    def _process_cohort_name(self):
        pass
