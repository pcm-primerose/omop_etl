import logging
from typing import List, Optional, Set
from dataclasses import dataclass, field
import datetime as dt
from omop_etl.harmonization.validation.validators import StrictValidators

# These models represent validated, transformed and cleaned harmonized data
# as intermediate structures they don't map 1:1 to the CDM table.
# Basic flow is:
# 1. add method to subclass, implement it to process some data, see docs for what to extract
# 2. once data extracted, make datamodel storing this, using getters/setters, valiation/parsers, implement specific parsers if needed
# 3. add field to patient collection class


class TumorType:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self._icd10_code: Optional[str] = None
        self._icd10_description: Optional[str] = None
        self._main_tumor_type: Optional[str] = None
        self._main_tumor_type_code: Optional[int] = None
        self._cohort_tumor_type: Optional[str] = None
        self._other_tumor_type: Optional[str] = None
        self.updated_fields: Set[str] = set()
        # self.logger = logger if logger else logging.Logger todo: add logging later

    @property
    def icd10_code(self) -> Optional[int]:
        return self._icd10_code

    @icd10_code.setter
    def icd10_code(self, value: Optional[str]) -> None:
        self._icd10_code = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.icd10_code.fset.__name__
        )
        self.updated_fields.add(self.__class__.icd10_code.fset.__name__)

    @property
    def icd10_description(self) -> Optional[str]:
        return self._icd10_description

    @icd10_description.setter
    def icd10_description(self, value: Optional[str]) -> None:
        self._icd10_description = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.icd10_description.fset.__name__
        )
        self.updated_fields.add(self.__class__.icd10_description.fset.__name__)

    @property
    def main_tumor_type(self) -> Optional[str]:
        return self._main_tumor_type

    @main_tumor_type.setter
    def main_tumor_type(self, value: Optional[str]) -> None:
        self._main_tumor_type = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.main_tumor_type.fset.__name__
        )
        self.updated_fields.add(self.__class__.main_tumor_type.fset.__name__)

    @property
    def main_tumor_type_code(self) -> Optional[int]:
        return self._main_tumor_type_code

    @main_tumor_type_code.setter
    def main_tumor_type_code(self, value: Optional[int]) -> None:
        self._main_tumor_type_code = StrictValidators.validate_optional_int(
            value=value, field_name=self.__class__.main_tumor_type_code.fset.__name__
        )
        self.updated_fields.add(self.__class__.main_tumor_type_code.fset.__name__)

    @property
    def cohort_tumor_type(self) -> Optional[str]:
        return self._cohort_tumor_type

    @cohort_tumor_type.setter
    def cohort_tumor_type(self, value: Optional[str]) -> None:
        self._cohort_tumor_type = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.cohort_tumor_type.fset.__name__
        )
        self.updated_fields.add(self.__class__.cohort_tumor_type.fset.__name__)

    @property
    def other_tumor_type(self) -> Optional[str]:
        return self._other_tumor_type

    @other_tumor_type.setter
    def other_tumor_type(self, value: Optional[str]) -> None:
        self._other_tumor_type = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.other_tumor_type.fset.__name__
        )
        self.updated_fields.add(self.__class__.other_tumor_type.fset.__name__)

    def __str__(self):
        return (
            f"ICD10 description: {self.icd10_description}, "
            f"ICD10 code: {self.icd10_code}, "
            f"Main tumor type: {self.main_tumor_type}, "
            f"Other tumor type: {self.other_tumor_type}, "
            f"Cohort tumor type: {self.cohort_tumor_type}"
        )


class StudyDrugs:
    def __init__(self):
        self._primary_treatment_drug: Optional[str] = None
        self._primary_treatment_drug_code: Optional[int] = None
        self._secondary_treatment_drug: Optional[str] = None
        self._secondary_treatment_drug_code: Optional[int] = None
        self.updated_fields: Set[str] = set()

    @property
    def primary_treatment_drug(self) -> Optional[str]:
        return self._primary_treatment_drug

    @primary_treatment_drug.setter
    def primary_treatment_drug(self, value: Optional[str]) -> None:
        self._primary_treatment_drug = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.primary_treatment_drug.fset.__name__
        )
        self.updated_fields.add(self.__class__.primary_treatment_drug.fset.__name__)

    @property
    def secondary_treatment_drug(self) -> Optional[str]:
        return self._secondary_treatment_drug

    @secondary_treatment_drug.setter
    def secondary_treatment_drug(self, value: Optional[str]) -> None:
        self._secondary_treatment_drug = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.secondary_treatment_drug.fset.__name__,
        )
        self.updated_fields.add(self.__class__.secondary_treatment_drug.fset.__name__)

    @property
    def primary_treatment_drug_code(self) -> Optional[int]:
        return self._primary_treatment_drug_code

    @primary_treatment_drug_code.setter
    def primary_treatment_drug_code(self, value: Optional[int]) -> None:
        self._primary_treatment_drug_code = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.primary_treatment_drug_code.fset.__name__,
        )
        self.updated_fields.add(
            self.__class__.primary_treatment_drug_code.fset.__name__
        )

    @property
    def secondary_treatment_drug_code(self) -> Optional[int]:
        return self._secondary_treatment_drug_code

    @secondary_treatment_drug_code.setter
    def secondary_treatment_drug_code(self, value: Optional[int]) -> None:
        self._secondary_treatment_drug_code = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.secondary_treatment_drug_code.fset.__name__,
        )
        self.updated_fields.add(
            self.__class__.secondary_treatment_drug_code.fset.__name__
        )

    def __str__(self):
        return (
            f"Primary treatment drug: {self.primary_treatment_drug}, "
            f"Primary treatment drug code: {self.primary_treatment_drug_code}, "
            f"Secondary treatment drug: {self.secondary_treatment_drug}, "
            f"Secondary treatment drug code: {self.secondary_treatment_drug_code}"
        )


class Biomarkers:
    def __init__(self):
        self._gene_and_mutation: Optional[str] = None
        self._gene_and_mutation_code: Optional[int] = None
        self._cohort_target_name: Optional[str] = None
        self._cohort_target_mutation: Optional[str] = None
        self._event_date: Optional[dt.date] = None
        self.updated_fields: Set[str] = set()

    @property
    def gene_and_mutation(self) -> Optional[str]:
        return self._gene_and_mutation

    @gene_and_mutation.setter
    def gene_and_mutation(self, value: Optional[str]) -> None:
        self._gene_and_mutation = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.gene_and_mutation.fset.__name__
        )
        self.updated_fields.add(self.__class__.gene_and_mutation.fset.__name__)

    @property
    def gene_and_mutation_code(self) -> Optional[int]:
        return self._gene_and_mutation_code

    @gene_and_mutation_code.setter
    def gene_and_mutation_code(self, value: Optional[int]) -> None:
        self._gene_and_mutation_code = StrictValidators.validate_optional_int(
            value=value, field_name=self.__class__.gene_and_mutation_code.fset.__name__
        )
        self.updated_fields.add(self.__class__.gene_and_mutation_code.fset.__name__)

    @property
    def cohort_target_name(self) -> Optional[str]:
        return self._cohort_target_name

    @cohort_target_name.setter
    def cohort_target_name(self, value: Optional[str]) -> None:
        self._cohort_target_name = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.cohort_target_name.fset.__name__
        )
        self.updated_fields.add(self.__class__.cohort_target_name.fset.__name__)

    @property
    def cohort_target_mutation(self) -> Optional[str]:
        return self._cohort_target_mutation

    @cohort_target_mutation.setter
    def cohort_target_mutation(self, value: Optional[str]) -> None:
        self._cohort_target_mutation = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.cohort_target_mutation.fset.__name__
        )
        self.updated_fields.add(self.__class__.cohort_target_mutation.fset.__name__)

    @property
    def event_date(self) -> Optional[dt.date]:
        return self._event_date

    @event_date.setter
    def event_date(self, value: Optional[dt.date]) -> None:
        self._event_date = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.event_date.fset.__name__
        )
        self.updated_fields.add(self.__class__.event_date.fset.__name__)

    def __str__(self):
        return (
            f"Gene and mutation: {self.gene_and_mutation}, "
            f"Gene and mutation code: {self.gene_and_mutation_code}, "
            f"Cohort target name: {self.cohort_target_name}, "
            f"Cohort target mutation: {self.cohort_target_mutation}, "
            f"Event date: {self.event_date}"
        )


class FollowUp:
    def __init__(self):
        self._lost_to_followup: Optional[bool] = None
        self._date_lost_to_followup: Optional[dt.datetime] = None
        self.updated_fields: Set[str] = set()

    @property
    def lost_to_followup(self) -> Optional[bool]:
        return self._lost_to_followup

    @lost_to_followup.setter
    def lost_to_followup(self, value: Optional[bool]) -> None:
        self._lost_to_followup = StrictValidators.validate_optional_bool(
            value=value, field_name=self.__class__.lost_to_followup.fset.__name__
        )
        self.updated_fields.add(self.__class__.lost_to_followup.fset.__name__)

    @property
    def date_lost_to_followup(self) -> Optional[dt.date]:
        return self._date_lost_to_followup

    @date_lost_to_followup.setter
    def date_lost_to_followup(self, value: Optional[dt.date]) -> None:
        self._date_lost_to_followup = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.date_lost_to_followup.fset.__name__
        )
        self.updated_fields.add(self.__class__.date_lost_to_followup.fset.__name__)

    def __str__(self):
        return (
            f"Lost to followup status: {self.lost_to_followup}, "
            f"Date lost to followup: {self.date_lost_to_followup}"
        )


class Ecog:
    def __init__(self):
        self._description: Optional[str] = None
        self._grade: Optional[int] = None
        self._date: Optional[dt.date] = None
        self.updated_fields: Set[str] = set()

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, value: Optional[str]) -> None:
        self._description = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.description.fset.__name__
        )
        self.updated_fields.add(self.__class__.description.fset.__name__)

    @property
    def grade(self) -> Optional[int]:
        return self._grade

    @grade.setter
    def grade(self, value: Optional[int]) -> None:
        self._grade = StrictValidators.validate_optional_int(
            value=value, field_name=self.__class__.grade.fset.__name__
        )
        self.updated_fields.add(self.__class__.grade.fset.__name__)

    @property
    def date(self) -> Optional[dt.date]:
        return self._date

    @date.setter
    def date(self, value: Optional[dt.date]) -> None:
        self._date = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.date.fset.__name__
        )
        self.updated_fields.add(self.__class__.date.fset.__name__)

    def __str__(self):
        return (
            f"Ecog description: {self.description},  "
            f"Ecog grade: {self.grade}, "
            f"Ecog date: {self.date}"
        )


class MedicalHistory:
    def __init__(self):
        self._treatment_type: Optional[str] = None
        self._treatment_type_code: Optional[int] = None
        self._treatment_specification: Optional[str] = None
        self._treatment_start_date: Optional[dt.date] = None
        self._treatment_end_date: Optional[dt.date] = None
        self._num_previous_treatment_lines: Optional[int] = None
        self.updated_fields: Set[str] = set()

    @property
    def treatment_type(self) -> Optional[str]:
        return self._treatment_type

    @treatment_type.setter
    def treatment_type(self, value: Optional[str]) -> None:
        self._treatment_type = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.treatment_type.fset.__name__
        )
        self.updated_fields.add(self.__class__.treatment_type.fset.__name__)

    @property
    def treatment_type_code(self) -> Optional[int]:
        return self._treatment_type_code

    @treatment_type_code.setter
    def treatment_type_code(self, value: Optional[int]) -> None:
        self._treatment_type_code = StrictValidators.validate_optional_int(
            value=value, field_name=self.__class__.treatment_type_code.fset.__name__
        )
        self.updated_fields.add(self.__class__.treatment_type_code.fset.__name__)

    @property
    def treatment_specification(self) -> Optional[str]:
        return self._treatment_specification

    @treatment_specification.setter
    def treatment_specification(self, value: Optional[str]) -> None:
        self._treatment_specification = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.treatment_specification.fset.__name__
        )
        self.updated_fields.add(self.__class__.treatment_specification.fset.__name__)

    @property
    def treatment_start_date(self) -> Optional[dt.date]:
        return self._treatment_start_date

    @treatment_start_date.setter
    def treatment_start_date(self, value: Optional[dt.date]) -> None:
        self._treatment_start_date = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.treatment_start_date.fset.__name__
        )
        self.updated_fields.add(self.__class__.treatment_start_date.fset.__name__)

    @property
    def treatment_end_date(self) -> Optional[dt.date]:
        return self._treatment_end_date

    @treatment_end_date.setter
    def treatment_end_date(self, value: Optional[dt.date]) -> None:
        self._treatment_end_date = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.treatment_end_date.fset.__name__
        )
        self.updated_fields.add(self.__class__.treatment_end_date.fset.__name__)

    @property
    def num_previous_treatment_lines(self) -> Optional[int]:
        return self._num_previous_treatment_lines

    @num_previous_treatment_lines.setter
    def num_previous_treatment_lines(self, value: Optional[int]) -> None:
        self._num_previous_treatment_lines = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.num_previous_treatment_lines.fset.__name__,
        )
        self.updated_fields.add(
            self.__class__.num_previous_treatment_lines.fset.__name__
        )

    def __str__(self):
        return (
            f"Treatment type: {self.treatment_type}, "
            f"Treatment specification: {self.treatment_specification}, "
            f"Treatment start date: {self.treatment_start_date}, "
            f"Treatment end date: {self.treatment_end_date}, "
            f"Number of previous treatments: {self.num_previous_treatment_lines}"
        )


class Patient:
    """
    Stores all data for a patient
    """

    def __init__(self, patient_id: str, trial_id: str):
        self._patient_id = patient_id
        self._trial_id = trial_id
        self._patient_id: Optional[str] = patient_id
        self._cohort_name: Optional[str] = None
        self._age: Optional[int] = None
        self._sex: Optional[str] = None
        self._tumor_type: Optional[TumorType] = None
        self._study_drugs: Optional[StudyDrugs] = None
        self._biomarker: Optional[Biomarkers] = None
        self._date_of_death: Optional[dt.datetime] = None
        self._lost_to_followup: Optional[FollowUp] = None
        self._evaluable_for_efficacy_analysis: bool = False
        self._ecog: Optional[Ecog] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        """Patient ID (immutable)"""
        return self._patient_id

    @patient_id.setter
    def patient_id(self, value: Optional[str]) -> None:
        self._patient_id = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.patient_id.fset.__name__
        )
        self.updated_fields.add(self.__class__.patient_id.fset.__name__)

    @property
    def trial_id(self) -> str:
        """Trial ID (immutable)"""
        return self._trial_id

    @trial_id.setter
    def trial_id(self, value: Optional[str]) -> None:
        """Trial ID (immutable)"""
        self._trial_id = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.trial_id.fset.__name__
        )
        self.updated_fields.add(self.__class__.trial_id.fset.__name__)

    @property
    def cohort_name(self) -> str:
        return self._cohort_name

    @cohort_name.setter
    def cohort_name(self, value: Optional[str]) -> None:
        self._cohort_name = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.cohort_name.fset.__name__
        )
        self.updated_fields.add(self.__class__.cohort_name.fset.__name__)

    @property
    def age(self):
        return self._age

    @age.setter
    def age(self, value: Optional[str | int | None]) -> None:
        """Set age with validation"""
        self._age = StrictValidators.validate_optional_int(
            value=value, field_name=self.__class__.age.fset.__name__
        )
        self.updated_fields.add(self.__class__.age.fset.__name__)

    @property
    def sex(self):
        return self._sex

    @sex.setter
    def sex(self, value: Optional[str | None]) -> None:
        """Set sex with validation"""
        self._sex = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.sex.fset.__name__
        )
        self.updated_fields.add(self.__class__.sex.fset.__name__)

    @property
    def date_of_death(self):
        return self._date_of_death

    @date_of_death.setter
    def date_of_death(self, value: Optional[dt.date | None]) -> None:
        """Set date of death with validation"""
        self._date_of_death = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.date_of_death.fset.__name__
        )
        self.updated_fields.add(self.__class__.date_of_death.fset.__name__)

    @property
    def evaluable_for_efficacy_analysis(self):
        return self._evaluable_for_efficacy_analysis

    @evaluable_for_efficacy_analysis.setter
    def evaluable_for_efficacy_analysis(self, value: Optional[bool]) -> None:
        """Set evaluable for efficacy analysis status with validation"""
        self._date_of_death = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.evaluable_for_efficacy_analysis.fset.__name__,
        )
        self.updated_fields.add(
            self.__class__.evaluable_for_efficacy_analysis.fset.__name__
        )

    @property
    def tumor_type(self) -> Optional[TumorType]:
        return self._tumor_type

    @tumor_type.setter
    def tumor_type(self, value: Optional[TumorType | None]) -> None:
        if value and not isinstance(value, TumorType):
            raise ValueError(
                f"tumor_type must be {TumorType.__name__} instance or None, got {value} with type {type(value)}"
            )

        self._tumor_type = value
        self.updated_fields.add(TumorType.__name__)

    @property
    def study_drugs(self) -> Optional[StudyDrugs]:
        return self._study_drugs

    @study_drugs.setter
    def study_drugs(self, value: Optional[StudyDrugs | None]) -> None:
        if value and not isinstance(value, StudyDrugs):
            raise ValueError(
                f"study_drugs must be {StudyDrugs.__name__} instance or None, got {value} with type {type(value)}"
            )

        self._study_drugs = value
        self.updated_fields.add(StudyDrugs.__name__)

    @property
    def biomarker(self) -> Optional[Biomarkers]:
        return self._biomarker

    @biomarker.setter
    def biomarker(self, value: Optional[Biomarkers | None]) -> None:
        if value and not isinstance(value, Biomarkers):
            raise ValueError(
                f"biomarker must be {Biomarkers.__name__} instance or None, got {value} with type {type(value)}"
            )

        self._biomarker = value
        self.updated_fields.add(Biomarkers.__name__)

    @property
    def lost_to_followup(self) -> Optional[FollowUp]:
        return self._lost_to_followup

    @lost_to_followup.setter
    def lost_to_followup(self, value: Optional[FollowUp | None]) -> None:
        if value and not isinstance(value, FollowUp):
            raise ValueError(
                f"lost_to_followup must be {FollowUp.__name__} instance or None, got {value} with type {type(value)}"
            )

        self._lost_to_followup = value
        self.updated_fields.add(FollowUp.__name__)

    @property
    def ecog(self) -> Optional[Ecog]:
        return self._ecog

    @ecog.setter
    def ecog(self, value: Optional[Ecog | None]) -> None:
        if value and not isinstance(value, Ecog):
            raise ValueError(
                f"ecog must be {Ecog.__name__} instance or None, got {value} with type {type(value)}"
            )

        self._ecog = value
        self.updated_fields.add(Ecog.__name__)

    def get_updated_fields(self) -> Set[str]:
        return self.updated_fields

    def __str__(self):
        return (
            f"Patient {self.patient_id}: \n"
            f"trial_id={self.trial_id} \n"
            f"cohort_name={self.cohort_name} \n"
            f"sex={self.sex} \n"
            f"age={self.age} \n"
            f"tumor_type={self.tumor_type} \n"
            f"study_drugs={self.study_drugs} \n"
            f"biomarkers={self.biomarker} \n"
            f"date_of_death={self.date_of_death} \n"
            f"lost_to_followup={self.lost_to_followup} \n"
            f"evaluable_for_efficacy_analysis={self.evaluable_for_efficacy_analysis} \n"
            f"ecog={self.ecog} \n"
        )


@dataclass
class HarmonizedData:
    """
    Stores all patient data for a processed trial
    """

    trial_id: str
    patients: List[Patient] = field(default_factory=list)

    def __str__(self):
        patient_str = "\n".join(str(p) for p in self.patients)
        return f"Trial ID: {self.trial_id}\nPatients:\n{patient_str}"

    # medical_histories: List[MedicalHistory] = field(default_factory=list)
    # previous_treatments: List[PreviousTreatmentLine] = field(default_factory=list)
    # ecog_assessments: List[Ecog] = field(default_factory=list)
    # adverse_events: List[AdverseEvent] = field(default_factory=list)
    # clinical_benefits: List[ClinicalBenefit] = field(default_factory=list)
    # quality_of_life_assessments: List[QualityOfLife] = field(default_factory=list)

    # add get specific patient data method
    # and get all patient data
    # and specific trial data
    # return as dict method etc
