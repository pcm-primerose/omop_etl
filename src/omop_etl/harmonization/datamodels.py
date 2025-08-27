# harmoinzation/datamodels.py

from typing import List, Optional, Set
from dataclasses import dataclass, field
import datetime as dt
from logging import getLogger
from omop_etl.harmonization.validation.validators import StrictValidators

# These models represent validated, transformed and cleaned harmonized data
# as intermediate structures they don't map 1:1 to the CDM table.
# Basic flow is:
# 1. add method to subclass, implement it to process some data, see docs for what to extract
# 2. once data extracted, make datamodel storing this, using getters/setters, valiation/parsers, implement specific parsers if needed
# 3. add field to patient collection class

log = getLogger(__name__)


class TumorType:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._icd10_code: Optional[str] = None
        self._icd10_description: Optional[str] = None
        self._main_tumor_type: Optional[str] = None
        self._main_tumor_type_code: Optional[int] = None
        self._cohort_tumor_type: Optional[str] = None
        self._other_tumor_type: Optional[str] = None
        self.updated_fields: Set[str] = set()

    @property
    def icd10_code(self) -> Optional[str]:
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

    def __str__(self) -> str:
        return (
            f"icd10_code={self.icd10_code} {'\n'}"
            f"icd10_description={self.icd10_description} {'\n'}"
            f"main_tumor_type={self.main_tumor_type} {'\n'}"
            f"main_tumor_type_code={self.main_tumor_type_code} {'\n'}"
            f"other_tumor_type={self.other_tumor_type} {'\n'}"
            f"cohort_tumor_type={self.cohort_tumor_type} {'\n'}"
        )


class StudyDrugs:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
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
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
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
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._lost_to_followup: Optional[bool] = None
        self._date_lost_to_followup: Optional[dt.datetime] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

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
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._description: Optional[str] = None
        self._grade: Optional[int] = None
        self._date: Optional[dt.date] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

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
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._term: Optional[str] = None
        self._sequence_id: Optional[int] = None
        self._start_date: Optional[dt.date] = None
        self._end_date: Optional[dt.date] = None
        self._status: Optional[dt.date] = None
        self._status_code: Optional[int] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def term(self) -> Optional[str]:
        return self._term

    @term.setter
    def term(self, value: Optional[str]) -> None:
        self._term = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.term.fset.__name__
        )
        self.updated_fields.add(self.__class__.term.fset.__name__)

    @property
    def sequence_id(self) -> Optional[int]:
        return self._sequence_id

    @sequence_id.setter
    def sequence_id(self, value: Optional[int]) -> None:
        self._sequence_id = StrictValidators.validate_optional_int(
            value=value, field_name=self.__class__.sequence_id.fset.__name__
        )
        self.updated_fields.add(self.__class__.sequence_id.fset.__name__)

    @property
    def start_date(self) -> Optional[dt.date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[dt.date]) -> None:
        self._start_date = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.start_date.fset.__name__
        )
        self.updated_fields.add(self.__class__.start_date.fset.__name__)

    @property
    def end_date(self) -> Optional[dt.date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[dt.date]) -> None:
        self._end_date = StrictValidators.validate_optional_date(
            value=value, field_name=self.__class__.end_date.fset.__name__
        )
        self.updated_fields.add(self.__class__.end_date.fset.__name__)

    @property
    def status(self) -> Optional[str]:
        return self._status

    @status.setter
    def status(self, value: Optional[str]) -> None:
        self._status = StrictValidators.validate_optional_str(
            value=value, field_name=self.__class__.status.fset.__name__
        )
        self.updated_fields.add(self.__class__.status.fset.__name__)

    @property
    def status_code(self) -> Optional[int]:
        return self._status_code

    @status_code.setter
    def status_code(self, value: Optional[int]) -> None:
        self._status_code = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.status_code.fset.__name__,
        )
        self.updated_fields.add(self.__class__.status_code.fset.__name__)

    def __str__(self):
        return (
            f"patient_id={self._patient_id!r}\n"
            f"term={self.term!r}\n"
            f"sequence_id={self.sequence_id!r}\n"
            f"start_date={self.start_date!r}\n"
            f"end_date={self.end_date!r}\n"
            f"status={self.status!r}\n"
            f"status_code={self.status_code!r}\n"
        )


class PreviousTreatments:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._treatment: Optional[str] = None  # cttype
        self._treatment_code: Optional[int] = None  # cttypecd
        self._treatment_sequence_number: Optional[int] = None  # ctspid
        self._start_date: Optional[dt.date] = None  # ctstdat
        self._end_date: Optional[dt.date] = None  # ctendat
        self._additional_treatment: Optional[
            str
        ] = None  # cttypesp (specification if "Other" in main treatment)
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def treatment(self) -> Optional[str]:
        return self._treatment

    @treatment.setter
    def treatment(self, value: Optional[str]) -> None:
        self._treatment = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.treatment.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment.fset.__name__)

    @property
    def treatment_code(self) -> Optional[int]:
        return self._treatment_code

    @treatment_code.setter
    def treatment_code(self, value: Optional[int]) -> None:
        self._treatment_code = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.treatment_code.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment_code.fset.__name__)

    @property
    def treatment_sequence_number(self) -> Optional[int]:
        return self._treatment_sequence_number

    @treatment_sequence_number.setter
    def treatment_sequence_number(self, value: Optional[int]) -> None:
        self._treatment_sequence_number = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.treatment_sequence_number.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment_sequence_number.fset.__name__)

    @property
    def start_date(self) -> Optional[dt.date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[dt.date]) -> None:
        self._start_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.start_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.start_date.fset.__name__)

    @property
    def end_date(self) -> Optional[dt.date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[dt.date]) -> None:
        self._end_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.end_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.end_date.fset.__name__)

    @property
    def additional_treatment(self) -> Optional[str]:
        return self._additional_treatment

    @additional_treatment.setter
    def additional_treatment(self, value: Optional[str]) -> None:
        self._additional_treatment = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.additional_treatment.fset.__name__,
        )
        self.updated_fields.add(self.__class__.additional_treatment.fset.__name__)

    def __str__(self):
        return (
            f"patient_id={self._patient_id!r}\n"
            f"treatment={self.treatment!r}\n"
            f"treatment_code={self.treatment_code!r}\n"
            f"treatment_sequence_number={self.treatment_sequence_number!r}\n"
            f"start_date={self.start_date!r}\n"
            f"end_date={self.end_date!r}\n"
            f"additional_treatment={self.additional_treatment!r}\n"
        )


class Patient:
    """
    Stores all data for a patient
    """

    def __init__(self, patient_id: str, trial_id: str):
        self.updated_fields: Set[str] = set()
        self._patient_id = patient_id
        self._trial_id = trial_id
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
        self._medical_history: Optional[List[MedicalHistory]] = None
        self._previous_treatments: Optional[PreviousTreatments] = None
        self._treatment_start_date: Optional[dt.date] = None

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
        self._evaluable_for_efficacy_analysis = StrictValidators.validate_optional_bool(
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
        if value is not None:
            value._patient_id = self._patient_id

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
        if value is not None:
            value._patient_id = self._patient_id

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
        if value is not None:
            value._patient_id = self._patient_id

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
        if value is not None:
            value._patient_id = self._patient_id

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
        if value is not None:
            value._patient_id = self._patient_id

        self._ecog = value
        self.updated_fields.add(Ecog.__name__)

    @property
    def medical_history(self) -> Optional[MedicalHistory]:
        return self._medical_history

    @medical_history.setter
    def medical_history(self, value: Optional[List[MedicalHistory]] | None) -> None:
        if value and not isinstance(value, List):
            raise ValueError(
                f"medical_history must be List[{MedicalHistory.__name__}] or None, got {type(value)}"
            )
        for val in value:
            if not isinstance(val, MedicalHistory):
                raise ValueError(
                    f"Expected List[{MedicalHistory.__name__}] or None, got {type(value)}"
                )
            val._patient_id = self._patient_id

        for mh in value:
            self._medical_history = mh
            self.updated_fields.add(mh.__class__.__name__)

    @property
    def previous_treatments(self) -> Optional[PreviousTreatments]:
        return self._previous_treatments

    @previous_treatments.setter
    def previous_treatments(self, value: Optional[PreviousTreatments]) -> None:
        if value is not None and not isinstance(value, PreviousTreatments):
            raise ValueError(
                f"previous_treatments must be {PreviousTreatments.__name__} or None, got {type(value)}"
            )
        if value is not None:
            value._patient_id = self._patient_id

        self._previous_treatments = value
        self.updated_fields.add(PreviousTreatments.__name__)

    @property
    def treatment_start_date(self) -> Optional[dt.date]:
        return self._treatment_start_date

    @treatment_start_date.setter
    def treatment_start_date(self, value: Optional[dt.date | None]) -> None:
        self._treatment_start_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.treatment_start_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment_start_date.fset.__name__)

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
            f"treatment start date={self.treatment_start_date} \n"
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
