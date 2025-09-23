# harmoinzation/datamodels.py
import re
from enum import Enum
from typing import List, Optional, Set, Sequence
from dataclasses import field
import datetime as dt
from logging import getLogger
from dataclasses import dataclass
from omop_etl.harmonization.validation.validators import StrictValidators

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
        self._icd10_code = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.icd10_code.fset.__name__)
        self.updated_fields.add(self.__class__.icd10_code.fset.__name__)

    @property
    def icd10_description(self) -> Optional[str]:
        return self._icd10_description

    @icd10_description.setter
    def icd10_description(self, value: Optional[str]) -> None:
        self._icd10_description = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.icd10_description.fset.__name__)
        self.updated_fields.add(self.__class__.icd10_description.fset.__name__)

    @property
    def main_tumor_type(self) -> Optional[str]:
        return self._main_tumor_type

    @main_tumor_type.setter
    def main_tumor_type(self, value: Optional[str]) -> None:
        self._main_tumor_type = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.main_tumor_type.fset.__name__)
        self.updated_fields.add(self.__class__.main_tumor_type.fset.__name__)

    @property
    def main_tumor_type_code(self) -> Optional[int]:
        return self._main_tumor_type_code

    @main_tumor_type_code.setter
    def main_tumor_type_code(self, value: Optional[int]) -> None:
        self._main_tumor_type_code = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.main_tumor_type_code.fset.__name__)
        self.updated_fields.add(self.__class__.main_tumor_type_code.fset.__name__)

    @property
    def cohort_tumor_type(self) -> Optional[str]:
        return self._cohort_tumor_type

    @cohort_tumor_type.setter
    def cohort_tumor_type(self, value: Optional[str]) -> None:
        self._cohort_tumor_type = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.cohort_tumor_type.fset.__name__)
        self.updated_fields.add(self.__class__.cohort_tumor_type.fset.__name__)

    @property
    def other_tumor_type(self) -> Optional[str]:
        return self._other_tumor_type

    @other_tumor_type.setter
    def other_tumor_type(self, value: Optional[str]) -> None:
        self._other_tumor_type = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.other_tumor_type.fset.__name__)
        self.updated_fields.add(self.__class__.other_tumor_type.fset.__name__)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"icd10_code={self.icd10_code!r}, "
            f"icd10_description={self.icd10_description!r}, "
            f"main_tumor_type={self.main_tumor_type!r}, "
            f"main_tumor_type_code={self.main_tumor_type_code!r}, "
            f"other_tumor_type={self.other_tumor_type!r}, "
            f"cohort_tumor_type={self.cohort_tumor_type!r}"
            f")"
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
            value=value,
            field_name=self.__class__.primary_treatment_drug.fset.__name__,
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
        self.updated_fields.add(self.__class__.primary_treatment_drug_code.fset.__name__)

    @property
    def secondary_treatment_drug_code(self) -> Optional[int]:
        return self._secondary_treatment_drug_code

    @secondary_treatment_drug_code.setter
    def secondary_treatment_drug_code(self, value: Optional[int]) -> None:
        self._secondary_treatment_drug_code = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.secondary_treatment_drug_code.fset.__name__,
        )
        self.updated_fields.add(self.__class__.secondary_treatment_drug_code.fset.__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(primary_treatment_drug={self.primary_treatment_drug!r}, primary_treatment_drug_code={self.primary_treatment_drug_code!r}, secondary_treatment_drug={self.secondary_treatment_drug!r}, secondary_treatment_drug_code={self.secondary_treatment_drug_code!r})"


class Biomarkers:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._gene_and_mutation: Optional[str] = None
        self._gene_and_mutation_code: Optional[int] = None
        self._cohort_target_name: Optional[str] = None
        self._cohort_target_mutation: Optional[str] = None
        self._date: Optional[dt.date] = None
        self.updated_fields: Set[str] = set()

    @property
    def gene_and_mutation(self) -> Optional[str]:
        return self._gene_and_mutation

    @gene_and_mutation.setter
    def gene_and_mutation(self, value: Optional[str]) -> None:
        self._gene_and_mutation = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.gene_and_mutation.fset.__name__)
        self.updated_fields.add(self.__class__.gene_and_mutation.fset.__name__)

    @property
    def gene_and_mutation_code(self) -> Optional[int]:
        return self._gene_and_mutation_code

    @gene_and_mutation_code.setter
    def gene_and_mutation_code(self, value: Optional[int]) -> None:
        self._gene_and_mutation_code = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.gene_and_mutation_code.fset.__name__,
        )
        self.updated_fields.add(self.__class__.gene_and_mutation_code.fset.__name__)

    @property
    def cohort_target_name(self) -> Optional[str]:
        return self._cohort_target_name

    @cohort_target_name.setter
    def cohort_target_name(self, value: Optional[str]) -> None:
        self._cohort_target_name = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.cohort_target_name.fset.__name__)
        self.updated_fields.add(self.__class__.cohort_target_name.fset.__name__)

    @property
    def cohort_target_mutation(self) -> Optional[str]:
        return self._cohort_target_mutation

    @cohort_target_mutation.setter
    def cohort_target_mutation(self, value: Optional[str]) -> None:
        self._cohort_target_mutation = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.cohort_target_mutation.fset.__name__,
        )
        self.updated_fields.add(self.__class__.cohort_target_mutation.fset.__name__)

    @property
    def date(self) -> Optional[dt.date]:
        return self._date

    @date.setter
    def date(self, value: Optional[dt.date]) -> None:
        self._date = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.date.fset.__name__)
        self.updated_fields.add(self.__class__.date.fset.__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(gene_and_mutation={self.gene_and_mutation!r}, gene_and_mutation_code={self.gene_and_mutation_code!r}, cohort_target_name={self.cohort_target_name!r}, cohort_target_mutation={self.cohort_target_mutation!r}, date={self.date!r})"


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
        self._lost_to_followup = StrictValidators.validate_optional_bool(value=value, field_name=self.__class__.lost_to_followup.fset.__name__)
        self.updated_fields.add(self.__class__.lost_to_followup.fset.__name__)

    @property
    def date_lost_to_followup(self) -> Optional[dt.date]:
        return self._date_lost_to_followup

    @date_lost_to_followup.setter
    def date_lost_to_followup(self, value: Optional[dt.date]) -> None:
        self._date_lost_to_followup = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.date_lost_to_followup.fset.__name__,
        )
        self.updated_fields.add(self.__class__.date_lost_to_followup.fset.__name__)

    def __str__(self):
        return f"{self.__class__.__name__}(lost_to_followup={self.lost_to_followup!r}, date_lost_to_followup={self.date_lost_to_followup!r})"


class EcogBaseline:
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
        self._description = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.description.fset.__name__)
        self.updated_fields.add(self.__class__.description.fset.__name__)

    @property
    def grade(self) -> Optional[int]:
        return self._grade

    @grade.setter
    def grade(self, value: Optional[int]) -> None:
        self._grade = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.grade.fset.__name__)
        self.updated_fields.add(self.__class__.grade.fset.__name__)

    @property
    def date(self) -> Optional[dt.date]:
        return self._date

    @date.setter
    def date(self, value: Optional[dt.date]) -> None:
        self._date = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.date.fset.__name__)
        self.updated_fields.add(self.__class__.date.fset.__name__)

    def __repr__(self):
        return f"{self.__class__.__name__}(description={self.description!r},  grade={self.grade!r}, date={self.date!r})"


class TumorAssessmentBaseline:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._asssessment_type: Optional[str] = None
        self._assessment_date: Optional[dt.date] = None
        self._target_lesion_size: Optional[int] = None
        self._target_lesion_nadir: Optional[int] = None
        self._target_lesion_measurment_date: Optional[dt.date] = None
        self._number_off_target_lesions: Optional[int] = None
        self._off_target_lesion_measurment_date: Optional[dt.date] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def assessment_type(self) -> Optional[str]:
        return self._asssessment_type

    @assessment_type.setter
    def assessment_type(self, value: Optional[str]) -> None:
        self._asssessment_type = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.assessment_type.fset.__name__)
        self.updated_fields.add(self.__class__.assessment_type.fset.__name__)

    @property
    def assessment_date(self) -> Optional[dt.date]:
        return self._assessment_date

    @assessment_date.setter
    def assessment_date(self, value: Optional[dt.date]) -> None:
        self._assessment_date = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.assessment_date.fset.__name__)
        self.updated_fields.add(self.__class__.assessment_date.fset.__name__)

    @property
    def target_lesion_size(self) -> Optional[int]:
        return self._target_lesion_size

    @target_lesion_size.setter
    def target_lesion_size(self, value: Optional[int]) -> None:
        self._target_lesion_size = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.target_lesion_size.fset.__name__)
        self.updated_fields.add(self.__class__.target_lesion_size.fset.__name__)

    @property
    def target_lesion_nadir(self) -> Optional[int]:
        return self._target_lesion_nadir

    @target_lesion_nadir.setter
    def target_lesion_nadir(self, value: Optional[int]) -> None:
        self._target_lesion_nadir = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.target_lesion_nadir.fset.__name__)
        self.updated_fields.add(self.__class__.target_lesion_nadir.fset.__name__)

    @property
    def target_lesion_measurment_date(self) -> Optional[dt.date]:
        return self._target_lesion_measurment_date

    @target_lesion_measurment_date.setter
    def target_lesion_measurment_date(self, value: Optional[dt.date]) -> None:
        self._target_lesion_measurment_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.target_lesion_measurment_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.target_lesion_measurment_date.fset.__name__)

    @property
    def number_off_target_lesions(self) -> Optional[int]:
        return self._number_off_target_lesions

    @number_off_target_lesions.setter
    def number_off_target_lesions(self, value: Optional[int]) -> None:
        self._number_off_target_lesions = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.number_off_target_lesions.fset.__name__,
        )
        self.updated_fields.add(self.__class__.number_off_target_lesions.fset.__name__)

    @property
    def off_target_lesion_measurment_date(self) -> Optional[dt.date]:
        return self._off_target_lesion_measurment_date

    @off_target_lesion_measurment_date.setter
    def off_target_lesion_measurment_date(self, value: Optional[dt.date]) -> None:
        self._off_target_lesion_measurment_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.off_target_lesion_measurment_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.off_target_lesion_measurment_date.fset.__name__)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"assessment_type={self.assessment_type!r}, "
            f"assessment_date={self.assessment_date!r}, "
            f"target_lesion_size={self.target_lesion_size!r}, "
            f"target_lesion_nadir={self.target_lesion_nadir!r}, "
            f"target_lesion_measurment_date={self.target_lesion_measurment_date!r}, "
            f"off_target_lesion_size={self.number_off_target_lesions!r}, "
            f"off_target_lesion_measurment_date={self.off_target_lesion_measurment_date!r}"
            f")"
        )


class BestOverallResponse:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._response: Optional[str] = None
        self._code: Optional[int] = None
        self._date: Optional[dt.date] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def response(self) -> Optional[str]:
        return self._response

    @response.setter
    def response(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.response.fset.__name__)
        self._response = validated
        self.updated_fields.add(self.__class__.response.fset.__name__)

    @property
    def code(self) -> Optional[int]:
        return self._code

    @code.setter
    def code(self, value: Optional[int]) -> None:
        validated = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.code.fset.__name__)
        self._code = validated
        self.updated_fields.add(self.__class__.code.fset.__name__)

    @property
    def date(self) -> Optional[dt.date]:
        return self._date

    @date.setter
    def date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.date.fset.__name__)
        self._date = validated
        self.updated_fields.add(self.__class__.date.fset.__name__)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(response={self.response!r}, code={self.code!r}, date={self.date!r})"


class MedicalHistory:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._term: Optional[str] = None
        self._sequence_id: Optional[int] = None
        self._start_date: Optional[dt.date] = None
        self._end_date: Optional[dt.date] = None
        self._status: Optional[str] = None
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
        self._term = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.term.fset.__name__)
        self.updated_fields.add(self.__class__.term.fset.__name__)

    @property
    def sequence_id(self) -> Optional[int]:
        return self._sequence_id

    @sequence_id.setter
    def sequence_id(self, value: Optional[int]) -> None:
        self._sequence_id = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.sequence_id.fset.__name__)
        self.updated_fields.add(self.__class__.sequence_id.fset.__name__)

    @property
    def start_date(self) -> Optional[dt.date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[dt.date]) -> None:
        self._start_date = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.start_date.fset.__name__)
        self.updated_fields.add(self.__class__.start_date.fset.__name__)

    @property
    def end_date(self) -> Optional[dt.date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[dt.date]) -> None:
        self._end_date = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.end_date.fset.__name__)
        self.updated_fields.add(self.__class__.end_date.fset.__name__)

    @property
    def status(self) -> Optional[str]:
        return self._status

    @status.setter
    def status(self, value: Optional[str]) -> None:
        self._status = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.status.fset.__name__)
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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(term={self.term!r}, seq={self.sequence_id!r}, start={self.start_date!r}, end={self.end_date!r}, status={self.status!r}, code={self.status_code!r})"


class PreviousTreatments:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._treatment: Optional[str] = None
        self._treatment_code: Optional[int] = None
        self._treatment_sequence_number: Optional[int] = None
        self._start_date: Optional[dt.date] = None
        self._end_date: Optional[dt.date] = None
        self._additional_treatment: Optional[str] = None
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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(treatment={self.treatment!r}, treatment_code={self.treatment_code!r}, treatment_sequence_number={self.treatment_sequence_number!r}, start_date={self.start_date!r}, end_date={self.end_date!r}, additional_treatment={self.additional_treatment!r})"


class TreatmentCycle:
    def __init__(self, patient_id: str):
        # core
        self._patient_id = patient_id
        self._treatment_name: Optional[str] = None
        self._cycle_type: Optional[str] = None
        self._treatment_number: Optional[int] = None
        self._cycle_number: Optional[int] = None
        self._start_date: Optional[dt.date] = None
        self._end_date: Optional[dt.date] = None
        self._recieved_treatment_this_cycle: Optional[bool] = None

        # iv only
        self._was_total_dose_delivered: Optional[bool] = None
        self._iv_dose_prescribed: Optional[str] = None
        self._iv_dose_prescribed_unit: Optional[str] = None

        # oral only
        self._was_dose_administered_to_spec: Optional[bool] = None
        self._reason_not_administered_to_spec: Optional[str] = None
        self._oral_dose_prescribed_per_day: Optional[str] = None
        self._oral_dose_unit: Optional[str] = None
        self._other_dose_unit: Optional[str] = None
        self._number_of_days_tablet_not_taken: Optional[int] = None
        self._reason_tablet_not_taken: Optional[str] = None
        self._was_tablet_taken_to_prescription_in_previous_cycle: Optional[bool] = None

        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def treatment_name(self) -> str:
        return self._treatment_name

    @treatment_name.setter
    def treatment_name(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.treatment_name.fset.__name__)
        self._treatment_name = validated
        self.updated_fields.add(self.__class__.treatment_name.fset.__name__)

    @property
    def cycle_type(self) -> Optional[str]:
        return self._cycle_type

    @cycle_type.setter
    def cycle_type(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.cycle_type.fset.__name__,
        )
        self._cycle_type = validated
        self.updated_fields.add(self.__class__.cycle_type.fset.__name__)

    @property
    def treatment_number(self) -> Optional[int]:
        return self._treatment_number

    @treatment_number.setter
    def treatment_number(self, value: Optional[int]) -> None:
        validated = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.treatment_number.fset.__name__,
        )
        self._treatment_number = validated
        self.updated_fields.add(self.__class__.treatment_number.fset.__name__)

    @property
    def cycle_number(self) -> Optional[int]:
        return self._cycle_number

    @cycle_number.setter
    def cycle_number(self, value: Optional[int]) -> None:
        validated = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.cycle_number.fset.__name__,
        )
        self._cycle_number = validated
        self.updated_fields.add(self.__class__.cycle_number.fset.__name__)

    @property
    def start_date(self) -> Optional[dt.date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.start_date.fset.__name__,
        )
        self._start_date = validated
        self.updated_fields.add(self.__class__.start_date.fset.__name__)

    @property
    def end_date(self) -> Optional[dt.date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.end_date.fset.__name__,
        )
        self._end_date = validated
        self.updated_fields.add(self.__class__.end_date.fset.__name__)

    @property
    def recieved_treatment_this_cycle(self) -> Optional[bool]:
        return self._recieved_treatment_this_cycle

    @recieved_treatment_this_cycle.setter
    def recieved_treatment_this_cycle(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.recieved_treatment_this_cycle.fset.__name__,
        )
        self._recieved_treatment_this_cycle = validated
        self.updated_fields.add(self.__class__.recieved_treatment_this_cycle.fset.__name__)

    @property
    def iv_dose_prescribed_unit(self) -> Optional[str]:
        return self._iv_dose_prescribed_unit

    @iv_dose_prescribed_unit.setter
    def iv_dose_prescribed_unit(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.iv_dose_prescribed_unit.fset.__name__,
        )
        self._iv_dose_prescribed_unit = validated
        self.updated_fields.add(self.__class__.iv_dose_prescribed_unit.fset.__name__)

    @property
    def iv_dose_prescribed(self) -> Optional[str]:
        return self._iv_dose_prescribed

    @iv_dose_prescribed.setter
    def iv_dose_prescribed(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.iv_dose_prescribed.fset.__name__,
        )
        self._iv_dose_prescribed = validated
        self.updated_fields.add(self.__class__.iv_dose_prescribed.fset.__name__)

    @property
    def was_total_dose_delivered(self) -> Optional[str]:
        return self._was_total_dose_delivered

    @was_total_dose_delivered.setter
    def was_total_dose_delivered(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_total_dose_delivered.fset.__name__,
        )
        self._was_total_dose_delivered = validated
        self.updated_fields.add(self.__class__.was_total_dose_delivered.fset.__name__)

    # oral only
    @property
    def was_dose_administered_to_spec(self) -> Optional[bool]:
        return self._was_dose_administered_to_spec

    @was_dose_administered_to_spec.setter
    def was_dose_administered_to_spec(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_dose_administered_to_spec.fset.__name__,
        )
        self._was_dose_administered_to_spec = validated
        self.updated_fields.add(self.__class__.was_dose_administered_to_spec.fset.__name__)

    @property
    def reason_not_administered_to_spec(self) -> Optional[str]:
        return self._reason_not_administered_to_spec

    @reason_not_administered_to_spec.setter
    def reason_not_administered_to_spec(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.reason_not_administered_to_spec.fset.__name__,
        )
        self._reason_not_administered_to_spec = validated
        self.updated_fields.add(self.__class__.reason_not_administered_to_spec.fset.__name__)

    @property
    def oral_dose_prescribed_per_day(self) -> Optional[float]:
        return self._oral_dose_prescribed_per_day

    @oral_dose_prescribed_per_day.setter
    def oral_dose_prescribed_per_day(self, value: Optional[float]) -> None:
        validated = StrictValidators.validate_optional_float(
            value=value,
            field_name=self.__class__.oral_dose_prescribed_per_day.fset.__name__,
        )
        self._oral_dose_prescribed_per_day = validated
        self.updated_fields.add(self.__class__.oral_dose_prescribed_per_day.fset.__name__)

    @property
    def oral_dose_unit(self) -> Optional[str]:
        return self._oral_dose_unit

    @oral_dose_unit.setter
    def oral_dose_unit(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.oral_dose_unit.fset.__name__,
        )
        self._oral_dose_unit = validated
        self.updated_fields.add(self.__class__.oral_dose_unit.fset.__name__)

    @property
    def other_dose_unit(self) -> Optional[str]:
        return self._other_dose_unit

    @other_dose_unit.setter
    def other_dose_unit(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.other_dose_unit.fset.__name__,
        )
        self._other_dose_unit = validated
        self.updated_fields.add(self.__class__.other_dose_unit.fset.__name__)

    @property
    def number_of_days_tablet_not_taken(self) -> Optional[int]:
        return self._number_of_days_tablet_not_taken

    @number_of_days_tablet_not_taken.setter
    def number_of_days_tablet_not_taken(self, value: Optional[int]) -> None:
        validated = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.number_of_days_tablet_not_taken.fset.__name__,
        )
        self._number_of_days_tablet_not_taken = validated
        self.updated_fields.add(self.__class__.number_of_days_tablet_not_taken.fset.__name__)

    @property
    def reason_tablet_not_taken(self) -> Optional[str]:
        return self._reason_tablet_not_taken

    @reason_tablet_not_taken.setter
    def reason_tablet_not_taken(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.reason_tablet_not_taken.fset.__name__,
        )
        self._reason_tablet_not_taken = validated
        self.updated_fields.add(self.__class__.reason_tablet_not_taken.fset.__name__)

    @property
    def was_tablet_taken_to_prescription_in_previous_cycle(self) -> Optional[bool]:
        return self._was_tablet_taken_to_prescription_in_previous_cycle

    @was_tablet_taken_to_prescription_in_previous_cycle.setter
    def was_tablet_taken_to_prescription_in_previous_cycle(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_tablet_taken_to_prescription_in_previous_cycle.fset.__name__,
        )
        self._was_tablet_taken_to_prescription_in_previous_cycle = validated
        self.updated_fields.add(self.__class__.was_tablet_taken_to_prescription_in_previous_cycle.fset.__name__)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id!r}, "
            f"cycle_type={self.cycle_type!r}, "
            f"treatment_number={self.treatment_number!r}, "
            f"cycle_number={self.cycle_number!r}, "
            f"start_date={self.start_date!r}, "
            f"end_date={self.end_date!r}, "
            f"recieved_treatment_this_cycle={self.recieved_treatment_this_cycle!r}, "
            f"iv_dose_prescribed={self.iv_dose_prescribed!r}, "
            f"iv_dose_prescribed_unit={self.iv_dose_prescribed_unit!r}, "
            f"was_total_dose_delivered={self.was_total_dose_delivered!r}, "
            f"was_dose_administered_to_spec={self.was_dose_administered_to_spec!r}, "
            f"reason_not_administered_to_spec={self.reason_not_administered_to_spec!r}, "
            f"oral_dose_prescribed_per_day={self.oral_dose_prescribed_per_day!r}, "
            f"oral_dose_unit={self.oral_dose_unit!r}, "
            f"other_dose_unit={self.other_dose_unit!r}, "
            f"number_of_days_tablet_not_taken={self.number_of_days_tablet_not_taken!r}, "
            f"reason_tablet_not_taken={self.reason_tablet_not_taken!r}, "
            f"was_tablet_taken_to_prescription_in_previous_cycle={self.was_tablet_taken_to_prescription_in_previous_cycle!r}"
            f")"
        )


class TumorAssessment:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._assessment_type: Optional[str] = None
        self._target_lesion_change_from_baseline: Optional[float] = None
        self._target_lesion_change_from_nadir: Optional[float] = None
        self._was_new_lesions_registered_after_baseline: Optional[bool] = None
        self._date: Optional[dt.date] = None
        self._recist_response: Optional[str] = None
        self._irecist_response: Optional[str] = None
        self._rano_response: Optional[str] = None
        self._recist_date_of_progression: Optional[dt.date] = None
        self._irecist_date_of_progression: Optional[dt.date] = None
        self._event_id: Optional[str] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def assessment_type(self) -> Optional[str]:
        return self._assessment_type

    @assessment_type.setter
    def assessment_type(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.assessment_type.fset.__name__)
        self._assessment_type = validated
        self.updated_fields.add(self.__class__.assessment_type.fset.__name__)

    @property
    def target_lesion_change_from_baseline(self) -> Optional[float]:
        return self._target_lesion_change_from_baseline

    @target_lesion_change_from_baseline.setter
    def target_lesion_change_from_baseline(self, value: Optional[float]) -> None:
        validated = StrictValidators.validate_optional_float(
            value=value,
            field_name=self.__class__.target_lesion_change_from_baseline.fset.__name__,
        )
        self._target_lesion_change_from_baseline = validated
        self.updated_fields.add(self.__class__.target_lesion_change_from_baseline.fset.__name__)

    @property
    def target_lesion_change_from_nadir(self) -> Optional[float]:
        return self._target_lesion_change_from_nadir

    @target_lesion_change_from_nadir.setter
    def target_lesion_change_from_nadir(self, value: Optional[float]) -> None:
        validated = StrictValidators.validate_optional_float(
            value=value,
            field_name=self.__class__.target_lesion_change_from_nadir.fset.__name__,
        )
        self._target_lesion_change_from_nadir = validated
        self.updated_fields.add(self.__class__.target_lesion_change_from_nadir.fset.__name__)

    @property
    def was_new_lesions_registered_after_baseline(self) -> Optional[bool]:
        return self._was_new_lesions_registered_after_baseline

    @was_new_lesions_registered_after_baseline.setter
    def was_new_lesions_registered_after_baseline(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_new_lesions_registered_after_baseline.fset.__name__,
        )
        self._was_new_lesions_registered_after_baseline = validated
        self.updated_fields.add(self.__class__.was_new_lesions_registered_after_baseline.fset.__name__)

    @property
    def date(self) -> Optional[dt.date]:
        return self._date

    @date.setter
    def date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.date.fset.__name__)
        self._date = validated
        self.updated_fields.add(self.__class__.date.fset.__name__)

    @property
    def recist_response(self) -> Optional[str]:
        return self._recist_response

    @recist_response.setter
    def recist_response(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.recist_response.fset.__name__)
        self._recist_response = validated
        self.updated_fields.add(self.__class__.recist_response.fset.__name__)

    @property
    def irecist_response(self) -> Optional[str]:
        return self._irecist_response

    @irecist_response.setter
    def irecist_response(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.irecist_response.fset.__name__)
        self._irecist_response = validated
        self.updated_fields.add(self.__class__.irecist_response.fset.__name__)

    @property
    def rano_response(self) -> Optional[str]:
        return self._rano_response

    @rano_response.setter
    def rano_response(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.rano_response.fset.__name__)
        self._rano_response = validated
        self.updated_fields.add(self.__class__.rano_response.fset.__name__)

    @property
    def recist_date_of_progression(self) -> Optional[dt.date]:
        return self._recist_date_of_progression

    @recist_date_of_progression.setter
    def recist_date_of_progression(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.recist_date_of_progression.fset.__name__,
        )
        self._recist_date_of_progression = validated
        self.updated_fields.add(self.__class__.recist_date_of_progression.fset.__name__)

    @property
    def irecist_date_of_progression(self) -> Optional[dt.date]:
        return self._irecist_date_of_progression

    @irecist_date_of_progression.setter
    def irecist_date_of_progression(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.irecist_date_of_progression.fset.__name__,
        )
        self._irecist_date_of_progression = validated
        self.updated_fields.add(self.__class__.irecist_date_of_progression.fset.__name__)

    @property
    def event_id(self) -> Optional[str]:
        return self._event_id

    @event_id.setter
    def event_id(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.event_id.fset.__name__)
        self._event_id = validated
        self.updated_fields.add(self.__class__.event_id.fset.__name__)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"assessment_type={self.assessment_type!r}, "
            f"target_lesion_change_from_baseline={self.target_lesion_change_from_baseline!r}, "
            f"target_lesion_change_from_nadir={self.target_lesion_change_from_nadir!r}, "
            f"was_new_lesions_registered_after_baseline={self.was_new_lesions_registered_after_baseline!r}, "
            f"date={self.date!r}, "
            f"recist_response={self.recist_response!r}, "
            f"irecist_response={self.irecist_response!r}, "
            f"rano_response={self.rano_response!r}, "
            f"recist_date_of_progression={self.recist_date_of_progression!r}, "
            f"irecist_response={self.irecist_response!r}, "
            f"event_id={self.event_id!r}"
            f")"
        )


class ConcomitantMedication:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._medication_name: Optional[str] = None
        self._medication_ongoing: Optional[bool] = None
        self._was_taken_due_to_medical_history_event: Optional[bool] = None
        self._was_taken_due_to_adverse_event: Optional[bool] = None
        self._is_adverse_event_ongoing: Optional[bool] = None
        self._start_date: Optional[dt.date] = None
        self._end_date: Optional[dt.date] = None
        self._sequence_id: Optional[int] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def medication_name(self) -> Optional[str]:
        return self._medication_name

    @medication_name.setter
    def medication_name(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.medication_name.fset.__name__)
        self._medication_name = validated
        self.updated_fields.add(self.__class__.medication_name.fset.__name__)

    @property
    def medication_ongoing(self) -> Optional[bool]:
        return self._medication_ongoing

    @medication_ongoing.setter
    def medication_ongoing(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(value=value, field_name=self.__class__.medication_ongoing.fset.__name__)
        self._medication_ongoing = validated
        self.updated_fields.add(self.__class__.medication_ongoing.fset.__name__)

    @property
    def was_taken_due_to_medical_history_event(self) -> Optional[bool]:
        return self._was_taken_due_to_medical_history_event

    @was_taken_due_to_medical_history_event.setter
    def was_taken_due_to_medical_history_event(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_taken_due_to_medical_history_event.fset.__name__,
        )
        self._was_taken_due_to_medical_history_event = validated
        self.updated_fields.add(self.__class__.was_taken_due_to_medical_history_event.fset.__name__)

    @property
    def was_taken_due_to_adverse_event(self) -> Optional[bool]:
        return self._was_taken_due_to_adverse_event

    @was_taken_due_to_adverse_event.setter
    def was_taken_due_to_adverse_event(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_taken_due_to_adverse_event.fset.__name__,
        )
        self._was_taken_due_to_adverse_event = validated
        self.updated_fields.add(self.__class__.was_taken_due_to_adverse_event.fset.__name__)

    @property
    def is_adverse_event_ongoing(self) -> Optional[bool]:
        return self._is_adverse_event_ongoing

    @is_adverse_event_ongoing.setter
    def is_adverse_event_ongoing(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.is_adverse_event_ongoing.fset.__name__,
        )
        self._is_adverse_event_ongoing = validated
        self.updated_fields.add(self.__class__.is_adverse_event_ongoing.fset.__name__)

    @property
    def start_date(self) -> Optional[dt.date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.start_date.fset.__name__)
        self._start_date = validated
        self.updated_fields.add(self.__class__.start_date.fset.__name__)

    @property
    def end_date(self) -> Optional[dt.date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.end_date.fset.__name__)
        self._end_date = validated
        self.updated_fields.add(self.__class__.end_date.fset.__name__)

    @property
    def sequence_id(self) -> Optional[int]:
        return self._sequence_id

    @sequence_id.setter
    def sequence_id(self, value: Optional[int]) -> None:
        validated = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.sequence_id.fset.__name__)
        self._sequence_id = validated
        self.updated_fields.add(self.__class__.sequence_id.fset.__name__)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id!r}, "
            f"medication_name={self.medication_name!r}, "
            f"was_taken_due_to_medical_history_event={self.was_taken_due_to_medical_history_event!r}, "
            f"was_taken_due_to_adverse_event={self.was_taken_due_to_adverse_event!r}, "
            f"is_adverse_event_ongoing={self.is_adverse_event_ongoing!r}, "
            f"start_date={self.start_date!r}, "
            f"end_date={self.end_date!r}, "
            f"sequence_id={self.sequence_id!r}"
            f")"
        )


class RelatedStatus(str, Enum):
    """Used in AdverseEvent"""

    RELATED = "related"  # code 4
    NOT_RELATED = "not_related"  # code 1
    UNKNOWN = "unknown"  # code 2 & 3


class AdverseEvent:
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._term: Optional[str] = None
        self._grade: Optional[int] = None
        self._outcome: Optional[str] = None
        self._start_date: Optional[dt.date] = None
        self._end_date: Optional[dt.date] = None
        self._was_serious: Optional[bool] = None
        self._turned_serious_date: Optional[dt.date] = None
        self._related_to_treatment_1_status: Optional[RelatedStatus] = None
        self._treatment_1_name: Optional[str] = None
        self._related_to_treatment_2_status: Optional[RelatedStatus] = None
        self._treatment_2_name: Optional[str] = None
        self._was_serious_grade_expected_treatment_1: Optional[bool] = None
        self._was_serious_grade_expected_treatment_2: Optional[bool] = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def term(self) -> Optional[str]:
        return self._term

    @term.setter
    def term(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.term.fset.__name__)
        self._term = validated
        self.updated_fields.add(self.__class__.term.fset.__name__)

    @property
    def grade(self) -> Optional[int]:
        return self._grade

    @grade.setter
    def grade(self, value: Optional[int]) -> None:
        validated = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.grade.fset.__name__)
        self._grade = validated
        self.updated_fields.add(self.__class__.grade.fset.__name__)

    @property
    def outcome(self) -> Optional[str]:
        return self._outcome

    @outcome.setter
    def outcome(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.outcome.fset.__name__)
        self._outcome = validated
        self.updated_fields.add(self.__class__.outcome.fset.__name__)

    @property
    def start_date(self) -> Optional[dt.date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.start_date.fset.__name__)
        self._start_date = validated
        self.updated_fields.add(self.__class__.start_date.fset.__name__)

    @property
    def end_date(self) -> Optional[dt.date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.end_date.fset.__name__)
        self._end_date = validated
        self.updated_fields.add(self.__class__.end_date.fset.__name__)

    @property
    def was_serious(self) -> Optional[bool]:
        return self._was_serious

    @was_serious.setter
    def was_serious(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(value=value, field_name=self.__class__.was_serious.fset.__name__)
        self._was_serious = validated
        self.updated_fields.add(self.__class__.was_serious.fset.__name__)

    @property
    def turned_serious_date(self) -> Optional[dt.date]:
        return self._turned_serious_date

    @turned_serious_date.setter
    def turned_serious_date(self, value: Optional[dt.date]) -> None:
        validated = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.turned_serious_date.fset.__name__)
        self._turned_serious_date = validated
        self.updated_fields.add(self.__class__.turned_serious_date.fset.__name__)

    @property
    def related_to_treatment_1_status(self) -> Optional[RelatedStatus]:
        return self._related_to_treatment_1_status

    @related_to_treatment_1_status.setter
    def related_to_treatment_1_status(self, value: Optional[RelatedStatus]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.related_to_treatment_1_status.fset.__name__,
        )
        self._related_to_treatment_1_status = validated
        self.updated_fields.add(self.__class__.related_to_treatment_1_status.fset.__name__)

    @property
    def treatment_1_name(self) -> Optional[str]:
        return self._treatment_1_name

    @treatment_1_name.setter
    def treatment_1_name(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.treatment_1_name.fset.__name__)
        self._treatment_1_name = validated
        self.updated_fields.add(self.__class__.treatment_1_name.fset.__name__)

    @property
    def related_to_treatment_2_status(self) -> Optional[RelatedStatus]:
        return self._related_to_treatment_2_status

    @related_to_treatment_2_status.setter
    def related_to_treatment_2_status(self, value: Optional[RelatedStatus]) -> None:
        validated = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.related_to_treatment_2_status.fset.__name__,
        )
        self._related_to_treatment_2_status = validated
        self.updated_fields.add(self.__class__.related_to_treatment_2_status.fset.__name__)

    @property
    def treatment_2_name(self) -> Optional[str]:
        return self._treatment_2_name

    @treatment_2_name.setter
    def treatment_2_name(self, value: Optional[str]) -> None:
        validated = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.treatment_2_name.fset.__name__)
        self._treatment_2_name = validated
        self.updated_fields.add(self.__class__.treatment_2_name.fset.__name__)

    @property
    def was_serious_grade_expected_treatment_1(self) -> Optional[bool]:
        return self._was_serious_grade_expected_treatment_1

    @was_serious_grade_expected_treatment_1.setter
    def was_serious_grade_expected_treatment_1(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_serious_grade_expected_treatment_1.fset.__name__,
        )
        self._was_serious_grade_expected_treatment_1 = validated
        self.updated_fields.add(self.__class__.was_serious_grade_expected_treatment_1.fset.__name__)

    @property
    def was_serious_grade_expected_treatment_2(self) -> Optional[bool]:
        return self._was_serious_grade_expected_treatment_2

    @was_serious_grade_expected_treatment_2.setter
    def was_serious_grade_expected_treatment_2(self, value: Optional[bool]) -> None:
        validated = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.was_serious_grade_expected_treatment_2.fset.__name__,
        )
        self._was_serious_grade_expected_treatment_2 = validated
        self.updated_fields.add(self.__class__.was_serious_grade_expected_treatment_2.fset.__name__)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id!r}, "
            f"term={self.term!r}, "
            f"grade={self.grade!r}, "
            f"outcome={self.outcome!r}, "
            f"start_date={self.start_date!r}, "
            f"end_date={self.end_date!r}, "
            f"end_date={self.end_date!r}, "
            f"was_serious={self.was_serious!r}, "
            f"turned_serious_date={self.turned_serious_date!r}, "
            f"related_to_treatment_1_status={self.related_to_treatment_1_status!r}, "
            f"treatment_1_name={self.treatment_1_name!r}, "
            f"related_to_treatment_2_status={self.related_to_treatment_2_status!r}, "
            f"treatment_2_name={self.treatment_2_name!r}, "
            f"was_serious_grade_expected_treatment_1={self.was_serious_grade_expected_treatment_1!r}, "
            f"was_serious_grade_expected_treatment_2={self.was_serious_grade_expected_treatment_2!r}"
            f")"
        )


# TODO: this is hacky, fix later (also EQ5D)
class C30:
    Q_RE = re.compile(r"^q([1-9]|[12]\d|30)$")
    QC_RE = re.compile(r"^q([1-9]|[12]\d|30)_code$")

    def __init__(self, patient_id: str):
        self.patient_id: str = patient_id
        self.date: Optional[dt.date] = None
        self.event_name: Optional[str] = None
        # pre-create q1 to q30
        for i in range(1, 31):
            setattr(self, f"q{i}", None)
            setattr(self, f"q{i}_code", None)

    def __setattr__(self, name, value):
        # normalize q* fields
        if getattr(self, self.Q_RE.pattern, None) and self.Q_RE.match(name):
            value = StrictValidators.validate_optional_str(value, field_name=name)
        if getattr(self, self.QC_RE.pattern, None) and self.QC_RE.match(name):
            value = StrictValidators.validate_optional_int(value, field_name=name)
        super().__setattr__(name, value)

    def __repr__(self) -> str:
        q_args = [f"q{i}={getattr(self, f'q{i}')!r}" for i in range(1, 31) if getattr(self, f"q{i}") is not None]

        qc_args = [f"q{i}_code={getattr(self, f'q{i}_code')!r}" for i in range(1, 31) if getattr(self, f"q{i}_code") is not None]

        base_args = [
            f"patient_id={self.patient_id!r}",
            f"date={self.date!r}",
            f"event_name={self.event_name!r}",
        ]

        all_args = base_args + q_args + qc_args
        return f"{self.__class__.__name__}({', '.join(all_args)})"


class EQ5D:
    Q_RE = re.compile(r"^q([1-5])$")
    QC_RE = re.compile(r"^q([1-5])_code$")

    def __init__(self, patient_id: str):
        self.patient_id: str = patient_id
        self.date: Optional[dt.date] = None
        self.event_name: Optional[str] = None
        self.qol_metric: Optional[int] = None
        # pre-create eq5d1 to eq5d5
        for i in range(1, 6):
            setattr(self, f"q{i}", None)
            setattr(self, f"q{i}_code", None)

    def __setattr__(self, name, value):
        if getattr(self, self.Q_RE.pattern, None) and self.Q_RE.match(name):
            value = StrictValidators.validate_optional_str(value, field_name=name)
        elif getattr(self, self.QC_RE.pattern, None) and self.QC_RE.match(name):
            StrictValidators.validate_optional_int(value, field_name=name)
        super().__setattr__(name, value)

    def __repr__(self) -> str:
        q_args = [f"q{i}={getattr(self, f'q{i}')!r}" for i in range(1, 6) if getattr(self, f"q{i}") is not None]

        qc_args = [f"q{i}_code={getattr(self, f'q{i}_code')!r}" for i in range(1, 6) if getattr(self, f"q{i}_code") is not None]

        base_args = [
            f"patient_id={self.patient_id!r}",
            f"date={self.date!r}",
            f"event_name={self.event_name!r}",
            f"qol_metric={self.qol_metric!r}",
        ]

        all_args = base_args + q_args + qc_args
        return f"{self.__class__.__name__}({', '.join(all_args)})"


class Patient:
    """
    Stores all data for a patient
    """

    def __init__(self, patient_id: str, trial_id: str):
        self.updated_fields: Set[str] = set()

        # scalars
        self._patient_id = patient_id
        self._trial_id = trial_id
        self._cohort_name: Optional[str] = None
        self._age: Optional[int] = None
        self._sex: Optional[str] = None
        self._evaluable_for_efficacy_analysis: bool = False
        self._treatment_start_date: Optional[dt.date] = None
        self._treatment_end_date: Optional[dt.date] = None
        self._treatment_start_last_cycle: Optional[dt.date] = None
        self._date_of_death: Optional[dt.date] = None
        self._has_any_adverse_events: Optional[bool] = None
        self._number_of_adverse_events: Optional[int] = None
        self._number_of_serious_adverse_events: Optional[int] = None
        self._has_clinical_benefit_at_week16: Optional[bool] = None
        self._end_of_treatment_reason: Optional[str] = None
        self._end_of_treatment_date: Optional[dt.date] = None

        # singletons
        self._tumor_type: Optional[TumorType] = None
        self._study_drugs: Optional[StudyDrugs] = None
        self._biomarker: Optional[Biomarkers] = None
        self._lost_to_followup: Optional[FollowUp] = None
        self._ecog_baseline: Optional[EcogBaseline] = None
        self._tumor_assessment_baseline: Optional[TumorAssessmentBaseline] = None
        self._best_overall_response: Optional[BestOverallResponse] = None

        # collections
        self._medical_histories: list[MedicalHistory] = []
        self._previous_treatments: list[PreviousTreatments] = []
        self._treatment_cycles: list[TreatmentCycle] = []
        self._concomitant_medications: list[ConcomitantMedication] = []
        self._adverse_events: list[AdverseEvent] = []
        self._tumor_assessments: list[TumorAssessment] = []
        self._c30_list: list[C30] = []
        self._eq5d_list: list[EQ5D] = []

    # scalars
    @property
    def patient_id(self) -> str:
        """Patient ID (immutable)"""
        return self._patient_id

    @patient_id.setter
    def patient_id(self, value: Optional[str]) -> None:
        self._patient_id = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.patient_id.fset.__name__)
        self.updated_fields.add(self.__class__.patient_id.fset.__name__)

    @property
    def trial_id(self) -> str:
        """Trial ID (immutable)"""
        return self._trial_id

    @trial_id.setter
    def trial_id(self, value: Optional[str]) -> None:
        """Trial ID (immutable)"""
        self._trial_id = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.trial_id.fset.__name__)
        self.updated_fields.add(self.__class__.trial_id.fset.__name__)

    @property
    def cohort_name(self) -> str:
        return self._cohort_name

    @cohort_name.setter
    def cohort_name(self, value: Optional[str]) -> None:
        self._cohort_name = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.cohort_name.fset.__name__)
        self.updated_fields.add(self.__class__.cohort_name.fset.__name__)

    @property
    def age(self):
        return self._age

    @age.setter
    def age(self, value: Optional[str | int | None]) -> None:
        """Set age with validation"""
        self._age = StrictValidators.validate_optional_int(value=value, field_name=self.__class__.age.fset.__name__)
        self.updated_fields.add(self.__class__.age.fset.__name__)

    @property
    def sex(self):
        return self._sex

    @sex.setter
    def sex(self, value: Optional[str | None]) -> None:
        """Set sex with validation"""
        self._sex = StrictValidators.validate_optional_str(value=value, field_name=self.__class__.sex.fset.__name__)
        self.updated_fields.add(self.__class__.sex.fset.__name__)

    @property
    def date_of_death(self):
        return self._date_of_death

    @date_of_death.setter
    def date_of_death(self, value: Optional[dt.date | None]) -> None:
        """Set date of death with validation"""
        self._date_of_death = StrictValidators.validate_optional_date(value=value, field_name=self.__class__.date_of_death.fset.__name__)
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
        self.updated_fields.add(self.__class__.evaluable_for_efficacy_analysis.fset.__name__)

    @property
    def treatment_start_date(self) -> Optional[dt.date]:
        return self._treatment_start_date

    @treatment_start_date.setter
    def treatment_start_date(self, value: Optional[dt.date]) -> None:
        self._treatment_start_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.treatment_start_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment_start_date.fset.__name__)

    @property
    def treatment_end_date(self) -> Optional[dt.date]:
        return self._treatment_end_date

    @treatment_end_date.setter
    def treatment_end_date(self, value: Optional[dt.date]) -> None:
        self._treatment_end_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.treatment_end_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment_end_date.fset.__name__)

    @property
    def treatment_start_last_cycle(self) -> Optional[dt.date]:
        return self._treatment_start_last_cycle

    @treatment_start_last_cycle.setter
    def treatment_start_last_cycle(self, value: Optional[dt.date]) -> None:
        self._treatment_end_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.treatment_start_last_cycle.fset.__name__,
        )
        self.updated_fields.add(self.__class__.treatment_start_last_cycle.fset.__name__)

    @property
    def has_any_adverse_events(self) -> Optional[bool]:
        return self._has_any_adverse_events

    @has_any_adverse_events.setter
    def has_any_adverse_events(self, value: Optional[bool]) -> None:
        self._has_any_adverse_events = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.has_any_adverse_events.fset.__name__,
        )
        self.updated_fields.add(self.__class__.has_any_adverse_events.fset.__name__)

    @property
    def number_of_adverse_events(self) -> Optional[int]:
        return self._number_of_adverse_events

    @number_of_adverse_events.setter
    def number_of_adverse_events(self, value: Optional[int]) -> None:
        self._number_of_adverse_events = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.number_of_adverse_events.fset.__name__,
        )
        self.updated_fields.add(self.__class__.has_any_adverse_events.fset.__name__)

    @property
    def number_of_serious_adverse_events(self) -> Optional[int]:
        return self._number_of_serious_adverse_events

    @number_of_serious_adverse_events.setter
    def number_of_serious_adverse_events(self, value: Optional[int]) -> None:
        self._number_of_serious_adverse_events = StrictValidators.validate_optional_int(
            value=value,
            field_name=self.__class__.number_of_serious_adverse_events.fset.__name__,
        )
        self.updated_fields.add(self.__class__.number_of_serious_adverse_events.fset.__name__)

    @property
    def has_clinical_benefit_at_week16(self) -> Optional[bool]:
        return self._has_any_adverse_events

    @has_clinical_benefit_at_week16.setter
    def has_clinical_benefit_at_week16(self, value: Optional[bool]) -> None:
        self._has_clinical_benefit_at_week16 = StrictValidators.validate_optional_bool(
            value=value,
            field_name=self.__class__.has_clinical_benefit_at_week16.fset.__name__,
        )
        self.updated_fields.add(self.__class__.has_clinical_benefit_at_week16.fset.__name__)

    @property
    def end_of_treatment_reason(self):
        return self._end_of_treatment_reason

    @end_of_treatment_reason.setter
    def end_of_treatment_reason(self, value: Optional[str]) -> None:
        self._end_of_treatment_reason = StrictValidators.validate_optional_str(
            value=value,
            field_name=self.__class__.end_of_treatment_reason.fset.__name__,
        )
        self.updated_fields.add(self.__class__.end_of_treatment_reason.fset.__name__)

    @property
    def end_of_treatment_date(self) -> Optional[dt.date]:
        return self._end_of_treatment_date

    @end_of_treatment_date.setter
    def end_of_treatment_date(self, value: Optional[dt.date]) -> None:
        self._end_of_treatment_date = StrictValidators.validate_optional_date(
            value=value,
            field_name=self.__class__.end_of_treatment_date.fset.__name__,
        )
        self.updated_fields.add(self.__class__.end_of_treatment_date.fset.__name__)

    # singletons
    @property
    def tumor_type(self) -> Optional[TumorType]:
        return self._tumor_type

    @tumor_type.setter
    def tumor_type(self, value: Optional[TumorType | None]) -> None:
        if value is not None and not isinstance(value, TumorType):
            raise ValueError(f"tumor_type must be {TumorType.__name__} or None, got {value} with type {type(value)}")
        if value is not None:
            value._patient_id = self._patient_id

        self._tumor_type = value
        self.updated_fields.add(TumorType.__name__)

    @property
    def study_drugs(self) -> Optional[StudyDrugs]:
        return self._study_drugs

    @study_drugs.setter
    def study_drugs(self, value: Optional[StudyDrugs | None]) -> None:
        if value is not None and not isinstance(value, StudyDrugs):
            raise ValueError(f"study_drugs must be {StudyDrugs.__name__} or None, got {value} with type {type(value)}")
        if value is not None:
            value._patient_id = self._patient_id

        self._study_drugs = value
        self.updated_fields.add(StudyDrugs.__name__)

    @property
    def biomarker(self) -> Optional[Biomarkers]:
        return self._biomarker

    @biomarker.setter
    def biomarker(self, value: Optional[Biomarkers | None]) -> None:
        if value is not None and not isinstance(value, Biomarkers):
            raise ValueError(f"biomarker must be {Biomarkers.__name__} or None, got {value} with type {type(value)}")
        if value is not None:
            value._patient_id = self._patient_id

        self._biomarker = value
        self.updated_fields.add(Biomarkers.__name__)

    @property
    def lost_to_followup(self) -> Optional[FollowUp]:
        return self._lost_to_followup

    @lost_to_followup.setter
    def lost_to_followup(self, value: Optional[FollowUp | None]) -> None:
        if value is not None and not isinstance(value, FollowUp):
            raise ValueError(f"lost_to_followup must be {FollowUp.__name__} or None, got {value} with type {type(value)}")
        if value is not None:
            value._patient_id = self._patient_id

        self._lost_to_followup = value
        self.updated_fields.add(FollowUp.__name__)

    @property
    def ecog_baseline(self) -> Optional[EcogBaseline]:
        return self._ecog_baseline

    @ecog_baseline.setter
    def ecog_baseline(self, value: Optional[EcogBaseline | None]) -> None:
        if value is not None and not isinstance(value, EcogBaseline):
            raise ValueError(f"ecog must be {EcogBaseline.__name__} or None, got {value} with type {type(value)}")
        if value is not None:
            value._patient_id = self._patient_id

        self._ecog_baseline = value
        self.updated_fields.add(EcogBaseline.__name__)

    @property
    def tumor_assessment_baseline(self) -> Optional[TumorAssessmentBaseline]:
        return self._tumor_assessment_baseline

    @tumor_assessment_baseline.setter
    def tumor_assessment_baseline(self, value: Optional[TumorAssessmentBaseline] | None) -> None:
        if value is not None and not isinstance(value, TumorAssessmentBaseline):
            raise ValueError(f"Tumor assessment baseline must be {TumorAssessmentBaseline.__name__} or None, got {value} with type{type(value)}")

        if value is not None:
            value._patient_id = self._patient_id

        self._tumor_assessment_baseline = value
        self.updated_fields.add(TumorAssessmentBaseline.__name__)

    @property
    def best_overall_response(self) -> Optional[BestOverallResponse]:
        return self._best_overall_response

    @best_overall_response.setter
    def best_overall_response(self, value: Optional[BestOverallResponse] | None) -> None:
        if value is not None and not isinstance(value, BestOverallResponse):
            raise ValueError(f"Best overall resoonse must be {BestOverallResponse.__name__} or None, got {value} with type{type(value)}")

        if value is not None:
            value._patient_id = self._patient_id

        self._best_overall_response = value
        self.updated_fields.add(BestOverallResponse.__name__)

    # multiple instances
    @property
    def medical_histories(self) -> tuple[MedicalHistory, ...]:
        """Immutable view, empty tuple if None."""
        return tuple(self._medical_histories)

    @medical_histories.setter
    def medical_histories(self, value: Optional[Sequence[MedicalHistory]]) -> None:
        items: list[MedicalHistory]
        if value is None:
            items = []
        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"expected a sequence of MedicalHistory, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, MedicalHistory)]
            if wrong_type:
                raise TypeError(f"all elements must be MedicalHistory, got {wrong_type}")
            items = list(value)

        for mh in items:
            if mh._patient_id and mh._patient_id != self._patient_id:
                raise ValueError(f"Mismatched patient_id in MedicalHistory: {mh._patient_id} != {self._patient_id}")
            mh._patient_id = self._patient_id

        self._medical_histories = items
        self.updated_fields.add(self.__class__.medical_histories.fset.__name__)

    @property
    def previous_treatments(self) -> tuple[PreviousTreatments, ...]:
        return tuple(self._previous_treatments)

    @previous_treatments.setter
    def previous_treatments(self, value: Optional[Sequence[PreviousTreatments]]) -> None:
        items: list[PreviousTreatments]
        if value is None:
            items = []
        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"expected a sequence of PreviousTreatments, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, PreviousTreatments)]
            if wrong_type:
                raise TypeError(f"all elements must be PreviousTreatments, got {wrong_type}")
            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._previous_treatments = items
        self.updated_fields.add(self.__class__.previous_treatments.fset.__name__)

    @property
    def treatment_cycles(self) -> tuple[TreatmentCycle, ...]:
        return tuple(self._treatment_cycles)

    @treatment_cycles.setter
    def treatment_cycles(self, value: Optional[Sequence[TreatmentCycle]]) -> None:
        items: list[TreatmentCycle]
        if value is None:
            items = []

        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"Expected a sequence of TreatmentCycles, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, TreatmentCycle)]
            if wrong_type:
                raise TypeError(f"All elements must be TreatmentCycle, got {wrong_type}")

            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._treatment_cycles = items
        self.updated_fields.add(self.__class__.treatment_cycles.fset.__name__)

    @property
    def concomitant_medications(self) -> tuple[ConcomitantMedication, ...]:
        return tuple(self._concomitant_medications)

    @concomitant_medications.setter
    def concomitant_medications(self, value: Optional[Sequence[ConcomitantMedication]]) -> None:
        items: List[ConcomitantMedication]
        if value is None:
            items = []

        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"Expected Sequence of ConcomitantMedication, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, ConcomitantMedication)]
            if wrong_type:
                raise TypeError(f"All elements should be of type ConcomitantMedication, got {wrong_type}")

            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._concomitant_medications = items
        self.updated_fields.add(self.__class__.concomitant_medications.fset.__name__)

    @property
    def adverse_events(self) -> tuple[AdverseEvent, ...]:
        return tuple(self._adverse_events)

    @adverse_events.setter
    def adverse_events(self, value: Optional[Sequence[AdverseEvent]]) -> None:
        items: List[AdverseEvent]
        if value is None:
            items = []

        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"Expected Sequence of AdverseEvents, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, AdverseEvent)]
            if wrong_type:
                raise TypeError(f"All elements should be of type AdverseEvents, got {wrong_type}")

            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._adverse_events = items
        self.updated_fields.add(self.__class__.adverse_events.fset.__name__)

    @property
    def tumor_assessments(self) -> tuple[TumorAssessment, ...]:
        return tuple(self._tumor_assessments)

    @tumor_assessments.setter
    def tumor_assessments(self, value: Optional[Sequence[TumorAssessment]]) -> None:
        items: List[TumorAssessment]
        if value is None:
            items = []

        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"Expected Sequence of TumorAssessment, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, TumorAssessment)]
            if wrong_type:
                raise TypeError(f"All elements should be of type TumorAssessment, got {wrong_type}")

            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._tumor_assessments = items
        self.updated_fields.add(self.__class__.tumor_assessments.fset.__name__)

    @property
    def c30_list(self) -> tuple[C30, ...]:
        return tuple(self._c30_list)

    @c30_list.setter
    def c30_list(self, value: Optional[Sequence[C30]]) -> None:
        items: List[C30]
        if value is None:
            items = []

        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"Expected Sequence of C30, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, C30)]
            if wrong_type:
                raise TypeError(f"All elements should be of type C30, got {wrong_type}")

            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._c30_list = items
        self.updated_fields.add(self.__class__.c30_list.fset.__name__)

    @property
    def eq5d_list(self) -> tuple[EQ5D, ...]:
        return tuple(self._eq5d_list)

    @eq5d_list.setter
    def eq5d_list(self, value: Optional[Sequence[EQ5D]]) -> None:
        items: List[EQ5D]
        if value is None:
            items = []

        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"Expected Sequence of EQ5D, got {type(value)}")

            wrong_type = [type(x) for x in value if not isinstance(x, EQ5D)]
            if wrong_type:
                raise TypeError(f"All elements should be of type EQ5D, got {wrong_type}")

            items = list(value)

        for pt in items:
            pt._patient_id = self._patient_id

        self._eq5d_list = items
        self.updated_fields.add(self.__class__.eq5d_list.fset.__name__)

    def get_updated_fields(self) -> Set[str]:
        return self.updated_fields

    def __str__(self):
        delim = ","
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id}{delim} "
            f"trial_id={self.trial_id}{delim} "
            f"cohort_name={self.cohort_name}{delim} "
            f"sex={self.sex}{delim} "
            f"age={self.age}{delim} "
            f"tumor_type={self.tumor_type}{delim} "
            f"study_drugs={self.study_drugs}{delim} "
            f"biomarkers={self.biomarker}{delim} "
            f"date_of_death={self.date_of_death}{delim} "
            f"has_any_adverse_events={self.has_any_adverse_events}{delim} "
            f"number_of_adverse_events={self.number_of_adverse_events}{delim} "
            f"number_of_serious_adverse_events={self.number_of_serious_adverse_events}{delim} "
            f"lost_to_followup={self.lost_to_followup}{delim} "
            f"evaluable_for_efficacy_analysis={self.evaluable_for_efficacy_analysis}{delim} "
            f"treatment_start_date={self.treatment_start_date}{delim} "
            f"has_clinical_benefit_at_week16={self.has_clinical_benefit_at_week16}{delim} "
            f"end_of_treatment_reason={self.end_of_treatment_reason}{delim} "
            f"end_of_treatment_date={self.end_of_treatment_date}{delim} "
            f"ecog={self.ecog_baseline}{delim} "
            f"tumor_assessment_baseline={self.tumor_assessment_baseline}{delim} "
            f"best_overall_respoonse={self.best_overall_response}{delim} "
            f"medical_histories={self.medical_histories}{delim} "
            f"previous_treatments={self.previous_treatments}{delim} "
            f"treatment_cycles={self.treatment_cycles}{delim} "
            f"concomitant_medications={self.concomitant_medications}{delim} "
            f"adverse_events={self.adverse_events}{delim} "
            f"tumor_assessments={self.tumor_assessments}{delim} "
            f"EQ5D={self.eq5d_list}{delim} "
            f"C30={self.c30_list}"
            f")"
        )


@dataclass
class HarmonizedData:
    """
    Stores all patient data for a processed trial
    """

    trial_id: str
    patients: List[Patient] = field(default_factory=list)
    # clinical_benefit? proportion of patients with CR PR SD,
    # does this even make sense to have in Patient class?
    # - could have flag "clinical benefit" at different visits, with date
    # - but then might as well just fitler and calcualte on tumor assessments..?
    # other fields and metadata etc on *dataset* level

    def __str__(self):
        patient_str = "\n".join(str(p) for p in self.patients)
        return f"Trial ID: {self.trial_id}\nPatients:\n{patient_str}"

    # repr, to_dict, to_df, to_csv, to_json
    # need to serialize (can evantually convert to pydantic model)
