from typing import ClassVar, Set, Sequence, Union, get_type_hints, get_origin, get_args
import datetime as dt
from logging import getLogger
from typing import TypeVar

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.core.track_validated import TrackedValidated
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.best_overall_response import BestOverallResponse
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.treatment_cycle import TreatmentCycle
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.domain.tumor_type import TumorType

log = getLogger(__name__)
T = TypeVar("T")


class Patient(TrackedValidated):
    """
    Stores all data for a patient
    """

    _attr_cache: ClassVar[dict[type, str]] = {}

    def __init__(self, patient_id: str, trial_id: str):
        self.updated_fields: Set[str] = set()

        # scalars
        self._patient_id = patient_id
        self._trial_id = trial_id
        self._cohort_name: str | None = None
        self._age: int | None = None
        self._date_of_birth: dt.date | None = None
        self._sex: str | None = None
        self._evaluable_for_efficacy_analysis: bool | None = None
        self._treatment_start_date: dt.date | None = None
        self._treatment_end_date: dt.date | None = None
        self._treatment_start_last_cycle: dt.date | None = None
        self._date_of_death: dt.date | None = None
        self._has_any_adverse_events: bool | None = None
        self._number_of_adverse_events: int | None = None
        self._number_of_serious_adverse_events: int | None = None
        self._has_clinical_benefit_at_week16: bool | None = None
        self._end_of_treatment_reason: str | None = None
        self._end_of_treatment_date: dt.date | None = None

        # singletons
        self._tumor_type: TumorType | None = None
        self._study_drugs: StudyDrugs | None = None
        self._biomarkers: Biomarkers | None = None
        self._lost_to_followup: FollowUp | None = None
        self._ecog_baseline: EcogBaseline | None = None
        self._tumor_assessment_baseline: TumorAssessmentBaseline | None = None
        self._best_overall_response: BestOverallResponse | None = None

        # collections
        self._medical_histories: tuple[MedicalHistory, ...] = ()
        self._previous_treatments: tuple[PreviousTreatments, ...] = ()
        self._treatment_cycles: tuple[TreatmentCycle, ...] = ()
        self._concomitant_medications: tuple[ConcomitantMedication, ...] = ()
        self._adverse_events: tuple[AdverseEvent, ...] = ()
        self._tumor_assessments: tuple[TumorAssessment, ...] = ()
        self._c30_collection: tuple[C30, ...] = ()
        self._eq5d_collection: tuple[EQ5D, ...] = ()

    # scalars
    @property
    def patient_id(self) -> str:
        return self._patient_id

    @patient_id.setter
    def patient_id(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.patient_id,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def trial_id(self) -> str:
        return self._trial_id

    @trial_id.setter
    def trial_id(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.trial_id,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def cohort_name(self) -> str:
        return self._cohort_name

    @cohort_name.setter
    def cohort_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cohort_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def age(self) -> int | None:
        return self._age

    @age.setter
    def age(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.age,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def date_of_birth(self) -> dt.date | None:
        return self._date_of_birth

    @date_of_birth.setter
    def date_of_birth(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.date_of_birth,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def sex(self) -> str | None:
        return self._sex

    @sex.setter
    def sex(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.sex,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def date_of_death(self) -> dt.date | None:
        return self._date_of_death

    @date_of_death.setter
    def date_of_death(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.date_of_death,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def evaluable_for_efficacy_analysis(self) -> bool | None:
        return self._evaluable_for_efficacy_analysis

    @evaluable_for_efficacy_analysis.setter
    def evaluable_for_efficacy_analysis(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.evaluable_for_efficacy_analysis,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def treatment_start_date(self) -> dt.date | None:
        return self._treatment_start_date

    @treatment_start_date.setter
    def treatment_start_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_start_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def treatment_end_date(self) -> dt.date | None:
        return self._treatment_end_date

    @treatment_end_date.setter
    def treatment_end_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_end_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def treatment_start_last_cycle(self) -> dt.date | None:
        return self._treatment_start_last_cycle

    @treatment_start_last_cycle.setter
    def treatment_start_last_cycle(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_start_last_cycle,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def has_any_adverse_events(self) -> bool | None:
        return self._has_any_adverse_events

    @has_any_adverse_events.setter
    def has_any_adverse_events(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.has_any_adverse_events,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def number_of_adverse_events(self) -> int | None:
        return self._number_of_adverse_events

    @number_of_adverse_events.setter
    def number_of_adverse_events(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.number_of_adverse_events,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def number_of_serious_adverse_events(self) -> int | None:
        return self._number_of_serious_adverse_events

    @number_of_serious_adverse_events.setter
    def number_of_serious_adverse_events(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.number_of_serious_adverse_events,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def has_clinical_benefit_at_week16(self) -> bool | None:
        return self._has_clinical_benefit_at_week16

    @has_clinical_benefit_at_week16.setter
    def has_clinical_benefit_at_week16(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.has_clinical_benefit_at_week16,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def end_of_treatment_reason(self) -> str | None:
        return self._end_of_treatment_reason

    @end_of_treatment_reason.setter
    def end_of_treatment_reason(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.end_of_treatment_reason,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def end_of_treatment_date(self) -> dt.date | None:
        return self._end_of_treatment_date

    @end_of_treatment_date.setter
    def end_of_treatment_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.end_of_treatment_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    # singletons
    @property
    def tumor_type(self) -> TumorType | None:
        return self._tumor_type

    @tumor_type.setter
    def tumor_type(self, value: TumorType | None) -> None:
        self._tumor_type = self.validate_singleton(
            value,
            item_type=TumorType,
            patient_id=self._patient_id,
            field_name=self.__class__.tumor_type.fset.__name__,
        )
        self.updated_fields.add(TumorType.__name__)

    @property
    def study_drugs(self) -> StudyDrugs | None:
        return self._study_drugs

    @study_drugs.setter
    def study_drugs(self, value: StudyDrugs | None) -> None:
        self._study_drugs = self.validate_singleton(
            value,
            item_type=StudyDrugs,
            patient_id=self._patient_id,
            field_name=self.__class__.study_drugs.fset.__name__,
        )
        self.updated_fields.add(StudyDrugs.__name__)

    @property
    def biomarkers(self) -> Biomarkers | None:
        return self._biomarkers

    @biomarkers.setter
    def biomarkers(self, value: Biomarkers | None) -> None:
        self._biomarkers = self.validate_singleton(
            value,
            item_type=Biomarkers,
            patient_id=self._patient_id,
            field_name=self.__class__.biomarkers.fset.__name__,
        )
        self.updated_fields.add(Biomarkers.__name__)

    @property
    def lost_to_followup(self) -> FollowUp | None:
        return self._lost_to_followup

    @lost_to_followup.setter
    def lost_to_followup(self, value: FollowUp | None) -> None:
        self._lost_to_followup = self.validate_singleton(
            value,
            item_type=FollowUp,
            patient_id=self._patient_id,
            field_name=self.__class__.lost_to_followup.fset.__name__,
        )
        self.updated_fields.add(FollowUp.__name__)

    @property
    def ecog_baseline(self) -> EcogBaseline | None:
        return self._ecog_baseline

    @ecog_baseline.setter
    def ecog_baseline(self, value: EcogBaseline | None) -> None:
        self._ecog_baseline = self.validate_singleton(
            value,
            item_type=EcogBaseline,
            patient_id=self._patient_id,
            field_name=self.__class__.ecog_baseline.fset.__name__,
        )
        self.updated_fields.add(EcogBaseline.__name__)

    @property
    def tumor_assessment_baseline(self) -> TumorAssessmentBaseline | None:
        return self._tumor_assessment_baseline

    @tumor_assessment_baseline.setter
    def tumor_assessment_baseline(self, value: TumorAssessmentBaseline | None) -> None:
        self._tumor_assessment_baseline = self.validate_singleton(
            value,
            item_type=TumorAssessmentBaseline,
            patient_id=self._patient_id,
            field_name=self.__class__.tumor_assessment_baseline.fset.__name__,
        )
        self.updated_fields.add(TumorAssessmentBaseline.__name__)

    @property
    def best_overall_response(self) -> BestOverallResponse | None:
        return self._best_overall_response

    @best_overall_response.setter
    def best_overall_response(self, value: BestOverallResponse | None | None) -> None:
        self._best_overall_response = self.validate_singleton(
            value,
            item_type=BestOverallResponse,
            patient_id=self._patient_id,
            field_name=self.__class__.best_overall_response.fset.__name__,
        )
        self.updated_fields.add(BestOverallResponse.__name__)

    # collections
    @property
    def medical_histories(self) -> tuple[MedicalHistory, ...]:
        return self._medical_histories

    @medical_histories.setter
    def medical_histories(self, value: Sequence[MedicalHistory] | None) -> None:
        self._medical_histories = self.validate_collection(
            value, item_type=MedicalHistory, patient_id=self._patient_id, field_name=self.__class__.medical_histories.fset.__name__
        )
        self.updated_fields.add(self.__class__.medical_histories.fset.__name__)

    @property
    def previous_treatments(self) -> tuple[PreviousTreatments, ...]:
        return self._previous_treatments

    @previous_treatments.setter
    def previous_treatments(self, value: Sequence[PreviousTreatments] | None) -> None:
        self._previous_treatments = self.validate_collection(
            value, item_type=PreviousTreatments, patient_id=self._patient_id, field_name=self.__class__.previous_treatments.fset.__name__
        )
        self.updated_fields.add(self.__class__.previous_treatments.fset.__name__)

    @property
    def treatment_cycles(self) -> tuple[TreatmentCycle, ...]:
        return self._treatment_cycles

    @treatment_cycles.setter
    def treatment_cycles(self, value: Sequence[TreatmentCycle] | None) -> None:
        self._treatment_cycles = self.validate_collection(
            value, item_type=TreatmentCycle, patient_id=self._patient_id, field_name=self.__class__.treatment_cycles.fset.__name__
        )
        self.updated_fields.add(self.__class__.treatment_cycles.fset.__name__)

    @property
    def concomitant_medications(self) -> tuple[ConcomitantMedication, ...]:
        return self._concomitant_medications

    @concomitant_medications.setter
    def concomitant_medications(self, value: Sequence[ConcomitantMedication] | None) -> None:
        self._concomitant_medications = self.validate_collection(
            value,
            item_type=ConcomitantMedication,
            patient_id=self._patient_id,
            field_name=self.__class__.concomitant_medications.fset.__name__,
        )
        self.updated_fields.add(self.__class__.concomitant_medications.fset.__name__)

    @property
    def adverse_events(self) -> tuple[AdverseEvent, ...]:
        return self._adverse_events

    @adverse_events.setter
    def adverse_events(self, value: Sequence[AdverseEvent] | None) -> None:
        self._adverse_events = Patient.validate_collection(
            value,
            item_type=AdverseEvent,
            patient_id=self._patient_id,
            field_name=self.__class__.adverse_events.fset.__name__,
        )
        self.updated_fields.add(self.__class__.adverse_events.fset.__name__)

    @property
    def tumor_assessments(self) -> tuple[TumorAssessment, ...]:
        return self._tumor_assessments

    @tumor_assessments.setter
    def tumor_assessments(self, value: Sequence[TumorAssessment] | None) -> None:
        self._tumor_assessments = Patient.validate_collection(
            value,
            item_type=TumorAssessment,
            patient_id=self._patient_id,
            field_name=self.__class__.tumor_assessments.fset.__name__,
        )
        self.updated_fields.add(self.__class__.tumor_assessments.fset.__name__)

    @property
    def c30_collection(self) -> tuple[C30, ...]:
        return self._c30_collection

    @c30_collection.setter
    def c30_collection(self, value: Sequence[C30] | None) -> None:
        self._c30_collection = Patient.validate_collection(
            value,
            item_type=C30,
            patient_id=self._patient_id,
            field_name=self.__class__.c30_collection.fset.__name__,
        )
        self.updated_fields.add(self.__class__.c30_collection.fset.__name__)

    @property
    def eq5d_collection(self) -> tuple[EQ5D, ...]:
        return self._eq5d_collection

    @eq5d_collection.setter
    def eq5d_collection(self, value: Sequence[EQ5D] | None) -> None:
        self._eq5d_collection = self.validate_collection(
            value,
            item_type=EQ5D,
            patient_id=self._patient_id,
            field_name=self.__class__.eq5d_collection.fset.__name__,
        )
        self.updated_fields.add(self.__class__.eq5d_collection.fset.__name__)

    @staticmethod
    def validate_singleton(
        value: T | None,
        *,
        item_type: type[T],
        patient_id: str,
        field_name: str = "item",
    ) -> T | None:
        """
        Validate an optional singleton.

        Boundary invariant is: every element must be None or instance of item_type,
        and must have _patient_id == patient_id.
        """
        if value is None:
            return None

        if not isinstance(value, item_type):
            raise TypeError(f"{field_name}: expected {item_type.__name__} | None, got {type(value)}")

        if not hasattr(value, "_patient_id"):
            raise TypeError(f"{field_name}: {type(value).__name__} has no '_patient_id' attribute")

        existing = getattr(value, "_patient_id")
        if existing != patient_id:
            raise ValueError(f"{field_name}: mismatched patient_id {existing!r} != {patient_id!r}")

        return value

    @staticmethod
    def validate_collection(
        value: Sequence[T] | None,
        *,
        item_type: type[T],
        patient_id: str,
        field_name: str = "items",
    ) -> tuple[T, ...]:
        """
        Validate an optional sequence and freeze as an immutable tuple.

        Boundary invariant is: every element must be None or instance of item_type,
        and must have _patient_id == patient_id.
        """
        if value is None:
            items: list[T] = []
        else:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise TypeError(f"{field_name}: expected Sequence[{item_type.__name__}], got {type(value)}")

            wrong = [type(x) for x in value if not isinstance(x, item_type)]
            if wrong:
                raise TypeError(f"{field_name}: all elements must be {item_type.__name__}, got {wrong}")

            items = list(value)

        for x in items:
            if not hasattr(x, "_patient_id"):
                raise TypeError(f"{field_name}: {type(x).__name__} has no '_patient_id' attribute")
            existing = getattr(x, "_patient_id")
            if existing != patient_id:
                raise ValueError(f"{field_name}: mismatched patient_id {existing!r} != {patient_id!r}")

        return tuple(items)

    @classmethod
    def get_attr_for_type(cls, item_type: type) -> str:
        """
        Find the attribute name that accepts the given type by inspecting property return type hints.
        Results are cached for performance.

        Works for both singletons (T | None) and collections (tuple[T, ...]).
        """
        if item_type in cls._attr_cache:
            return cls._attr_cache[item_type]

        for name in dir(cls):
            if name.startswith("_"):
                continue

            attr = getattr(cls, name, None)
            if not isinstance(attr, property) or attr.fget is None:
                continue

            hints = get_type_hints(attr.fget)
            return_hint = hints.get("return")
            if return_hint is None:
                continue

            origin = get_origin(return_hint)

            # collections: tuple[T, ...]
            if origin is tuple:
                args = get_args(return_hint)
                if args and args[0] is item_type:
                    cls._attr_cache[item_type] = name
                    return name

            # singletons: T | None (Union[T, None])
            if origin is Union:
                args = get_args(return_hint)
                if item_type in args:
                    cls._attr_cache[item_type] = name
                    return name

        raise KeyError(f"No attribute found for type {item_type.__name__}")

    def get_updated_fields(self) -> Set[str]:
        return self.updated_fields

    def __iter__(self):
        return iter(self.__dict__.values())

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        delim = ","
        return (
            f"{self.__class__.__name__}("
            # sclalars
            f"patient_id={self.patient_id}{delim} "
            f"trial_id={self.trial_id}{delim} "
            f"cohort_name={self.cohort_name}{delim} "
            f"sex={self.sex}{delim} "
            f"age={self.age}{delim} "
            f"date_of_birth={self.date_of_birth}{delim} "
            f"treatment_start_last_cycle={self.treatment_start_last_cycle}{delim} "
            f"date_of_death={self.date_of_death}{delim} "
            f"has_any_adverse_events={self.has_any_adverse_events}{delim} "
            f"number_of_adverse_events={self.number_of_adverse_events}{delim} "
            f"number_of_serious_adverse_events={self.number_of_serious_adverse_events}{delim} "
            f"evaluable_for_efficacy_analysis={self.evaluable_for_efficacy_analysis}{delim} "
            f"treatment_start_date={self.treatment_start_date}{delim} "
            f"has_clinical_benefit_at_week16={self.has_clinical_benefit_at_week16}{delim} "
            f"end_of_treatment_reason={self.end_of_treatment_reason}{delim} "
            f"end_of_treatment_date={self.end_of_treatment_date}{delim} "
            # singletons
            f"tumor_type={self.tumor_type}{delim} "
            f"tumor_assessment_baseline={self.tumor_assessment_baseline}{delim} "
            f"biomarkers={self.biomarkers}{delim} "
            f"ecog={self.ecog_baseline}{delim} "
            f"lost_to_followup={self.lost_to_followup}{delim} "
            f"best_overall_response={self.best_overall_response}{delim} "
            # collections
            f"study_drugs={self.study_drugs}{delim} "
            f"medical_histories={self.medical_histories}{delim} "
            f"previous_treatments={self.previous_treatments}{delim} "
            f"treatment_cycles={self.treatment_cycles}{delim} "
            f"concomitant_medications={self.concomitant_medications}{delim} "
            f"adverse_events={self.adverse_events}{delim} "
            f"tumor_assessments={self.tumor_assessments}{delim} "
            f"EQ5D={self.eq5d_collection}{delim} "
            f"C30={self.c30_collection}"
            f")"
        )
