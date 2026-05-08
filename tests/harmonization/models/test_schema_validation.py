import datetime as dt
from typing import get_type_hints, get_origin, get_args, Union

import pytest

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.base import DomainBase
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.best_overall_response import BestOverallResponse
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatment
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.domain.tumor_type import TumorType


PATIENT_EXCLUDED_PROPERTIES = {"patient_id", "trial_id"}


def _get_all_constant_values(*sections) -> set[str]:
    values = set()
    for section in sections:
        for name, value in vars(section).items():
            if not name.startswith("_") and isinstance(value, str):
                values.add(value)
    return values


_DOMAIN_BASE_PROPERTIES = {name for name, attr in vars(DomainBase).items() if isinstance(attr, property)}


def _get_data_properties(cls) -> set[str]:
    props = set()
    for name in dir(cls):
        if name.startswith("_") or name in _DOMAIN_BASE_PROPERTIES:
            continue
        attr = getattr(cls, name, None)
        if isinstance(attr, property):
            props.add(name)
    return props


class TestPatientSchemaCompleteness:
    def test_every_patient_property_has_a_constant(self):
        """Every Patient data property must have a corresponding constant."""
        all_constants = _get_all_constant_values(Patient.Scalars, Patient.Singletons, Patient.Collections)
        properties = _get_data_properties(Patient) - PATIENT_EXCLUDED_PROPERTIES

        missing = properties - all_constants
        assert not missing, f"Patient properties without constants in Scalars/Singletons/Collections: {missing}"

    def test_no_extra_constants(self):
        """Every constant value must correspond to an actual Patient property."""
        all_constants = _get_all_constant_values(Patient.Scalars, Patient.Singletons, Patient.Collections)
        properties = _get_data_properties(Patient)

        extra = all_constants - properties
        assert not extra, f"Constants that don't match any Patient property: {extra}"


ALL_DOMAIN_CLASSES = [
    AdverseEvent,
    BestOverallResponse,
    Biomarkers,
    C30,
    ConcomitantMedication,
    EcogBaseline,
    EQ5D,
    FollowUp,
    MedicalHistory,
    PreviousTreatment,
    StudyDrugs,
    TreatmentCycleComponent,
    TumorAssessment,
    TumorAssessmentBaseline,
    TumorType,
]


class TestDomainSchemaCompleteness:
    @pytest.mark.parametrize("domain_cls", ALL_DOMAIN_CLASSES, ids=lambda c: c.__name__)
    def test_every_domain_property_has_a_field(self, domain_cls):
        """Every domain data property (excluding patient_id) must have a Fields constant."""
        fields_cls = getattr(domain_cls, "Fields", None)
        assert fields_cls is not None, f"{domain_cls.__name__} has no Fields inner class"

        field_values = _get_all_constant_values(fields_cls)
        properties = _get_data_properties(domain_cls) - {"patient_id"}

        missing = properties - field_values
        assert not missing, f"{domain_cls.__name__} properties without Fields constants: {missing}"

    @pytest.mark.parametrize("domain_cls", ALL_DOMAIN_CLASSES, ids=lambda c: c.__name__)
    def test_no_extra_domain_fields(self, domain_cls):
        domain_cls.data_fields()  # triggers _ensure_schema that validates


SCALAR_TYPES = (str, int, float, bool, dt.date, dt.datetime)


def _get_property_kind(prop_name: str) -> str:
    """Classify a Patient property as scalar, singleton, or collection by its return type."""
    prop = getattr(Patient, prop_name)
    hints = get_type_hints(prop.fget)
    return_hint = hints.get("return")
    if return_hint is None:
        return "unknown"

    origin = get_origin(return_hint)

    # tuple[T, ...] to collection
    if origin is tuple:
        return "collection"

    # T | None (Union[T, None]) to check if T is a domain class or a scalar
    if origin is Union:
        args = [a for a in get_args(return_hint) if a is not type(None)]
        if args and issubclass(args[0], DomainBase):
            return "singleton"
        return "scalar"

    # bare type, no Optional: scalar
    if isinstance(return_hint, type) and issubclass(return_hint, SCALAR_TYPES):
        return "scalar"

    return "unknown"


class TestPatientCategorization:
    """Verify that Scalars/Singletons/Collections contain correctly categorized properties."""

    def test_scalars_are_scalars(self):
        for name, value in vars(Patient.Scalars).items():
            if name.startswith("_") or not isinstance(value, str):
                continue
            kind = _get_property_kind(value)
            assert kind == "scalar", f"Patient.Scalars.{name} = {value!r} but property is a {kind}, not a scalar"

    def test_singletons_are_singletons(self):
        for name, value in vars(Patient.Singletons).items():
            if name.startswith("_") or not isinstance(value, str):
                continue
            kind = _get_property_kind(value)
            assert kind == "singleton", f"Patient.Singletons.{name} = {value!r} but property is a {kind}, not a singleton"

    def test_collections_are_collections(self):
        for name, value in vars(Patient.Collections).items():
            if name.startswith("_") or not isinstance(value, str):
                continue
            kind = _get_property_kind(value)
            assert kind == "collection", f"Patient.Collections.{name} = {value!r} but property is a {kind}, not a collection"
