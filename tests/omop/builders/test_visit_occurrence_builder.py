import datetime as dt

import pytest

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.visit import Visit
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.core.linkage import BuildResult, SourceReference
from omop_etl.omop.models.tables import OmopTables
from tests.omop.conftest import (
    create_build_context,
    create_patient,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


def _visit(date: dt.date | None, event_id: str | None = "V00") -> Visit:
    v = Visit(patient_id=PID)
    v.date = date
    v.event_id = event_id
    return v


class TestVisitOccurrenceBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert VisitOccurrenceBuilder(concepts).table_name == "visit_occurrence"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_single_visit_all_fields(self, static_index, structural_index):
        """One Visit: one visit_occurrence, source_value is the event id."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2023, 1, 15), "V00VI")]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.person_id == PERSON_ID
        assert row.visit_start_date == dt.date(2023, 1, 15)
        assert row.visit_end_date == dt.date(2023, 1, 15)
        assert row.visit_source_value == "V00VI"
        assert row.visit_concept_id == 9202  # outpatient_visit
        assert row.visit_type_concept_id == 32809  # ecrf

    def test_visit_without_date_is_skipped(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(None, "V00")]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_event_id_none_still_builds_if_date(self, static_index, structural_index):
        """event_id is not required, a dated visit still produces a row."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2023, 3, 1), None)]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].visit_source_value is None


class TestVisitRowsSupersetAndOrdering:
    def test_multiple_distinct_dates_create_multiple_visits(self, static_index, structural_index):
        """Every VI visit becomes a row, ordered by date (NK)."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        # assigned out of order; the collection sorts by date (NK)
        patient.visits = [_visit(dt.date(2023, 4, 1), "V02"), _visit(dt.date(2023, 3, 1), "V00")]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert result.rows[0].visit_start_date == dt.date(2023, 3, 1)
        assert result.rows[0].visit_source_value == "V00"
        assert result.rows[1].visit_start_date == dt.date(2023, 4, 1)
        assert result.rows[1].visit_source_value == "V02"
        assert result.rows[0].visit_occurrence_id != result.rows[1].visit_occurrence_id

    def test_visits_include_non_assessment_dates(self, static_index, structural_index):
        """Superset: screening / EOT visits with no tumor assessment still appear."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2023, 1, 1), "Scr"), _visit(dt.date(2023, 9, 1), "EOT")]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert {r.visit_source_value for r in result.rows} == {"Scr", "EOT"}

    def test_row_ids_are_deterministic(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2023, 1, 1), "V00")]

        result_a = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))
        result_b = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result_a.rows[0].visit_occurrence_id == result_b.rows[0].visit_occurrence_id


class TestPrecedingVisitOccurrenceId:
    def test_single_visit_has_no_preceding(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2023, 1, 1), "V00")]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].preceding_visit_occurrence_id is None

    def test_chain_of_three_visits(self, static_index, structural_index):
        """Each visit points to the date-previous visit."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [
            _visit(dt.date(2023, 1, 1), "V00"),
            _visit(dt.date(2023, 3, 1), "V02"),
            _visit(dt.date(2023, 5, 1), "V04"),
        ]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 3
        assert result.rows[0].preceding_visit_occurrence_id is None
        assert result.rows[1].preceding_visit_occurrence_id == result.rows[0].visit_occurrence_id
        assert result.rows[2].preceding_visit_occurrence_id == result.rows[1].visit_occurrence_id


class TestPublicationAndResolution:
    def test_resolve_visit_id_by_date(self, static_index, structural_index):
        """A published visit resolves by its date via ctx.resolve_visit_id."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2024, 7, 28), "V00VI")]

        ctx = create_build_context(patient, PERSON_ID)
        rows = VisitOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 1
        assert ctx.resolve_visit_id(dt.date(2024, 7, 28)) == rows[0].visit_occurrence_id

    def test_unmatched_date_resolves_to_none(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.visits = [_visit(dt.date(2024, 7, 28), "V00")]

        ctx = create_build_context(patient, PERSON_ID)
        VisitOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert ctx.resolve_visit_id(dt.date(2024, 8, 1)) is None

    def test_publication_source_ref_is_visit_collection(self, static_index, structural_index):
        """Published under the real Visit SourceReference, NK = (date,)."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        date = dt.date(2024, 7, 28)
        patient.visits = [_visit(date, "V00")]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        pub = result.publications[0]
        assert pub.target_table == OmopTables.VISIT_OCCURRENCE
        assert pub.source_ref == SourceReference(PID, Patient.Collections.VISITS, (date,))


class TestSameDateInvariant:
    def test_two_visits_same_date_rejected_by_patient(self):
        """One visit = one date is a Patient trust-boundary invariant: the
        same-date collapse happens upstream in the harmonizer, so a collection
        with two visits sharing a date is a hard error, not a builder concern."""
        patient = create_patient(PID, TRIAL)
        with pytest.raises(ValueError):
            patient.visits = [_visit(dt.date(2023, 1, 1), "V00"), _visit(dt.date(2023, 1, 1), "V00VI")]
