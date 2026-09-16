import datetime as dt
import pytest

from omop_etl.omop.core.linkage import PolymorphicTarget
from omop_etl.omop.core.pre_load_gate import PreLoadIntegrityError, validate_integrity
from omop_etl.omop.models.rows import (
    CohortDefinitionRow,
    CohortRow,
    ConditionOccurrenceRow,
    DeathRow,
    EpisodeEventRow,
    EpisodeRow,
    PersonRow,
    VisitOccurrenceRow,
)
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.models import AthenaConcept
from omop_etl.vocabulary.core.vocabulary import Vocabulary

D = dt.date


def _concept(concept_id: int) -> AthenaConcept:
    return AthenaConcept(
        concept_id=concept_id,
        concept_code="x",
        concept_name="x",
        domain_id="x",
        vocabulary_id="x",
        concept_class_id="x",
        validity="valid",
    )


def _vocab(*concept_ids: int) -> Vocabulary:
    return Vocabulary({cid: _concept(cid) for cid in {0, *concept_ids}})


def _person(person_id: int, gender_concept_id: int = 8507) -> PersonRow:
    return PersonRow(
        person_id=person_id,
        gender_concept_id=gender_concept_id,
        year_of_birth=1980,
        race_concept_id=0,
        ethnicity_concept_id=0,
    )


def _visit(visit_occurrence_id: int, person_id: int, visit_concept_id: int = 9202) -> VisitOccurrenceRow:
    return VisitOccurrenceRow(
        visit_occurrence_id=visit_occurrence_id,
        person_id=person_id,
        visit_concept_id=visit_concept_id,
        visit_start_date=D(2023, 1, 1),
        visit_end_date=D(2023, 1, 1),
        visit_type_concept_id=32809,
    )


class TestValidateIntegrity:
    def test_clean_data_raises_nothing(self):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        tables.extend(OmopTables.VISIT_OCCURRENCE, rows=[_visit(visit_occurrence_id=10, person_id=1)])
        vocabulary = _vocab(8507, 9202, 32809)

        validate_integrity(tables, vocabulary)

    def test_empty_tables_raises_nothing(self):
        validate_integrity(OmopTables(), Vocabulary({}))

    def test_duplicate_pk_raises(self):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1), _person(1)])
        vocabulary = _vocab(8507)

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)

        assert any(v.kind == "duplicate_pk" and v.table == OmopTables.PERSON for v in exc_info.value.violations)

    def test_orphan_fk_raises(self):
        tables = OmopTables()
        tables.extend(OmopTables.VISIT_OCCURRENCE, rows=[_visit(visit_occurrence_id=10, person_id=999)])  # no person with id=999
        vocabulary = _vocab(9202, 32809)

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)

        assert any(v.kind == "orphan_fk" and "person_id=999" in v.detail for v in exc_info.value.violations)

    def test_orphan_concept_raises(self):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(person_id=1, gender_concept_id=424242)])
        vocabulary = _vocab()  # 424242 left unmapped

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)

        assert any(v.kind == "orphan_concept" and "gender_concept_id=424242" in v.detail for v in exc_info.value.violations)

    def test_nul_byte_in_string_field_raises(self):
        # Postgres text/varchar columns reject an embedded NUL byte outright,
        # regardless of string length, pydantic doesn't catch it either
        tables = OmopTables()
        tables.extend(
            OmopTables.PERSON,
            rows=[
                PersonRow(
                    person_id=1,
                    gender_concept_id=8507,
                    year_of_birth=1980,
                    race_concept_id=0,
                    ethnicity_concept_id=0,
                    person_source_value="p\x001",
                )
            ],
        )
        vocabulary = _vocab(8507)

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)

        assert any(v.kind == "nul_byte" and v.table == OmopTables.PERSON and "person_source_value" in v.detail for v in exc_info.value.violations)

    def test_null_fk_is_skipped(self):
        # preceding_visit_occurrence_id is None: must not be treated as an orphan
        tables = OmopTables()
        tables.extend(OmopTables.VISIT_OCCURRENCE, rows=[_visit(visit_occurrence_id=10, person_id=1)])
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        vocabulary = _vocab(8507, 9202, 32809)

        validate_integrity(tables, vocabulary)

    def test_death_uses_person_id_as_its_pk(self):
        tables = OmopTables()
        tables.extend(
            OmopTables.DEATH,
            rows=[
                DeathRow(person_id=1, death_date=D(2023, 1, 1)),
                DeathRow(person_id=1, death_date=D(2023, 6, 1)),
            ],
        )
        # satisfy the real and correctly-checked person_id FK so only the
        # PK-uniqueness-override behavior is tested
        tables.extend(OmopTables.PERSON, rows=[_person(1)])

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, _vocab(8507))

        assert any(v.kind == "duplicate_pk" and v.table == OmopTables.DEATH and "person_id=1" in v.detail for v in exc_info.value.violations)

    def test_death_person_id_is_also_checked_as_fk_into_person(self):
        # death.person_id has two roles:
        # it's death's dedup key (from `PK_COLUMN_OVERRIDES`)
        # and a real FK into person, every other table has FKs that don't link to another table
        tables = OmopTables()
        tables.extend(OmopTables.DEATH, rows=[DeathRow(person_id=999, death_date=D(2023, 1, 1))])

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, _vocab())

        assert any(v.kind == "orphan_fk" and v.table == OmopTables.DEATH and "person_id=999 not found in person" in v.detail for v in exc_info.value.violations)

    def test_cohort_has_no_surrogate_pk_field_checked(self):
        # cohort has no {table}_id field: it must not be checked as a
        # single-column PK (composite-key uniqueness is checked separately)
        tables = OmopTables()
        tables.extend(
            OmopTables.COHORT,
            [
                CohortRow(
                    cohort_definition_id=1,
                    subject_id=1,
                    cohort_start_date=D(2023, 1, 1),
                    cohort_end_date=D(2023, 6, 1),
                ),
                CohortRow(
                    cohort_definition_id=1,
                    subject_id=2,
                    cohort_start_date=D(2023, 1, 1),
                    cohort_end_date=D(2023, 6, 1),
                ),
            ],
        )
        tables.extend(OmopTables.PERSON, rows=[_person(1), _person(2)])
        tables.extend(
            OmopTables.COHORT_DEFINITION,
            rows=[CohortDefinitionRow(cohort_definition_id=1, cohort_definition_name="x", definition_type_concept_id=0, subject_concept_id=0)],
        )

        validate_integrity(tables, _vocab(8507))

    def test_duplicate_cohort_composite_key_raises(self):
        # (cohort_definition_id, subject_id, cohort_start_date) is cohort's NK:
        # two rows sharing it are a real duplicate even though cohort_end_date differs
        tables = OmopTables()
        tables.extend(
            OmopTables.COHORT,
            [
                CohortRow(
                    cohort_definition_id=1,
                    subject_id=1,
                    cohort_start_date=D(2023, 1, 1),
                    cohort_end_date=D(2023, 6, 1),
                ),
                CohortRow(
                    cohort_definition_id=1,
                    subject_id=1,
                    cohort_start_date=D(2023, 1, 1),
                    cohort_end_date=D(2023, 7, 1),
                ),
            ],
        )
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        tables.extend(
            OmopTables.COHORT_DEFINITION,
            rows=[CohortDefinitionRow(cohort_definition_id=1, cohort_definition_name="x", definition_type_concept_id=0, subject_concept_id=0)],
        )

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, _vocab(8507))

        assert any(v.kind == "duplicate_pk" and v.table == OmopTables.COHORT for v in exc_info.value.violations)

    def test_episode_parent_id_checked_against_episode_table_not_a_literal_episode_parent_table(self):
        tables = OmopTables()
        parent = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        child_with_orphan_parent = EpisodeRow(
            episode_id=2,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 2, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
            episode_parent_id=999,  # no episode 999
        )
        tables.extend(OmopTables.EPISODE, [parent, child_with_orphan_parent])
        vocabulary = _vocab(100, 200, 300)

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)

        assert any(v.kind == "orphan_fk" and "episode_parent_id=999 not found in episode" in v.detail for v in exc_info.value.violations)

    def test_polymorphic_event_id_orphan_raises(self):
        # event_id=999 is recorded as targeting condition_occurrence
        # (as a real builder would via ctx.record_polymorphic_target),
        # but this row doesn't exist (orphan) and must raise
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        episode = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        tables.extend(OmopTables.EPISODE, rows=[episode])
        event_row = EpisodeEventRow(episode_id=1, event_id=999, episode_event_field_concept_id=400)
        tables.extend(OmopTables.EPISODE_EVENT, rows=[event_row])
        vocabulary = _vocab(8507, 100, 200, 300, 400)
        polymorphic_targets = [
            PolymorphicTarget(
                table=OmopTables.EPISODE_EVENT,
                row_key=(event_row.episode_id, event_row.event_id, event_row.episode_event_field_concept_id),
                field="event_id",
                target_table=OmopTables.CONDITION_OCCURRENCE,
            )
        ]

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary, polymorphic_targets)

        assert any(
            v.kind == "orphan_fk" and v.table == OmopTables.EPISODE_EVENT and "event_id=999 not found in condition_occurrence" in v.detail
            for v in exc_info.value.violations
        )

    def test_polymorphic_event_id_valid_reference_does_not_raise(self):
        # event_id correctly points at a real condition_occurrence row
        # and the target table is recorded: no violation
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        episode = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        tables.extend(OmopTables.EPISODE, rows=[episode])
        condition = ConditionOccurrenceRow(
            condition_occurrence_id=500,
            person_id=1,
            condition_concept_id=100,
            condition_start_date=D(2023, 1, 1),
            condition_type_concept_id=300,
        )
        tables.extend(OmopTables.CONDITION_OCCURRENCE, rows=[condition])
        event_row = EpisodeEventRow(episode_id=1, event_id=500, episode_event_field_concept_id=400)
        tables.extend(OmopTables.EPISODE_EVENT, rows=[event_row])
        vocabulary = _vocab(8507, 100, 200, 300, 400)
        polymorphic_targets = [
            PolymorphicTarget(
                table=OmopTables.EPISODE_EVENT,
                row_key=(event_row.episode_id, event_row.event_id, event_row.episode_event_field_concept_id),
                field="event_id",
                target_table=OmopTables.CONDITION_OCCURRENCE,
            )
        ]

        validate_integrity(tables, vocabulary, polymorphic_targets)

    def test_polymorphic_event_id_without_recorded_target_raises(self):
        # event_id is set but no PolymorphicTarget was recorded for it:
        # this is a builder bug (forgot to call record_polymorphic_target)
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        episode = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        tables.extend(OmopTables.EPISODE, rows=[episode])
        tables.extend(
            OmopTables.EPISODE_EVENT,
            rows=[EpisodeEventRow(episode_id=1, event_id=999, episode_event_field_concept_id=400)],
        )
        vocabulary = _vocab(8507, 100, 200, 300, 400)

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)  # no polymorphic_targets passed

        assert any(v.kind == "orphan_fk" and "no recorded polymorphic target table" in v.detail for v in exc_info.value.violations)

    def test_duplicate_episode_event_composite_key_raises(self):
        # episode_event has no surrogate id:
        # (episode_id, event_id, episode_event_field_concept_id) is the NK
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        episode = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        tables.extend(OmopTables.EPISODE, rows=[episode])
        condition = ConditionOccurrenceRow(
            condition_occurrence_id=500,
            person_id=1,
            condition_concept_id=100,
            condition_start_date=D(2023, 1, 1),
            condition_type_concept_id=300,
        )
        tables.extend(OmopTables.CONDITION_OCCURRENCE, rows=[condition])
        row = EpisodeEventRow(episode_id=1, event_id=500, episode_event_field_concept_id=400)
        tables.extend(OmopTables.EPISODE_EVENT, rows=[row, row])
        vocabulary = _vocab(8507, 100, 200, 300, 400)
        polymorphic_targets = [
            PolymorphicTarget(
                table=OmopTables.EPISODE_EVENT,
                row_key=(row.episode_id, row.event_id, row.episode_event_field_concept_id),
                field="event_id",
                target_table=OmopTables.CONDITION_OCCURRENCE,
            )
        ]

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary, polymorphic_targets)

        assert any(v.kind == "duplicate_pk" and v.table == OmopTables.EPISODE_EVENT for v in exc_info.value.violations)

    def test_table_we_dont_populate_is_out_of_scope(self):
        tables = OmopTables()
        tables.extend(OmopTables.CONDITION_OCCURRENCE, [])
        person = _person(1)
        tables.extend(OmopTables.PERSON, [person])
        vocabulary = _vocab(8507)

        validate_integrity(tables, vocabulary)

    def test_collects_every_violation_before_raising_not_just_the_first(self):
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1), _person(1)])  # duplicate_pk
        tables.extend(OmopTables.VISIT_OCCURRENCE, rows=[_visit(visit_occurrence_id=10, person_id=999)])  # orphan_fk
        vocabulary = _vocab(8507, 9202, 32809)

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary)

        kinds = {v.kind for v in exc_info.value.violations}
        assert kinds == {"duplicate_pk", "orphan_fk"}

    def test_conflicting_polymorphic_targets_for_the_same_key_raises(self):
        # episode_event's row_key includes the target's id, which is in a
        # different `RowIdGenerator` namespace than the episode_event,
        # so a (table, row_key, field) is not guaranteed to be collision-free,
        # any recorded target tables shring the same key can't just pick one
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        episode = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        tables.extend(OmopTables.EPISODE, rows=[episode])
        condition = ConditionOccurrenceRow(
            condition_occurrence_id=500,
            person_id=1,
            condition_concept_id=100,
            condition_start_date=D(2023, 1, 1),
            condition_type_concept_id=300,
        )
        tables.extend(OmopTables.CONDITION_OCCURRENCE, rows=[condition])
        row = EpisodeEventRow(episode_id=1, event_id=500, episode_event_field_concept_id=400)
        tables.extend(OmopTables.EPISODE_EVENT, rows=[row])
        vocabulary = _vocab(8507, 100, 200, 300, 400)
        row_key = (row.episode_id, row.event_id, row.episode_event_field_concept_id)
        polymorphic_targets = [
            PolymorphicTarget(table=OmopTables.EPISODE_EVENT, row_key=row_key, field="event_id", target_table=OmopTables.CONDITION_OCCURRENCE),
            PolymorphicTarget(table=OmopTables.EPISODE_EVENT, row_key=row_key, field="event_id", target_table=OmopTables.DRUG_EXPOSURE),
        ]

        with pytest.raises(PreLoadIntegrityError) as exc_info:
            validate_integrity(tables, vocabulary, polymorphic_targets)

        assert any(v.kind == "polymorphic_target_collision" and v.table == OmopTables.EPISODE_EVENT for v in exc_info.value.violations)

    def test_repeated_identical_polymorphic_target_is_not_a_collision(self):
        # `RowIdGenerator` allows for duplicate digests, the same (table, row_key, field)
        # being recorded twice with the same `taget_table` is not problematic
        tables = OmopTables()
        tables.extend(OmopTables.PERSON, rows=[_person(1)])
        episode = EpisodeRow(
            episode_id=1,
            person_id=1,
            episode_concept_id=100,
            episode_start_date=D(2023, 1, 1),
            episode_object_concept_id=200,
            episode_type_concept_id=300,
        )
        tables.extend(OmopTables.EPISODE, rows=[episode])
        condition = ConditionOccurrenceRow(
            condition_occurrence_id=500,
            person_id=1,
            condition_concept_id=100,
            condition_start_date=D(2023, 1, 1),
            condition_type_concept_id=300,
        )
        tables.extend(OmopTables.CONDITION_OCCURRENCE, rows=[condition])
        row = EpisodeEventRow(episode_id=1, event_id=500, episode_event_field_concept_id=400)
        tables.extend(OmopTables.EPISODE_EVENT, rows=[row])
        vocabulary = _vocab(8507, 100, 200, 300, 400)
        row_key = (row.episode_id, row.event_id, row.episode_event_field_concept_id)
        polymorphic_targets = [
            PolymorphicTarget(table=OmopTables.EPISODE_EVENT, row_key=row_key, field="event_id", target_table=OmopTables.CONDITION_OCCURRENCE),
            PolymorphicTarget(table=OmopTables.EPISODE_EVENT, row_key=row_key, field="event_id", target_table=OmopTables.CONDITION_OCCURRENCE),
        ]

        validate_integrity(tables, vocabulary, polymorphic_targets)
