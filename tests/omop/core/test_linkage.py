import datetime as dt

import pytest

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.core.linkage import OmopRowReference, RowPublication, SourceReference, polymorphic_row_key
from omop_etl.omop.models.rows import EpisodeEventRow, MeasurementRow
from omop_etl.omop.models.tables import OmopTables

PID = "p1"


def _ref() -> SourceReference:
    return SourceReference(PID, Patient.Collections.ADVERSE_EVENTS, ("ae1",))


def _row(table: str = OmopTables.CONDITION_OCCURRENCE) -> OmopRowReference:
    return OmopRowReference(table=table, row_id=1, primary_concept_id=0)


class TestSourceReference:
    def test_unknown_source_kind_raises(self):
        with pytest.raises(ValueError, match="Unknown source_kind"):
            SourceReference(PID, "not_a_real_collection", ("k",))


class TestOmopRowReference:
    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown OMOP table"):
            OmopRowReference(table="not_a_real_table", row_id=1, primary_concept_id=0)


class TestPolymorphicRowKey:
    def test_episode_event_key_is_the_full_triple(self):
        row = EpisodeEventRow(episode_id=1, event_id=999, episode_event_field_concept_id=400)

        assert polymorphic_row_key(OmopTables.EPISODE_EVENT, row) == (1, 999, 400)

    def test_other_table_key_is_its_own_pk(self):
        row = MeasurementRow(
            measurement_id=42,
            person_id=1,
            measurement_concept_id=100,
            measurement_date=dt.date(2023, 1, 1),
            measurement_type_concept_id=200,
        )

        assert polymorphic_row_key(OmopTables.MEASUREMENT, row) == (42,)


class TestRowPublication:
    def test_unknown_target_table_raises(self):
        with pytest.raises(ValueError, match="Unknown OMOP target_table"):
            RowPublication(target_table="not_a_real_table", source_ref=_ref(), rows=(_row(),))

    def test_empty_rows_raises(self):
        with pytest.raises(ValueError, match="Cannot publish empty row set"):
            RowPublication(target_table=OmopTables.CONDITION_OCCURRENCE, source_ref=_ref(), rows=())

    def test_row_table_mismatch_raises(self):
        mismatched_row = _row(table=OmopTables.MEASUREMENT)

        with pytest.raises(ValueError, match="Published row table mismatch"):
            RowPublication(target_table=OmopTables.CONDITION_OCCURRENCE, source_ref=_ref(), rows=(mismatched_row,))
