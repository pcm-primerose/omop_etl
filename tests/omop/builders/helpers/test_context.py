import pytest

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.helpers.context import BuildContext
from omop_etl.omop.core.linkage import OmopRowReference, SourceReference
from omop_etl.omop.models.tables import OmopTables
from tests.omop.conftest import create_build_context, create_patient

PID = "p1"
TRIAL = "test"


def _ctx() -> BuildContext:
    return create_build_context(create_patient(PID, TRIAL))


def _ref() -> SourceReference:
    return SourceReference(PID, Patient.Collections.ADVERSE_EVENTS, ("ae1",))


def _row(table: str = OmopTables.CONDITION_OCCURRENCE) -> OmopRowReference:
    return OmopRowReference(table=table, row_id=1, primary_concept_id=0)


class TestPublishRows:
    def test_unknown_target_table_raises(self):
        with pytest.raises(ValueError, match="Unknown OMOP target_table"):
            _ctx().publish_rows("not_a_real_table", _ref(), [_row()])

    def test_duplicate_publish_for_same_key_raises(self):
        ctx = _ctx()
        ref = _ref()
        ctx.publish_rows(OmopTables.CONDITION_OCCURRENCE, ref, [_row()])

        with pytest.raises(RuntimeError, match="Duplicate publish"):
            ctx.publish_rows(OmopTables.CONDITION_OCCURRENCE, ref, [_row()])

    def test_empty_row_set_raises(self):
        with pytest.raises(ValueError, match="Cannot publish empty row set"):
            _ctx().publish_rows(OmopTables.CONDITION_OCCURRENCE, _ref(), [])

    def test_row_table_mismatch_raises(self):
        mismatched_row = _row(table=OmopTables.MEASUREMENT)

        with pytest.raises(ValueError, match="Published row table mismatch"):
            _ctx().publish_rows(OmopTables.CONDITION_OCCURRENCE, _ref(), [mismatched_row])
