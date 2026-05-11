import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory


PATIENT_ID = "P001"


def _ae(start_date: dt.date | None, sequence_id: int | None, term: str = "nausea") -> AdverseEvent:
    e = AdverseEvent(PATIENT_ID)
    e.term = term
    e.start_date = start_date
    e.sequence_id = sequence_id
    return e


class TestCollectionSorting:
    def test_sorted_by_natural_key_on_assignment(self):
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        unsorted = [
            _ae(dt.date(2024, 3, 1), 2),
            _ae(dt.date(2024, 1, 1), 1),
            _ae(dt.date(2024, 2, 1), 5),
        ]
        p.adverse_events = unsorted

        ordered = [(e.start_date, e.sequence_id) for e in p.adverse_events]
        assert ordered == [
            (dt.date(2024, 1, 1), 1),
            (dt.date(2024, 2, 1), 5),
            (dt.date(2024, 3, 1), 2),
        ]

    def test_tiebreak_by_secondary_natural_key_field(self):
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        same_date = dt.date(2024, 1, 1)
        p.adverse_events = [
            _ae(same_date, 3),
            _ae(same_date, 1),
            _ae(same_date, 2),
        ]
        assert [e.sequence_id for e in p.adverse_events] == [1, 2, 3]

    def test_none_values_sort_last(self):
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        p.adverse_events = [
            _ae(None, 1),
            _ae(dt.date(2024, 2, 1), 2),
            _ae(dt.date(2024, 1, 1), 3),
        ]
        ordered = [(e.start_date, e.sequence_id) for e in p.adverse_events]
        assert ordered == [
            (dt.date(2024, 1, 1), 3),
            (dt.date(2024, 2, 1), 2),
            (None, 1),
        ]

    def test_none_in_secondary_field_sorts_last_within_group(self):
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        same_date = dt.date(2024, 1, 1)
        p.adverse_events = [
            _ae(same_date, None),
            _ae(same_date, 2),
            _ae(same_date, 1),
        ]
        assert [e.sequence_id for e in p.adverse_events] == [1, 2, None]

    def test_all_none_natural_key_is_stable(self):
        """All-None keys produce equal sort keys, the order is stable."""
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        a = _ae(None, None, term="A")
        b = _ae(None, None, term="B")
        c = _ae(None, None, term="C")
        p.adverse_events = [a, b, c]
        assert [e.term for e in p.adverse_events] == ["A", "B", "C"]

    def test_empty_collection(self):
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        p.adverse_events = []
        assert p.adverse_events == ()

    def test_works_across_domain_types(self):
        """Same mechanism works on a different collection with the same NK shape."""
        p = Patient(patient_id=PATIENT_ID, trial_id="T1")
        mh1 = MedicalHistory(PATIENT_ID)
        mh1.term = "hypertension"
        mh1.start_date = dt.date(2024, 3, 1)
        mh1.sequence_id = 1

        mh2 = MedicalHistory(PATIENT_ID)
        mh2.term = "diabetes"
        mh2.start_date = dt.date(2024, 1, 1)
        mh2.sequence_id = 2

        p.medical_histories = [mh1, mh2]
        assert [m.start_date for m in p.medical_histories] == [
            dt.date(2024, 1, 1),
            dt.date(2024, 3, 1),
        ]
