import datetime as dt
from dataclasses import asdict, dataclass
from typing import ClassVar

import pytest
import polars as pl

from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.base import DomainBase
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.harmonizers.base import (
    BaseHarmonizer,
    ScalarSpec,
    SingletonSpec,
    CollectionSpec,
    scalar,
    singleton,
    collection,
)


class SimpleDomain(DomainBase):
    """
    Simple domain for testing with two fields: name and value.
    """

    class Fields:
        NAME = "name"
        VALUE = "value"

    REQUIRED_FIELDS = (Fields.NAME,)

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._name: str | None = None
        self._value: int | None = None
        self.updated_fields: set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, v: str | None) -> None:
        self._name = v

    @property
    def value(self) -> int | None:
        return self._value

    @value.setter
    def value(self, v: int | None) -> None:
        self._value = v


class MinimalHarmonizer(BaseHarmonizer):
    """Minimal concrete harmonizer for testing base class methods."""

    SPECS: ClassVar[tuple] = ()

    def _create_patients(self) -> None:
        for sid in self.data.select("SubjectId").unique().to_series():
            self.patient_data[sid] = Patient(patient_id=sid, trial_id=self.trial_id)


class TestCreatePatients:
    """Tests for _create_patients() behavior from SubjectIds."""

    def test_creates_patient_for_each_unique_subject_id(self):
        data = pl.DataFrame({"SubjectId": ["p1", "p2", "p3"]})
        harmonizer = MinimalHarmonizer(data=data, trial_id="TEST")
        harmonizer._create_patients()

        assert len(harmonizer.patient_data) == 3
        assert set(harmonizer.patient_data.keys()) == {"p1", "p2", "p3"}

    def test_deduplicates_subject_ids(self):
        data = pl.DataFrame({"SubjectId": ["p1", "p1", "p2", "p2", "p2"]})
        harmonizer = MinimalHarmonizer(data=data, trial_id="TEST")
        harmonizer._create_patients()

        assert len(harmonizer.patient_data) == 2
        assert set(harmonizer.patient_data.keys()) == {"p1", "p2"}

    def test_patient_id_matches_key(self):
        data = pl.DataFrame({"SubjectId": ["alpha", "beta", "gamma"]})
        harmonizer = MinimalHarmonizer(data=data, trial_id="TEST")
        harmonizer._create_patients()

        for key, patient in harmonizer.patient_data.items():
            assert patient.patient_id == key

    def test_trial_id_propagation(self):
        data = pl.DataFrame({"SubjectId": ["p1", "p2"]})
        harmonizer = MinimalHarmonizer(data=data, trial_id="MY_TRIAL_123")
        harmonizer._create_patients()

        for patient in harmonizer.patient_data.values():
            assert patient.trial_id == "MY_TRIAL_123"

    def test_empty_data_creates_no_patients(self):
        data = pl.DataFrame({"SubjectId": []})
        harmonizer = MinimalHarmonizer(data=data, trial_id="TEST")
        harmonizer._create_patients()

        assert len(harmonizer.patient_data) == 0

    def test_preserves_all_subject_id_variants(self):
        data = pl.DataFrame(
            {
                "SubjectId": [
                    "unique_1",
                    "unique_2",
                    "unique_3",
                    "unique_4",
                    "duplicate_id",
                    "duplicate_id",
                    "duplicate_variant",
                ]
            }
        )
        harmonizer = MinimalHarmonizer(data=data, trial_id="IMPRESS_TEST")
        harmonizer._create_patients()

        # 6 unique IDs, duplicate_id appears twice but creates one patient
        assert len(harmonizer.patient_data) == 6
        expected_ids = {"unique_1", "unique_2", "unique_3", "unique_4", "duplicate_id", "duplicate_variant"}
        assert set(harmonizer.patient_data.keys()) == expected_ids

        for patient_id, patient in harmonizer.patient_data.items():
            assert patient.patient_id == patient_id
            assert patient.trial_id == "IMPRESS_TEST"


class TestValidateSchemaSubset:
    def test_valid_subset(self):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"]})
        BaseHarmonizer.validate_schema_subset(df, SimpleDomain)  # shouldn't raise

    def test_full_schema(self):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"], "value": [42]})
        BaseHarmonizer.validate_schema_subset(df, SimpleDomain)

    def test_missing_subject_col_raises(self):
        df = pl.DataFrame({"name": ["foo"], "value": [42]})
        with pytest.raises(ValueError, match="Missing subject_col"):
            BaseHarmonizer.validate_schema_subset(df, SimpleDomain)

    def test_unknown_columns_strict_raises(self):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"], "unknown_col": [1]})
        with pytest.raises(ValueError, match="unknown columns"):
            BaseHarmonizer.validate_schema_subset(df, SimpleDomain, strict_unknown=True)

    def test_unknown_columns_lenient_warns(self, caplog):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"], "unknown_col": [1]})
        BaseHarmonizer.validate_schema_subset(df, SimpleDomain, strict_unknown=False)
        assert "unknown columns" in caplog.text.lower()

    def test_subject_col_in_data_fields_raises(self):
        class BadDomain(DomainBase):
            class Fields:
                SUBJECT_ID = "SubjectId"

            def __init__(self, patient_id: str):
                self._patient_id = patient_id
                self._SubjectId: str | None = None
                self.updated_fields: set[str] = set()

            @property
            def patient_id(self) -> str:
                return self._patient_id

            @property
            def SubjectId(self) -> str | None:  # noqa
                return self._SubjectId

            @SubjectId.setter
            def SubjectId(self, v: str | None) -> None:  # noqa
                self._SubjectId = v

        df = pl.DataFrame({"SubjectId": ["p1"]})
        with pytest.raises(ValueError, match="must not be in data_fields"):
            BaseHarmonizer.validate_schema_subset(df, BadDomain)


class TestConformSchema:
    def test_fills_missing_columns_with_null(self):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"]})
        result = BaseHarmonizer.conform_schema(df, SimpleDomain)

        assert result.columns == ["SubjectId", "name", "value"]
        assert result["value"].to_list() == [None]

    def test_preserves_existing_values(self):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"], "value": [42]})
        result = BaseHarmonizer.conform_schema(df, SimpleDomain)

        assert result["name"].to_list() == ["foo"]
        assert result["value"].to_list() == [42]

    def test_drops_extra_columns(self):
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["foo"], "extra": ["drop"]})
        result = BaseHarmonizer.conform_schema(df, SimpleDomain)

        assert "extra" not in result.columns
        assert result.columns == ["SubjectId", "name", "value"]

    def test_column_order_matches_data_fields(self):
        df = pl.DataFrame({"value": [42], "SubjectId": ["p1"], "name": ["foo"]})
        result = BaseHarmonizer.conform_schema(df, SimpleDomain)

        expected_order = ["SubjectId", *SimpleDomain.data_fields()]
        assert result.columns == expected_order


class TestPackStructs:
    def test_groups_rows_by_subject(self):
        """Rows should be grouped into list of structs per subject."""
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p1", "p2"],
                "name": ["a", "b", "c"],
                "value": [1, 2, 3],
            }
        )
        result = BaseHarmonizer.pack_structs(df, value_cols=["name", "value"])

        assert result.columns == ["SubjectId", "items"]
        assert result.height == 2

        p1_items = result.filter(pl.col("SubjectId") == "p1")["items"].to_list()[0]
        assert len(p1_items) == 2
        assert p1_items[0] == {"name": "a", "value": 1}
        assert p1_items[1] == {"name": "b", "value": 2}

    def test_order_by_cols(self):
        """Items should be sorted by order_by_cols within each subject."""
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p1", "p1"],
                "name": ["c", "a", "b"],
                "value": [3, 1, 2],
            }
        )
        result = BaseHarmonizer.pack_structs(df, value_cols=["name", "value"], order_by_cols=["name"])

        p1_items = result["items"].to_list()[0]
        assert [i["name"] for i in p1_items] == ["a", "b", "c"]

    def test_order_by_multiple_cols(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p1", "p1", "p1"],
                "seq": [2, 1, 1, 2],
                "date": ["2024-03-01", "2024-02-01", "2024-01-01", "2024-01-15"],
                "label": ["d", "b", "a", "c"],
            }
        )
        result = BaseHarmonizer.pack_structs(
            df,
            value_cols=["seq", "date", "label"],
            order_by_cols=["seq", "date"],
        )

        p1_items = result["items"].to_list()[0]

        assert [item["seq"] for item in p1_items] == [1, 1, 2, 2]
        assert [item["date"] for item in p1_items] == [
            "2024-01-01",
            "2024-02-01",
            "2024-01-15",
            "2024-03-01",
        ]
        assert [item["label"] for item in p1_items] == ["a", "b", "c", "d"]

    def test_require_order_by_raises(self):
        """require_order_by=True without order_by_cols should raise."""
        df = pl.DataFrame({"SubjectId": ["p1"], "name": ["a"]})
        with pytest.raises(ValueError, match="order_by_cols is required"):
            BaseHarmonizer.pack_structs(df, value_cols=["name"], require_order_by=True)


@pytest.fixture
def mock_simple_domain_attr(monkeypatch):
    """Mock Patient.get_attr_for_type to allow SimpleDomain hydration with a test-only attr."""
    original = Patient.get_attr_for_type

    def mock_get_attr(cls, item_type):  # noqa
        if item_type is SimpleDomain:
            return "_test_simple_domain"
        return original(item_type)

    monkeypatch.setattr(Patient, "get_attr_for_type", classmethod(mock_get_attr))

    # test-only attribute to Patient class for SimpleDomain using raising=False to allow new attr
    setattr(Patient, "_test_simple_domain", None)
    yield
    if hasattr(Patient, "_test_simple_domain"):
        delattr(Patient, "_test_simple_domain")


class TestHydrateSingleton:
    def test_hydrates_single_row_per_patient(self, mock_simple_domain_attr):
        """Each patient should get one domain object via builder."""
        patients = {
            "p1": Patient(patient_id="p1", trial_id="test"),
            "p2": Patient(patient_id="p2", trial_id="test"),
        }
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p2"],
                "name": ["foo", "bar"],
                "value": [1, 2],
            }
        )

        built = {}

        def tracking_builder(pid, row):
            obj = SimpleDomain.from_row(pid, row)
            built[pid] = obj
            return obj

        BaseHarmonizer.hydrate_singleton(
            df,
            item_type=SimpleDomain,
            patients=patients,
            builder=tracking_builder,
        )

        assert built["p1"].name == "foo"
        assert built["p2"].name == "bar"

    def test_skip_missing_patients(self, mock_simple_domain_attr):
        """skip_missing_patients=True should skip unknown subjects."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        df = pl.DataFrame({"SubjectId": ["p1", "unknown"], "name": ["foo", "bar"], "value": [1, 2]})

        built = {}

        def tracking_builder(pid, row):
            obj = SimpleDomain.from_row(pid, row)
            built[pid] = obj
            return obj

        # should not raise
        BaseHarmonizer.hydrate_singleton(
            df,
            item_type=SimpleDomain,
            patients=patients,
            skip_missing_patients=True,
            builder=tracking_builder,
        )
        assert built["p1"].name == "foo"
        assert "unknown" not in built

    def test_missing_patient_raises(self, mock_simple_domain_attr):
        """Missing patient without skip should raise KeyError."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        df = pl.DataFrame({"SubjectId": ["unknown"], "name": ["foo"], "value": [1]})

        with pytest.raises(KeyError, match="unknown"):
            BaseHarmonizer.hydrate_singleton(
                df,
                item_type=SimpleDomain,
                patients=patients,
                skip_missing_patients=False,
            )

    def test_duplicate_error(self, mock_simple_domain_attr):
        """on_duplicate='error' should raise on duplicates."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        df = pl.DataFrame({"SubjectId": ["p1", "p1"], "name": ["foo", "bar"], "value": [1, 2]})

        def noop_builder(pid, row):
            return SimpleDomain.from_row(pid, row)

        with pytest.raises(ValueError, match="Duplicate singleton"):
            BaseHarmonizer.hydrate_singleton(
                df,
                item_type=SimpleDomain,
                patients=patients,
                on_duplicate="error",
                builder=noop_builder,
            )

    def test_duplicate_first(self, mock_simple_domain_attr):
        """on_duplicate='first' should keep first occurrence."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        df = pl.DataFrame({"SubjectId": ["p1", "p1"], "name": ["first", "second"], "value": [1, 2]})

        built = []

        def tracking_builder(pid, row):
            obj = SimpleDomain.from_row(pid, row)
            built.append(obj.name)
            return obj

        BaseHarmonizer.hydrate_singleton(
            df,
            item_type=SimpleDomain,
            patients=patients,
            on_duplicate="first",
            builder=tracking_builder,
        )

        # only first should be built
        assert built == ["first"]

    def test_duplicate_last(self, mock_simple_domain_attr):
        """on_duplicate='last' should keep last occurrence."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        df = pl.DataFrame({"SubjectId": ["p1", "p1"], "name": ["first", "second"], "value": [1, 2]})

        built = []

        def tracking_builder(pid, row):
            obj = SimpleDomain.from_row(pid, row)
            built.append(obj.name)
            return obj

        BaseHarmonizer.hydrate_singleton(
            df,
            item_type=SimpleDomain,
            patients=patients,
            on_duplicate="last",
            builder=tracking_builder,
        )

        # both should be built, last wins
        assert built == ["first", "second"]


class TestHydrateCollectionField:
    def test_hydrates_multiple_items_per_patient(self, mock_simple_domain_attr):
        """Each patient should get a list of domain objects."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}

        packed = pl.DataFrame(
            {
                "SubjectId": ["p1"],
                "items": [[{"name": "a", "value": 1}, {"name": "b", "value": 2}]],
            }
        )

        built_items = []

        def tracking_builder(pid, row):
            obj = SimpleDomain.from_row(pid, row)
            built_items.append(obj)
            return obj

        BaseHarmonizer.hydrate_collection_field(
            packed,
            item_type=SimpleDomain,
            patients=patients,
            builder=tracking_builder,
        )

        assert len(built_items) == 2
        assert [i.name for i in built_items] == ["a", "b"]

    def test_mode_extend(self, mock_simple_domain_attr):
        """mode='extend' should append to existing collection."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}

        all_built = []

        def builder(pid, row):
            obj = SimpleDomain(pid)
            obj.name = row["name"]
            obj.value = row["value"]
            all_built.append(obj)
            return obj

        # first batch
        packed1 = pl.DataFrame({"SubjectId": ["p1"], "items": [[{"name": "a", "value": 1}]]})
        BaseHarmonizer.hydrate_collection_field(packed1, item_type=SimpleDomain, patients=patients, builder=builder, mode="replace")

        # second batch with extend
        packed2 = pl.DataFrame({"SubjectId": ["p1"], "items": [[{"name": "b", "value": 2}]]})
        BaseHarmonizer.hydrate_collection_field(packed2, item_type=SimpleDomain, patients=patients, builder=builder, mode="extend")

        # both batches should have been built
        assert len(all_built) == 2
        assert [i.name for i in all_built] == ["a", "b"]


class TestNaturalKeyConflictDetection:
    """
    Uses AdverseEvent (NATURAL_KEY_FIELDS = (term, start_date, sequence_id)) as
    the test domain. Conflicts are constructed by sharing the full NK and differing
    in a non-NK field (grade).
    """

    def _ae_row(self, *, start_date, sequence_id, term="diarrhea", grade=None, outcome=None):  # noqa
        return {
            "term": term,
            "grade": grade,
            "outcome": outcome,
            "start_date": start_date,
            "end_date": None,
            "sequence_id": sequence_id,
            "was_serious": None,
            "turned_serious_date": None,
            "related_to_treatment_1_status": None,
            "treatment_1_name": None,
            "related_to_treatment_2_status": None,
            "treatment_2_name": None,
            "was_serious_grade_expected_treatment_1": None,
            "was_serious_grade_expected_treatment_2": None,
        }

    def _packed(self, items):  # noqa
        return pl.DataFrame({"SubjectId": ["p1"], "items": [items]})

    def test_identical_duplicates_pass_silently(self, caplog):
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=2),
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=2),
        ]
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer.hydrate_collection_field(
                self._packed(items),
                item_type=AdverseEvent,
                patients=patients,
            )
        assert not any("natural-key conflict" in r.getMessage() for r in caplog.records)
        # identical duplicate collapses to a single instance
        assert len(patients["p1"].adverse_events) == 1

    def test_conflicting_data_logs_warning_and_keeps_last(self, caplog):
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=3),
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=2),
        ]
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer.hydrate_collection_field(
                self._packed(items),
                item_type=AdverseEvent,
                patients=patients,
            )
        warnings = [r for r in caplog.records if "natural-key conflict" in r.getMessage()]
        assert len(warnings) == 1
        assert "p1" in warnings[0].getMessage()
        # warn-and-keep-last: collapses to one instance, the later row wins
        aes = patients["p1"].adverse_events
        assert len(aes) == 1
        assert aes[0].grade == 2

    def test_conflicting_data_raises_under_error_policy(self):
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=3),
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=2),
        ]
        with pytest.raises(ValueError, match="natural-key conflict"):
            BaseHarmonizer.hydrate_collection_field(
                self._packed(items),
                item_type=AdverseEvent,
                patients=patients,
                on_natural_key_conflict="error",
            )

    def test_lone_none_in_nk_does_not_warn(self, caplog):
        """
        A null NK component with no collision is benign (a conditional
        disambiguator), shouldn't warn.
        """
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [self._ae_row(start_date=None, sequence_id=1, grade=2)]
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer.hydrate_collection_field(
                self._packed(items),
                item_type=AdverseEvent,
                patients=patients,
            )
        assert not any("natural-key" in r.getMessage() for r in caplog.records)
        assert len(patients["p1"].adverse_events) == 1

    def test_none_in_nk_collision_flags_weak_identity(self, caplog):
        """
        When a null-containing NK actually collides (distinct records merge),
        the conflict warning flags the null component as weak identity.
        """
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [
            self._ae_row(start_date=None, sequence_id=1, grade=3),
            self._ae_row(start_date=None, sequence_id=1, grade=2),
        ]
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer.hydrate_collection_field(
                self._packed(items),
                item_type=AdverseEvent,
                patients=patients,
            )
        warnings = [r for r in caplog.records if "natural-key conflict" in r.getMessage()]
        assert len(warnings) == 1
        msg = warnings[0].getMessage()
        assert "start_date" in msg
        assert "weak" in msg.lower()
        # over-collapsed to one, last wins
        aes = patients["p1"].adverse_events
        assert len(aes) == 1
        assert aes[0].grade == 2

    def test_fully_populated_nk_conflict_has_no_weak_identity_note(self, caplog):
        """
        A conflict on a fully populated NK is an actual duplicate, not weak
        identity.
        """
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=3),
            self._ae_row(start_date=dt.date(2024, 1, 1), sequence_id=1, grade=2),
        ]
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer.hydrate_collection_field(
                self._packed(items),
                item_type=AdverseEvent,
                patients=patients,
            )
        warnings = [r for r in caplog.records if "natural-key conflict" in r.getMessage()]
        assert len(warnings) == 1
        assert "weak" not in warnings[0].getMessage().lower()

    def test_empty_natural_key_skips_check(self, mock_simple_domain_attr, caplog):
        """Domains without NATURAL_KEY_FIELDS bypass the conflict check entirely."""
        patients = {"p1": Patient(patient_id="p1", trial_id="test")}
        items = [{"name": "a", "value": 1}, {"name": "a", "value": 2}]
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer.hydrate_collection_field(
                pl.DataFrame({"SubjectId": ["p1"], "items": [items]}),
                item_type=SimpleDomain,
                patients=patients,
            )
        assert not any("natural-key conflict" in r.message for r in caplog.records)


class TestEnforceRequired:
    """
    Central materiality drop. Rows missing any REQUIRED_FIELDS member are
    removed, and every dropped subject is logged (subject + null field) so
    upstream data issues stay traceable.

    Uses SimpleDomain (REQUIRED_FIELDS = (name,)).
    """

    def test_drops_rows_with_null_required_field(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p2", "p3"],
                "name": ["alpha", None, "gamma"],
                "value": [1, 2, 3],
            }
        )
        result = BaseHarmonizer._enforce_required(df, item_type=SimpleDomain, spec_name="simple", subject_col="SubjectId")
        assert result["SubjectId"].to_list() == ["p1", "p3"]

    def test_logs_every_dropped_subject_and_field(self, caplog):
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p2", "p4"],
                "name": [None, "beta", None],
                "value": [1, 2, 3],
            }
        )
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            BaseHarmonizer._enforce_required(df, item_type=SimpleDomain, spec_name="simple", subject_col="SubjectId")
        warnings = [r for r in caplog.records if "dropped" in r.getMessage()]
        assert len(warnings) == 1
        msg = warnings[0].getMessage()
        assert "dropped 2 record(s)" in msg
        # every dropped subject is named, with the offending field, the kept row is not
        assert "p1: missing ['name']" in msg
        assert "p4: missing ['name']" in msg
        assert "p2" not in msg

    def test_all_required_present_keeps_all_rows_silently(self, caplog):
        df = pl.DataFrame(
            {
                "SubjectId": ["p1", "p2"],
                "name": ["alpha", "beta"],
                "value": [1, 2],
            }
        )
        with caplog.at_level("WARNING", logger="omop_etl.harmonization.harmonizers.base"):
            result = BaseHarmonizer._enforce_required(df, item_type=SimpleDomain, spec_name="simple", subject_col="SubjectId")
        assert result.height == 2
        assert not any("dropped" in r.getMessage() for r in caplog.records)

    def test_empty_required_is_noop(self):
        class _NoRequired(SimpleDomain):
            REQUIRED_FIELDS = ()

        # every row has a null name, all would drop if name were required
        df = pl.DataFrame({"SubjectId": ["p1", "p2"], "name": [None, None], "value": [1, 2]})
        result = BaseHarmonizer._enforce_required(df, item_type=_NoRequired, spec_name="no_required", subject_col="SubjectId")
        assert result.height == 2


class TestHydrateScalar:
    def test_sets_scalar_attribute(self):
        """Scalar value should be set on patient."""
        harmonizer = MinimalHarmonizer(
            data=pl.DataFrame({"SubjectId": ["p1"]}),
            trial_id="test",
        )
        harmonizer._create_patients()

        df = pl.DataFrame({"SubjectId": ["p1"], "the_value": ["hello"]})

        harmonizer.hydrate_scalar(df, attr="sex", value_col="the_value")

        assert harmonizer.patient_data["p1"].sex == "hello"

    def test_skip_missing_patients(self):
        """skip_missing_patients=True should skip unknown subjects."""
        harmonizer = MinimalHarmonizer(
            data=pl.DataFrame({"SubjectId": ["p1"]}),
            trial_id="test",
        )
        harmonizer._create_patients()

        df = pl.DataFrame({"SubjectId": ["p1", "unknown"], "the_value": ["a", "b"]})

        # should not raise
        harmonizer.hydrate_scalar(df, attr="sex", value_col="the_value", skip_missing_patients=True)
        assert harmonizer.patient_data["p1"].sex == "a"

    def test_duplicate_error(self):
        """on_duplicate='error' should raise on duplicates."""
        harmonizer = MinimalHarmonizer(
            data=pl.DataFrame({"SubjectId": ["p1"]}),
            trial_id="test",
        )
        harmonizer._create_patients()

        df = pl.DataFrame({"SubjectId": ["p1", "p1"], "the_value": ["a", "b"]})

        with pytest.raises(ValueError, match="Duplicate scalar"):
            harmonizer.hydrate_scalar(df, attr="sex", value_col="the_value", on_duplicate="error")

    def test_duplicate_first(self):
        """on_duplicate='first' should keep the first value seen for a subject."""
        harmonizer = MinimalHarmonizer(
            data=pl.DataFrame({"SubjectId": ["p1"]}),
            trial_id="test",
        )
        harmonizer._create_patients()

        df = pl.DataFrame({"SubjectId": ["p1", "p1"], "the_value": ["first_value", "second_value"]})

        harmonizer.hydrate_scalar(df, attr="sex", value_col="the_value", on_duplicate="first")

        assert harmonizer.patient_data["p1"].sex == "first_value"

    def test_duplicate_last(self):
        """on_duplicate='last' should overwrite with the last value seen for a subject."""
        harmonizer = MinimalHarmonizer(
            data=pl.DataFrame({"SubjectId": ["p1"]}),
            trial_id="test",
        )
        harmonizer._create_patients()

        df = pl.DataFrame({"SubjectId": ["p1", "p1"], "the_value": ["first_value", "second_value"]})

        harmonizer.hydrate_scalar(df, attr="sex", value_col="the_value", on_duplicate="last")

        assert harmonizer.patient_data["p1"].sex == "second_value"


def _noop_processor(h) -> None:  # noqa
    return None


class TestValidateSpecs:
    """
    Validation runs in __init_subclass__, misconfigured SPECS raises
    when the `class` statement executes, before any instance is constructed.
    Each test wraps the class definition itself in pytest.raises.
    """

    def test_duplicate_spec_names_raises(self):
        with pytest.raises(ValueError, match="Duplicate processor names"):

            class BadHarmonizer(BaseHarmonizer):  # noqa
                SPECS = (
                    ScalarSpec(name="foo", process=_noop_processor, target_attr="sex", value_col="x"),
                    ScalarSpec(name="foo", process=_noop_processor, target_attr="sex", value_col="y"),
                )

                def _create_patients(self) -> None:
                    pass

    def test_scalar_requires_target_attr(self):
        with pytest.raises(ValueError, match="scalar requires target_attr"):

            class BadHarmonizer(BaseHarmonizer):  # noqa
                SPECS = (ScalarSpec(name="foo", process=_noop_processor, value_col="x"),)

                def _create_patients(self) -> None:
                    pass

    def test_scalar_requires_value_col(self):
        with pytest.raises(ValueError, match="scalar requires value_col"):

            class BadHarmonizer(BaseHarmonizer):  # noqa
                SPECS = (ScalarSpec(name="foo", process=_noop_processor, target_attr="sex"),)

                def _create_patients(self) -> None:
                    pass

    def test_singleton_requires_target_domain(self):
        with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'target_domain'"):
            SingletonSpec(name="foo", process=_noop_processor)  # noqa

    def test_collection_requires_target_domain(self):
        with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'target_domain'"):
            CollectionSpec(name="foo", process=_noop_processor)  # noqa

    def test_empty_subject_col_raises(self):
        with pytest.raises(ValueError, match="subject_col cannot be empty"):

            class BadHarmonizer(BaseHarmonizer):  # noqa
                SPECS = (ScalarSpec(name="foo", process=_noop_processor, target_attr="sex", value_col="x", subject_col=""),)

                def _create_patients(self) -> None:
                    pass

    def test_singleton_decorator_on_collection_domain_raises(self):
        with pytest.raises(ValueError, match=r"@singleton used with .* but Patient maps it to a collection"):

            class BadHarmonizer(BaseHarmonizer):  # noqa
                SPECS = (SingletonSpec(name="foo", process=_noop_processor, target_domain=MedicalHistory),)

                def _create_patients(self) -> None:
                    pass

    def test_collection_decorator_on_singleton_domain_raises(self):
        with pytest.raises(ValueError, match=r"@collection used with .* but Patient maps it to a singleton"):

            class BadHarmonizer(BaseHarmonizer):  # noqa
                SPECS = (CollectionSpec(name="foo", process=_noop_processor, target_domain=FollowUp),)

                def _create_patients(self) -> None:
                    pass


class TestRunTemplateMethod:
    def test_run_calls_processors_in_order(self):
        call_order = []

        class TrackedHarmonizer(BaseHarmonizer):
            def _create_patients(self) -> None:
                call_order.append("create_patients")
                self.patient_data["p1"] = Patient(patient_id="p1", trial_id=self.trial_id)

            @scalar(name="first", target_attr="sex", value_col="v")
            def _process_first(self) -> pl.DataFrame | None:
                call_order.append("first")
                return pl.DataFrame({"SubjectId": ["p1"], "v": ["a"]})

            @scalar(name="second", target_attr="age", value_col="v")
            def _process_second(self) -> pl.DataFrame | None:
                call_order.append("second")
                return pl.DataFrame({"SubjectId": ["p1"], "v": [42]})

        harmonizer = TrackedHarmonizer(data=pl.DataFrame({"SubjectId": ["p1"]}), trial_id="test")
        harmonizer.run()

        assert call_order == ["create_patients", "first", "second"]
        assert harmonizer.patient_data["p1"].sex == "a"
        assert harmonizer.patient_data["p1"].age == 42

    def test_run_with_empty_patient_data_raises(self):
        class EmptyHarmonizer(BaseHarmonizer):
            SPECS = ()

            def _create_patients(self) -> None:
                pass

        harmonizer = EmptyHarmonizer(data=pl.DataFrame({"SubjectId": ["p1"]}), trial_id="test")

        with pytest.raises(RuntimeError, match="patient_data is empty"):
            harmonizer.run()

    def test_processor_returning_none_is_skipped(self):
        class SkipHarmonizer(BaseHarmonizer):
            def _create_patients(self) -> None:
                self.patient_data["p1"] = Patient(patient_id="p1", trial_id=self.trial_id)

            @scalar(name="skip", target_attr="sex", value_col="v")
            def _process_skip(self) -> pl.DataFrame | None:
                return None

        harmonizer = SkipHarmonizer(data=pl.DataFrame({"SubjectId": ["p1"]}), trial_id="test")
        harmonizer.run()  # shouldn't raise

        assert harmonizer.patient_data["p1"].sex is None


@dataclass(frozen=True, slots=True)
class E2ERawRow:
    """
    One row of denormalized raw input for the e2e test harmonizer.
    """

    SubjectId: str
    # scalar source: Patient.sex
    sex: str | None = None
    # singleton source: FollowUp
    raw_lost: bool | None = None
    raw_lost_date: dt.date | None = None
    # collection source: MedicalHistory
    raw_mh_term: str | None = None
    raw_mh_start: dt.date | None = None
    raw_mh_end: dt.date | None = None


def _rows_to_df(rows: list[E2ERawRow]) -> pl.DataFrame:
    """Build a polars DataFrame from a list of E2ERawRow instances."""
    return pl.from_dicts([asdict(r) for r in rows])


class E2EHarmonizer(BaseHarmonizer):
    """
    Small concrete harmonizer covering one type of spec with real domains
    for scalar, singleeton and collection to Patient domain.
    """

    def _create_patients(self) -> None:
        for sid in self.data.select("SubjectId").unique().to_series():
            self.patient_data[sid] = Patient(patient_id=sid, trial_id=self.trial_id)

    @scalar()
    def _process_sex(self) -> pl.DataFrame | None:
        return self.data.select("SubjectId", "sex").filter(pl.col("sex").is_not_null()).unique(subset=["SubjectId"], keep="first")

    @singleton(FollowUp)
    def _process_lost_to_followup(self) -> pl.DataFrame | None:
        return (
            self.data.select("SubjectId", "raw_lost", "raw_lost_date")
            .filter(pl.col("raw_lost").is_not_null())
            .rename(
                {
                    "raw_lost": FollowUp.Fields.LOST_TO_FOLLOWUP,
                    "raw_lost_date": FollowUp.Fields.DATE_LOST_TO_FOLLOWUP,
                }
            )
            .unique(subset=["SubjectId"], keep="first")
        )

    @collection(MedicalHistory, order_by=(MedicalHistory.Fields.START_DATE,), require_order_by=True)
    def _process_medical_histories(self) -> pl.DataFrame | None:
        return (
            self.data.select("SubjectId", "raw_mh_term", "raw_mh_start", "raw_mh_end")
            .filter(pl.col("raw_mh_term").is_not_null())
            .rename(
                {
                    "raw_mh_term": MedicalHistory.Fields.TERM,
                    "raw_mh_start": MedicalHistory.Fields.START_DATE,
                    "raw_mh_end": MedicalHistory.Fields.END_DATE,
                }
            )
        )


class TestEndToEndPipeline:
    """
    Small E2E to verify raw data to run() for scalar, singleton and collection processors to instantiated Patient model.
    """

    def test_full_pipeline_two_patients(self):
        """Complete pipeline with two patients: p1 has 2 MH rows, p2 has 1."""
        rows = [
            E2ERawRow("p1", sex="female", raw_lost=False),
            E2ERawRow("p1", raw_mh_term="hypertension", raw_mh_start=dt.date(2020, 1, 15), raw_mh_end=dt.date(2021, 6, 1)),
            E2ERawRow("p1", raw_mh_term="diabetes", raw_mh_start=dt.date(2022, 3, 10)),
            E2ERawRow("p2", sex="male", raw_lost=True, raw_lost_date=dt.date(2024, 5, 1)),
            E2ERawRow("p2", raw_mh_term="asthma", raw_mh_start=dt.date(2019, 8, 20)),
        ]
        harmonizer = E2EHarmonizer(data=_rows_to_df(rows), trial_id="TEST001")
        harmonizer.run()

        assert set(harmonizer.patient_data.keys()) == {"p1", "p2"}
        p1 = harmonizer.patient_data["p1"]
        p2 = harmonizer.patient_data["p2"]

        # scalar
        assert p1.sex == "female"
        assert p2.sex == "male"

        # singleton (FollowUp)
        assert p1.lost_to_followup is not None
        lfu = p1.lost_to_followup
        assert lfu is not None
        assert lfu.lost_to_followup is False
        assert lfu.date_lost_to_followup is None

        assert p2.lost_to_followup is not None
        lfu_2 = p2.lost_to_followup
        assert lfu_2 is not None
        assert lfu_2.lost_to_followup is True
        assert lfu_2.date_lost_to_followup == dt.date(2024, 5, 1)

        # collection (MedicalHistory), ordered by natural key (term, start_date);
        # "diabetes" sorts before "hypertension"
        assert len(p1.medical_histories) == 2
        assert [mh.term for mh in p1.medical_histories] == ["diabetes", "hypertension"]
        assert [mh.start_date for mh in p1.medical_histories] == [dt.date(2022, 3, 10), dt.date(2020, 1, 15)]
        assert p1.medical_histories[0].end_date is None
        assert p1.medical_histories[1].end_date == dt.date(2021, 6, 1)

        assert len(p2.medical_histories) == 1
        assert p2.medical_histories[0].term == "asthma"

    def test_patient_with_missing_domains(self):
        """p1 has everything; p2 has only cohort (no FollowUp, no MedicalHistory)."""
        rows = [
            E2ERawRow("p1", sex="female", raw_lost=True),
            E2ERawRow("p1", raw_mh_term="hypertension", raw_mh_start=dt.date(2020, 1, 15)),
            E2ERawRow("p2", sex="male"),
        ]
        harmonizer = E2EHarmonizer(data=_rows_to_df(rows), trial_id="TEST001")
        harmonizer.run()

        p1 = harmonizer.patient_data["p1"]
        p2 = harmonizer.patient_data["p2"]

        # p1 has everything
        assert p1.sex == "female"
        assert p1.lost_to_followup is not None
        lfu = p1.lost_to_followup
        assert lfu is not None
        assert lfu.lost_to_followup is True
        assert len(p1.medical_histories) == 1

        # p2 only has cohort
        assert p2.sex == "male"
        assert p2.lost_to_followup is None
        assert len(p2.medical_histories) == 0

    def test_collection_ordering(self):
        """Medical histories should be sorted by start_date regardless of row insertion order."""
        rows = [
            E2ERawRow("p1", sex="female"),
            # intentionally out of order to verify sort
            E2ERawRow("p1", raw_mh_term="third", raw_mh_start=dt.date(2024, 3, 1)),
            E2ERawRow("p1", raw_mh_term="first", raw_mh_start=dt.date(2024, 1, 1)),
            E2ERawRow("p1", raw_mh_term="second", raw_mh_start=dt.date(2024, 2, 1)),
        ]
        harmonizer = E2EHarmonizer(data=_rows_to_df(rows), trial_id="TEST001")
        harmonizer.run()

        p1 = harmonizer.patient_data["p1"]

        assert len(p1.medical_histories) == 3
        assert [mh.term for mh in p1.medical_histories] == ["first", "second", "third"]
        assert [mh.start_date for mh in p1.medical_histories] == [
            dt.date(2024, 1, 1),
            dt.date(2024, 2, 1),
            dt.date(2024, 3, 1),
        ]


class TestDecoratorRegistration:
    """
    Tests for @scalar, @singleton and @collection decorators and the
    __init_subclass__ method that creates SPECS.
    """

    def test_scalar_default_name_from_method(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @scalar()
            def _process_sex(self) -> pl.DataFrame | None:
                return None

        assert len(H.SPECS) == 1
        spec = H.SPECS[0]
        assert isinstance(spec, ScalarSpec)
        assert spec.name == "sex"
        assert spec.target_attr == "sex"
        assert spec.value_col == "sex"
        assert spec.on_duplicate == "error"

    def test_scalar_explicit_overrides_win(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @scalar(name="custom_name", target_attr="sex", value_col="raw_col", on_duplicate="last")
            def _process_anything(self) -> pl.DataFrame | None:
                return None

        spec = H.SPECS[0]
        assert isinstance(spec, ScalarSpec)
        assert spec.name == "custom_name"
        assert spec.target_attr == "sex"
        assert spec.value_col == "raw_col"
        assert spec.on_duplicate == "last"

    def test_scalar_partial_override_keeps_other_defaults(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @scalar(target_attr="sex")
            def _process_patient_sex(self) -> pl.DataFrame | None:
                return None

        spec = H.SPECS[0]
        assert isinstance(spec, ScalarSpec)
        assert spec.name == "patient_sex"  # default from method name
        assert spec.target_attr == "sex"  # explicit
        assert spec.value_col == "patient_sex"  # default from method name

    def test_specs_collected_in_declaration_order(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @scalar()
            def _process_sex(self) -> pl.DataFrame | None:
                return None

            @scalar()
            def _process_date_of_death(self) -> pl.DataFrame | None:
                return None

            @scalar()
            def _process_age(self) -> pl.DataFrame | None:
                return None

        assert [s.name for s in H.SPECS] == ["sex", "date_of_death", "age"]

    def test_undecorated_methods_are_ignored_from_specs(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            def _helper(self) -> str:  # noqa
                return "not a spec"

            @scalar()
            def _process_sex(self) -> pl.DataFrame | None:
                return None

            def _another_helper(self) -> None:  # noqa
                return None

        assert len(H.SPECS) == 1
        assert H.SPECS[0].name == "sex"

    def test_subclass_with_no_decorated_methods_has_empty_specs(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

        assert H.SPECS == ()

    def test_validation_fires_at_class_definition_time_via_decorator(self):
        with pytest.raises(ValueError, match="Patient has no attribute"):

            class H(BaseHarmonizer):  # noqa
                def _create_patients(self) -> None:
                    pass

                @scalar(target_attr="not_a_real_patient_attribute")
                def _process_foo(self) -> pl.DataFrame | None:
                    return None

    def test_decorated_method_remains_callable(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                self.patient_data["p1"] = Patient(patient_id="p1", trial_id=self.trial_id)

            @scalar()
            def _process_sex(self) -> pl.DataFrame | None:
                return pl.DataFrame({"SubjectId": ["p1"], "sex": ["male"]})

        h = H(data=pl.DataFrame({"SubjectId": ["p1"]}), trial_id="test")
        # direct method call still works, the decorator returned the function unchanged
        result = h._process_sex()
        assert result is not None
        assert result["sex"].to_list() == ["male"]

    def test_singleton_decorator_attaches_spec(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @singleton(FollowUp)
            def _process_lost_to_followup(self) -> pl.DataFrame | None:
                return None

        assert len(H.SPECS) == 1
        spec = H.SPECS[0]
        assert isinstance(spec, SingletonSpec)
        assert spec.name == "lost_to_followup"
        assert spec.target_domain is FollowUp

    def test_collection_decorator_attaches_spec(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @collection(MedicalHistory, order_by=("start_date",), require_order_by=True)
            def _process_medical_histories(self) -> pl.DataFrame | None:
                return None

        assert len(H.SPECS) == 1
        spec = H.SPECS[0]
        assert isinstance(spec, CollectionSpec)
        assert spec.name == "medical_histories"
        assert spec.target_domain is MedicalHistory
        assert spec.order_by == ("start_date",)
        assert spec.require_order_by is True

    def test_mixed_spec_types_collected_in_declaration_order(self):
        class H(BaseHarmonizer):
            def _create_patients(self) -> None:
                pass

            @scalar()
            def _process_sex(self) -> pl.DataFrame | None:
                return None

            @singleton(FollowUp)
            def _process_lost_to_followup(self) -> pl.DataFrame | None:
                return None

            @scalar()
            def _process_age(self) -> pl.DataFrame | None:
                return None

        assert [s.name for s in H.SPECS] == ["sex", "lost_to_followup", "age"]
        assert isinstance(H.SPECS[0], ScalarSpec)
        assert isinstance(H.SPECS[1], SingletonSpec)
        assert isinstance(H.SPECS[2], ScalarSpec)
