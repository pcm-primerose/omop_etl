from typing import List
import pytest
from pathlib import Path
import polars as pl
import datetime as dt

from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.semantic_mapping.core.models import (
    Query,
    OmopDomain,
    QueryTarget,
    FieldConfig,
)


@pytest.fixture
def semantic_data() -> pl.DataFrame:
    df = pl.DataFrame(
        data={
            "term_id": ["a", "b", "c"],
            "source_col": ["x", "y", "z"],
            "source_term": ["something", "AML", "gsv sleeper service"],
            "frequency": [1, 2, 3],
            "omop_concept_id": [10, 20, 30],
            "omop_concept_code": [100, 200, 300],
            "omop_name": ["something else", "Acute Myeloid Leukemia", "systems vehicle"],
            "omop_class": ["abc", "cde", "efg"],
            "omop_concept": ["S", "S", "S"],
            "omop_validity": ["Valid", "Valid", "Valid"],
            "omop_domain": ["condition", "CONDITION", "Condition"],
            "omop_vocab": ["a", "b", "c"],
        }
    )
    return df


@pytest.fixture
def semantic_file(tmp_path, semantic_data) -> Path:
    path = Path(tmp_path / "semantic_test.csv").resolve()
    semantic_data.write_csv(path)
    return path


@pytest.fixture
def queries() -> List[Query]:
    queries: List[Query] = [
        Query(
            patient_id="A",
            id="abc",
            query="aml",
            field_path=("tumor_type", "main_tumor_type"),
            raw_value="AML",
            leaf_index=None,
            target=QueryTarget(domains=[OmopDomain.CONDITION]),
        ),
        Query(
            patient_id="B",
            id="def",
            query="missing in semantic data",
            field_path=("tumor_type", "main_tumor_type"),
            raw_value="missing in semantic data",
            leaf_index=None,
            target=QueryTarget(domains=[OmopDomain.CONDITION]),
        ),
    ]
    return queries


@pytest.fixture
def patients() -> List[Patient]:
    patient_1 = Patient(
        patient_id="1",
        trial_id="test",
    )
    patient_2 = Patient(
        patient_id="2",
        trial_id="test",
    )

    tumor_1 = TumorType(patient_id="1")
    tumor_2 = TumorType(patient_id="2")
    medical_history_1 = [MedicalHistory(patient_id="1")]
    medical_history_2 = [MedicalHistory(patient_id="2")]

    # scalars
    patient_1.age = 1
    patient_1.evaluable_for_efficacy_analysis = False
    patient_1.treatment_start_date = dt.date(1999, 1, 1)
    patient_1.sex = "M"
    patient_2.age = 100
    patient_2.evaluable_for_efficacy_analysis = True
    patient_2.treatment_start_date = dt.date(2999, 2, 2)
    patient_2.sex = "F"

    # singleton
    patient_1.tumor_type = tumor_1
    patient_1.tumor_type.main_tumor_type = "tumor_1"
    patient_2.tumor_type = tumor_2
    patient_2.tumor_type.main_tumor_type = "tumor_2"

    # collection
    patient_1.medical_histories = medical_history_1
    for mh in patient_1.medical_histories:
        mh.term = "medical_history_1"
    patient_2.medical_histories = medical_history_2
    for mh in patient_2.medical_histories:
        mh.term = "medical_history_2"

    return [patient_1, patient_2]


@pytest.fixture
def configs() -> List[FieldConfig]:
    tumor_config = FieldConfig(
        name="tumor type main", field_path=("tumor_type", "main_tumor_type"), target=QueryTarget([OmopDomain.CONDITION])
    )
    medical_history_config = FieldConfig(
        name="medical histories", field_path=("medical_histories", "term"), target=QueryTarget([OmopDomain.CONDITION, OmopDomain.PROCEDURE])
    )

    return [tumor_config, medical_history_config]
