from typing import List

from omop_etl.semantic_mapping.models import Query
from omop_etl.semantic_mapping.query_extractor import extract_queries


def test_query_extractor(patients, configs):
    queries: List[Query] = []
    for patient in patients:
        queries.extend(extract_queries(patient, configs))

    assert len(queries) == 4, (
        "Each instance of leaf classes (here TumorType and MedicalHistory, one instance per patient) creates one query"
    )

    # assert TumorType singleton produces correct query
    patient_1_tumor_query = queries[0]
    assert patient_1_tumor_query.query == "tumor_1"
    assert patient_1_tumor_query.patient_id == "1"
    assert patient_1_tumor_query.field_path == ("tumor_type", "main_tumor_type")
    assert patient_1_tumor_query.id == "fbf4cbe6fb9df353"

    # assert MedicalHistories collection produces correct query
    patient_2_medical_history_query = queries[3]
    assert patient_2_medical_history_query.query == "medical_history_2"
    assert patient_2_medical_history_query.patient_id == "2"
    assert patient_2_medical_history_query.field_path == ("medical_histories", "term")
    assert patient_2_medical_history_query.id == "4734158dcbb58615"
