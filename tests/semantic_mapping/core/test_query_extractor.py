from typing import List

from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.semantic_mapping.core.models import Query
from omop_etl.semantic_mapping.core.query_extractor import extract_queries


def test_query_extractor(patients, configs):
    queries: List[Query] = []
    for patient in patients:
        queries.extend(extract_queries(patient, configs))

    assert len(queries) == 4, "Each instance of leaf classes (here TumorType and MedicalHistory, one instance per patient) creates one query"

    # query ids correlate each query to its result within a run, so they must be distinct
    assert len({q.id for q in queries}) == len(queries)

    # assert TumorType singleton produces correct query
    patient_1_tumor_query = queries[0]
    assert patient_1_tumor_query.query == "tumor_1"
    assert patient_1_tumor_query.patient_id == "1"
    assert patient_1_tumor_query.field_path == (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE)

    # assert MedicalHistories collection produces correct query
    patient_2_medical_history_query = queries[3]
    assert patient_2_medical_history_query.query == "medical_history_2"
    assert patient_2_medical_history_query.patient_id == "2"
    assert patient_2_medical_history_query.field_path == (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM)
