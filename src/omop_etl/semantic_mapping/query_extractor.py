import hashlib
from typing import List, Sequence, Any

from omop_etl.harmonization.datamodels import Patient
from omop_etl.semantic_mapping.models import Query, FieldConfig


def extract_queries(patient: Patient, configs: List[FieldConfig]) -> List[Query]:
    queries: List[Query] = []

    for cfg in configs:
        head, *tail = cfg.field_path
        attr = getattr(patient, head, None)
        if attr is None:
            continue

        # collections
        if isinstance(attr, (list, tuple, set)):
            for idx, item in enumerate(attr):
                val = _get_nested_attr(item, tail) if tail else item
                if isinstance(val, str) and val:
                    query_id = _make_query_id(
                        patient_id=patient.patient_id,
                        field_path=cfg.field_path,
                        leaf_index=idx,
                        raw_value=val,
                    )
                    queries.append(
                        Query(
                            id=query_id,
                            patient_id=patient.patient_id,
                            query=val.lower().strip(),
                            field_path=cfg.field_path,
                            leaf_index=idx,
                            target=cfg.target,
                            raw_value=val,
                        )
                    )
            continue

        # singletons & scalars
        val = _get_nested_attr(attr, tail) if tail else attr
        if isinstance(val, str) and val:
            query_id = _make_query_id(
                patient_id=patient.patient_id,
                field_path=cfg.field_path,
                leaf_index=None,
                raw_value=val,
            )
            queries.append(
                Query(
                    id=query_id,
                    patient_id=patient.patient_id,
                    query=val.lower().strip(),
                    field_path=cfg.field_path,
                    leaf_index=None,
                    target=cfg.target,
                    raw_value=val,
                )
            )

    return queries


def _get_nested_attr(obj: object, path: Sequence[str]) -> Any:
    """
    Walk chain of attrs on single object,
    e.g.: _get_nested_attr(patient.tumor_type, ["main_tumor_type"])
    """
    current = obj
    for name in path:
        if current is None:
            return None
        current = getattr(current, name, None)
    return current


def _make_query_id(
    patient_id: str,
    field_path: tuple[str, ...],
    leaf_index: int | None,
    raw_value: str,
) -> str:
    key = "|".join(
        [
            patient_id,
            ".".join(field_path),
            "" if leaf_index is None else str(leaf_index),
            raw_value.strip().lower(),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
