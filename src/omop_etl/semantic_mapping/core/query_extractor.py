import hashlib
from typing import List, Sequence, Any

from omop_etl.harmonization.models import Patient
from omop_etl.semantic_mapping.core.models import (
    Query,
    FieldConfig,
)


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


def _get_nested_attr(obj: object, path: Sequence[str]) -> Any:
    """
    Walk chain of attrs on single object,
    e.g.: _get_nested_attr(patient.tumor_type, ["main_tumor_type"])
    """
    current = obj
    for name in path:
        if current is None:
            return None
        if not hasattr(current, name):
            raise AttributeError(f"{current!r} has no attribute {name!r}")
        current = getattr(current, name)
    return current


def validate_field_paths(
    patients: Sequence[Patient],
    configs: Sequence[FieldConfig],
) -> None:
    errors: list[str] = []

    for cfg in configs:
        head, *tail = cfg.field_path

        if not hasattr(patients[0], head):
            errors.append(f"{cfg.name}: Patient has no attribute: {head}")
            continue

        sample_obj = None
        for patient in patients:
            attr = getattr(patient, head, None)
            if attr is None:
                continue

            if isinstance(attr, (list, tuple, set)):
                try:
                    sample_obj = next(iter(attr))
                except StopIteration:
                    continue
            else:
                sample_obj = attr
            if sample_obj is not None:
                break

        # No sample object with data: can’t validate tail,
        # but head is structurally ok
        if sample_obj is None:
            continue

        try:
            _ = _get_nested_attr(sample_obj, tail) if tail else sample_obj
        except AttributeError:
            bad_leaf = tail[-1] if tail else "<empty>"
            errors.append(
                f"{cfg.name}: invalid field_path {cfg.field_path!r} – '{bad_leaf}' is not an attribute of {type(sample_obj).__name__}"
            )

        if errors:
            raise ValueError("Invalid FieldConfig field_paths:\n" + "\n".join(f"- {e}" for e in errors))
