from typing import List
from logging import getLogger

log = getLogger(__name__)


def detect_mutually_exclusive_collision(row: dict, field_names: List[str], patient_id: str = None, context: str = None) -> tuple[bool, List[str]]:
    """
    Detect collisions in mutually exclusive fields for a single subject.

    Args:
        row: Dictionary containing the subject's data
        field_names: List of field names that should be mutually exclusive
        patient_id: Optional patient ID for logging
        context: Optional context description for logging (e.g., "study_drugs", "biomarkers")

    Returns:
        (has_collision, list_of_fields_with_values)
    """
    fields_with_values = []

    for field in field_names:
        if field in row:
            value = row[field]
            if value is not None and str(value).strip() not in [
                "",
                "NA",
                "N/A",
                "null",
            ]:  # todo: use constant instead
                fields_with_values.append(field)

    has_collision = len(fields_with_values) > 1

    if has_collision and patient_id and context:
        log.info(f"Mutually exclusive field collision detected - Patient: {patient_id}, Context: {context}, Conflicting fields: {fields_with_values}")

    return has_collision, fields_with_values


def detect_paired_field_collisions(
    row: dict,
    field_pairs: list[tuple[str, str]],
    patient_id: str = None,
    context: str = None,
) -> tuple[bool, list[tuple[str, str]]]:
    """
    Detect collisions in paired mutually exclusive fields (e.g., drug name + code pairs).

    Args:
        row: Dictionary containing the subject's data
        field_pairs: List of tuples, each containing (main_field, code_field)
        patient_id: Optional patient ID for logging
        context: Optional context description for logging

    Returns:
        (has_collision, list_of_pairs_with_values)
    """
    pairs_with_values = []

    for main_field, code_field in field_pairs:
        main_value = row.get(main_field)
        code_value = row.get(code_field)

        # todo: use constant instead
        if (main_value is not None and str(main_value).strip() not in ["", "NA", "N/A", "null"]) or (
            code_value is not None and str(code_value).strip() not in ["", "NA", "N/A", "null"]
        ):
            pairs_with_values.append((main_field, code_field))

    has_collision = len(pairs_with_values) > 1

    if has_collision and patient_id and context:
        log.info(
            f"Mutually exclusive paired field collision detected - Patient: {patient_id}, Context: {context}, Conflicting pairs: {pairs_with_values}",
        )

    return has_collision, pairs_with_values
