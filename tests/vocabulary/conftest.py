import pytest
from dataclasses import dataclass, asdict
from pathlib import Path
import polars as pl

from omop_etl.vocabulary.vocabulary import CONCEPT_COLUMNS


@dataclass(frozen=True, slots=True)
class ConceptRow:
    """
    One OMOP CONCEPT row (mimicking Athena export) for vocabulary tests. Only concept_id
    is the subject, the rest default to a plausible standard/valid SNOMED conditions.
    """

    concept_id: int
    concept_name: str = "Malignant melanoma"
    domain_id: str = "Condition"
    vocabulary_id: str = "SNOMED"
    concept_class_id: str = "Clinical Finding"
    standard_concept: str = "S"
    concept_code: str = "93655004"
    valid_start_date: str = "20020131"
    valid_end_date: str = "20991231"
    invalid_reason: str = ""

    def as_row(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


def write_concept_tsv(path: Path, *rows: ConceptRow) -> Path:
    """Write ConceptRows to a tab-delimited OMOP CONCEPT file, as Vocabulary.from_csv reads."""
    lines = ["\t".join(CONCEPT_COLUMNS)]
    lines += ["\t".join(r.as_row()[column] for column in CONCEPT_COLUMNS) for r in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.fixture
def static_mapping_data() -> pl.DataFrame:
    return pl.DataFrame(
        data={
            "value_set": ["sex", "sex"],
            "source_value": ["M", "F"],
            "concept_id": [8507, 8532],
        }
    )


@pytest.fixture
def static_mapping_file(tmp_path, static_mapping_data) -> Path:
    path = Path(tmp_path / "static_mapping.csv").resolve()
    static_mapping_data.write_csv(path)
    return path


@pytest.fixture
def structural_mapping_data() -> pl.DataFrame:
    return pl.DataFrame(
        data={
            "value_set": ["ecrf"],
            "concept_id": [32817],
        }
    )


@pytest.fixture
def structural_mapping_file(tmp_path, structural_mapping_data) -> Path:
    path = Path(tmp_path / "structural_mapping.csv").resolve()
    structural_mapping_data.write_csv(path)
    return path
