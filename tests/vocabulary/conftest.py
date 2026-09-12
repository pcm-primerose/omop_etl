import pytest
from dataclasses import dataclass, asdict, fields
from pathlib import Path
import polars as pl

from omop_etl.vocabulary.core.helpers import ATHENA_CONCEPT_COLUMNS, REQUIRED_ATHENA_FILES


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


@dataclass(frozen=True, slots=True)
class VocabularyRow:
    vocabulary_id: str | None = None
    vocabulary_name: str = "OMOP Standardized Vocabularies"
    vocabulary_reference: str = "OMOP generated"
    vocabulary_version: str = "v5.0 01-JAN-26"
    vocabulary_concept_id: str = "44819096"

    def as_row(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


_VOCABULARY_COLUMNS = tuple(f.name for f in fields(VocabularyRow))


def write_concept_csv(athena_dir: Path, *rows: ConceptRow) -> Path:
    """
    Write Athena's CONCEPT.csv into `athena_dir` and return its path.
    """
    concept_path = athena_dir / "CONCEPT.csv"
    lines = ["\t".join(ATHENA_CONCEPT_COLUMNS)]
    lines += ["\t".join(r.as_row()[column] for column in ATHENA_CONCEPT_COLUMNS) for r in rows]
    concept_path.write_text("\n".join(lines) + "\n")
    return concept_path


def write_vocabulary_csv(athena_dir: Path, row: VocabularyRow = VocabularyRow()) -> Path:
    """Write Athena's VOCABULARY.csv into `athena_dir` and return its path."""
    vocabulary_path = athena_dir / "VOCABULARY.csv"
    values = row.as_row()
    lines = ["\t".join(_VOCABULARY_COLUMNS), "\t".join(values[column] for column in _VOCABULARY_COLUMNS)]
    vocabulary_path.write_text("\n".join(lines) + "\n")
    return vocabulary_path


@dataclass(frozen=True, slots=True)
class ConceptAncestorRow:
    ancestor_concept_id: int
    descendant_concept_id: int
    min_levels_of_separation: int = 1
    max_levels_of_separation: int = 1

    def as_row(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}


_CONCEPT_ANCESTOR_COLUMNS = tuple(f.name for f in fields(ConceptAncestorRow))


def write_concept_ancestor_csv(athena_dir: Path, *rows: ConceptAncestorRow) -> Path:
    """Write Athena's CONCEPT_ANCESTOR.csv into `athena_dir` and return its path."""
    path = athena_dir / "CONCEPT_ANCESTOR.csv"
    lines = ["\t".join(_CONCEPT_ANCESTOR_COLUMNS)]
    lines += ["\t".join(r.as_row()[column] for column in _CONCEPT_ANCESTOR_COLUMNS) for r in rows]
    path.write_text("\n".join(lines) + "\n")
    return path


def write_athena_bundle(athena_dir: Path, *, skip: tuple[str, ...] = ()) -> Path:
    """
    Write a minimal, well-formed placeholder for every file in `REQUIRED_ATHENA_FILES`
    into `athena_dir`, for bundle-presence/readability tests.
    """
    if "CONCEPT.csv" not in skip:
        write_concept_csv(athena_dir, ConceptRow(4112853))
    if "VOCABULARY.csv" not in skip and not (athena_dir / "VOCABULARY.csv").exists():
        write_vocabulary_csv(athena_dir)
    for filename in REQUIRED_ATHENA_FILES:
        if filename in ("CONCEPT.csv", "VOCABULARY.csv") or filename in skip:
            continue
        (athena_dir / filename).write_text("placeholder_column\n")
    return athena_dir


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
