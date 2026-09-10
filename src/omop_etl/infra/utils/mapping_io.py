from dataclasses import dataclass
from pathlib import Path
import polars as pl

# concept columns from mapping files, same colnames as Athena, except for validity
# which is derived (valid/invalid isntead of the raw invalid_reason)
MAPPING_CONCEPT_COLUMNS = (
    "concept_id",
    "concept_code",
    "concept_name",
    "concept_class_id",
    "standard_concept",
    "validity",
    "domain_id",
    "vocabulary_id",
)

# the three canonical mapping files' fixed names inside a mapping dir (see
# `resolve_mapping_paths`), kept here since it's cross-module
STATIC_MAPPING_FILENAME = "static.csv"
STRUCTURAL_MAPPING_FILENAME = "structural.csv"
SEMANTIC_MAPPING_FILENAME = "semantic.csv"


@dataclass(frozen=True, slots=True)
class MappingPaths:
    static: Path
    structural: Path
    semantic: Path

    def as_list(self) -> list[Path]:
        return [self.static, self.structural, self.semantic]


def resolve_mapping_paths(mapping_dir: Path) -> MappingPaths:
    """The three canonical mapping files in `mapping_dir`."""
    return MappingPaths(
        static=mapping_dir / STATIC_MAPPING_FILENAME,
        structural=mapping_dir / STRUCTURAL_MAPPING_FILENAME,
        semantic=mapping_dir / SEMANTIC_MAPPING_FILENAME,
    )


def read_mapping_csv(source) -> pl.DataFrame:
    """
    Read a mapping CSV file (static/structural/semantic).
    """
    return pl.read_csv(source, comment_prefix="#", infer_schema_length=0)
