from dataclasses import dataclass, astuple


@dataclass(frozen=True, slots=True)
class MappedConcept:
    concept_id: int | str
    concept_code: str
    concept_name: str
    domain_id: str
    vocabulary_id: str
    standard_flag: str


@dataclass(frozen=True, slots=True)
class StaticConcept:
    value_set: str
    local_value: str
    concept_id: str
    concept_code: str
    concept_name: str
    concept_class: str
    concept_category: str
    valid_flag: str
    domain_id: str
    vocabulary_id: str

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "StaticConcept":
        return cls(
            value_set=row["value_set"],
            local_value=row["local_value"],
            concept_id=row["omop_concept_id"],
            concept_code=row["omop_concept_code"],
            concept_name=row["omop_concept_name"],
            concept_class=row["omop_class"],
            concept_category=row["omop_concept_category"],
            valid_flag=row["omop_valid_flag"],
            domain_id=row["omop_domain"],
            vocabulary_id=row["omop_vocab"],
        )


@dataclass(frozen=True, slots=True)
class StructuralConcept:
    value_set: str
    concept_id: str
    concept_code: str
    concept_name: str
    domain_id: str
    vocabulary_id: str
    valid_flag: str
    concept_class: str
    concept_category: str
    table_name: str | None = None

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "StructuralConcept":
        return cls(
            value_set=row["value_set"],
            concept_id=row["omop_concept_id"],
            concept_code=row["omop_concept_code"],
            concept_name=row["omop_concept_name"],
            concept_class=row["omop_class"],
            concept_category=row["omop_concept_category"],
            valid_flag=row["omop_valid_flag"],
            domain_id=row["omop_domain"],
            vocabulary_id=row["omop_vocab"],
        )

    def __iter__(self):
        return iter(astuple(self))
