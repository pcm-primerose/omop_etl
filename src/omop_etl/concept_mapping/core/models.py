from dataclasses import dataclass, astuple, field
from typing import Literal, Dict, List


LookupType = Literal["static", "structural", "semantic"]


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
    def from_csv_row(cls, row: dict[str, str]) -> StaticConcept:
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
    def from_csv_row(cls, row: dict[str, str]) -> StructuralConcept:
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


@dataclass(frozen=True, slots=True)
class MissedLookup:
    """A lookup that failed to find a mapping."""

    lookup_type: LookupType
    value_set: str
    local_value: str


@dataclass(frozen=True, slots=True)
class FieldCoverage:
    """Coverage statistics for a single field (value_set)."""

    value_set: str
    lookup_type: LookupType
    matched: int
    missed: int
    total: int
    coverage_fraction: float


@dataclass
class LookupResult:
    """Aggregated results from concept lookups during a run."""

    matched: Dict[LookupType, List[tuple[str, str, MappedConcept]]] = field(default_factory=lambda: {"static": [], "structural": [], "semantic": []})
    missed: Dict[LookupType, List[MissedLookup]] = field(default_factory=lambda: {"static": [], "structural": [], "semantic": []})

    def record_match(
        self,
        lookup_type: LookupType,
        value_set: str,
        local_value: str,
        concept: MappedConcept,
    ) -> None:
        self.matched[lookup_type].append((value_set, local_value, concept))

    def record_miss(
        self,
        lookup_type: LookupType,
        value_set: str,
        local_value: str,
    ) -> None:
        self.missed[lookup_type].append(MissedLookup(lookup_type=lookup_type, value_set=value_set, local_value=local_value))

    def coverage_by_field(self, lookup_type: LookupType) -> Dict[str, FieldCoverage]:
        """Compute coverage statistics per value_set for a lookup type."""
        from collections import defaultdict

        counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"matched": 0, "missed": 0})

        for value_set, _, _ in self.matched[lookup_type]:
            counts[value_set]["matched"] += 1

        for miss in self.missed[lookup_type]:
            counts[miss.value_set]["missed"] += 1

        result: Dict[str, FieldCoverage] = {}
        for vs, c in counts.items():
            total = c["matched"] + c["missed"]
            frac = round(c["matched"] / total, 5) if total > 0 else 0.0
            result[vs] = FieldCoverage(
                value_set=vs,
                lookup_type=lookup_type,
                matched=c["matched"],
                missed=c["missed"],
                total=total,
                coverage_fraction=frac,
            )

        return result

    def all_coverage(self) -> Dict[str, FieldCoverage]:
        """Compute coverage for all lookup types combined."""
        result: Dict[str, FieldCoverage] = {}
        lt: LookupType
        for lt in ("static", "structural"):
            result.update(self.coverage_by_field(lt))
        return result

    def missed_list(self, lookup_type: LookupType | None = None) -> List[MissedLookup]:
        """Get list of missed lookups, optionally filtered by type."""
        if lookup_type:
            return list(self.missed[lookup_type])
        all_missed = []
        for misses in self.missed.values():
            all_missed.extend(misses)
        return all_missed
