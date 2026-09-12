import datetime as dt
from dataclasses import dataclass
import polars as pl

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.ingredient_rollup import ingredient_exposures
from omop_etl.omop.builders.intervals import collapse_intervals
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import DoseEraRow, DrugExposureRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.vocabulary import Vocabulary

_DOSED_EXPOSURES_SCHEMA = {
    "person_id": pl.Int64,
    "ingredient_concept_id": pl.Int64,
    "unit_concept_id": pl.Int64,
    "dose_value": pl.Float64,
    "start": pl.Date,
    "end": pl.Date,
}


@dataclass(frozen=True, slots=True)
class DerivedDose:
    dose_value: float
    unit_concept_id: int


def derive_dose(exposure: DrugExposureRow, concepts: ConceptLookupService) -> DerivedDose | None:
    """
    A drug_exposure row's daily dose, or None if it can't be derived.

    Requires `quantity`, `days_supply > 0` and a resolvable `dose_unit_source_value`
    (against the curated "unit" value_set).
    Any row that fails these checks are excluded from dose_era,
    only rows in drug_exposure with per-day dosage wil emit (rn just TreatmentCycle).
    """
    if exposure.quantity is None or exposure.days_supply is None or exposure.days_supply <= 0:
        return None
    if not exposure.dose_unit_source_value:
        return None

    unit_matches = concepts.resolve("unit", exposure.dose_unit_source_value, domains={"Unit"})
    if not unit_matches:
        return None

    return DerivedDose(
        dose_value=exposure.quantity / exposure.days_supply,
        unit_concept_id=unit_matches[0].concept_id,
    )


class DoseEraBuilder:
    """
    Derives dose_era rows from already-built drug_exposure rows:
    one era per (person_id, ingredient_concept_id, unit_concept_id, dose_value),
    collapsing dosage records separated by up to a 30-day gap, same persistence window as
    condition_era/drug_era (CDM spec: eras end when there are 31+ days between
    dosage records). Reuses drug_era's ingredient rollup (`ingredient_rollup`).

    Only exposures with a confidently-derived dose (see `derive_dose`) contribute,
    typically a small minority of drug_exposure overall, since most drug sources
    don't report structured per-day dosage.
    """

    PERSISTENCE_DAYS = 30

    def build(
        self,
        drug_exposure: list[DrugExposureRow],
        concept_ancestor: pl.DataFrame,
        vocabulary: Vocabulary,
        concepts: ConceptLookupService,
    ) -> list[DoseEraRow]:
        rollup = ingredient_exposures(drug_exposure, concept_ancestor, vocabulary)
        if rollup.is_empty():
            return []

        exposures_by_id = {row.drug_exposure_id: row for row in drug_exposure}
        dosed = self._dosed_exposures(rollup, exposures_by_id, concepts)
        if dosed.is_empty():
            return []

        eras = collapse_intervals(
            dosed,
            group_by=["person_id", "ingredient_concept_id", "unit_concept_id", "dose_value"],
            start_col="start",
            end_col="end",
            persistence_days=self.PERSISTENCE_DAYS,
        )

        return [
            DoseEraRow(
                dose_era_id=row_id(
                    OmopTables.DOSE_ERA,
                    era["person_id"],
                    era["ingredient_concept_id"],
                    era["unit_concept_id"],
                    era["dose_value"],
                    era["interval_start"],
                ),
                person_id=era["person_id"],
                drug_concept_id=era["ingredient_concept_id"],
                unit_concept_id=era["unit_concept_id"],
                dose_value=era["dose_value"],
                dose_era_start_date=era["interval_start"],
                dose_era_end_date=era["interval_end"],
            )
            for era in eras.iter_rows(named=True)
        ]

    @staticmethod
    def _dosed_exposures(
        rollup: pl.DataFrame,
        exposures_by_id: dict[int, DrugExposureRow],
        concepts: ConceptLookupService,
    ) -> pl.DataFrame:
        person_ids: list[int] = []
        ingredient_ids: list[int] = []
        unit_ids: list[int] = []
        dose_values: list[float] = []
        starts: list[dt.date] = []
        ends: list[dt.date] = []

        for row in rollup.iter_rows(named=True):
            exposure = exposures_by_id[row["drug_exposure_id"]]
            dose = derive_dose(exposure, concepts)
            if dose is None:
                continue
            person_ids.append(row["person_id"])
            ingredient_ids.append(row["ingredient_concept_id"])
            unit_ids.append(dose.unit_concept_id)
            dose_values.append(dose.dose_value)
            starts.append(row["start"])
            ends.append(row["end"])

        if not person_ids:
            return pl.DataFrame(schema=_DOSED_EXPOSURES_SCHEMA)

        return pl.DataFrame(
            {
                "person_id": person_ids,
                "ingredient_concept_id": ingredient_ids,
                "unit_concept_id": unit_ids,
                "dose_value": dose_values,
                "start": starts,
                "end": ends,
            }
        )
