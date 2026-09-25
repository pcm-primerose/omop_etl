from collections.abc import Sequence
from pathlib import Path
import polars as pl

from omop_etl.harmonization.models.patient import Patient
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.helpers.context import BuildContext
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.builders.death import DeathBuilder
from omop_etl.omop.builders.measurement import MeasurementBuilder
from omop_etl.omop.builders.observation import ObservationBuilder
from omop_etl.omop.builders.person import PersonBuilder
from omop_etl.omop.builders.observation_period import ObservationPeriodBuilder
from omop_etl.omop.builders.cdm_source import CdmSourceBuilder
from omop_etl.omop.builders.procedure_occurrence import ProcedureOccurrenceBuilder
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.builders.drug_exposure import DrugExposureBuilder
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.builders.episode_event import EpisodeEventBuilder
from omop_etl.omop.builders.cohort import CohortBuilder
from omop_etl.omop.builders.cohort_definition import CohortDefinitionBuilder
from omop_etl.omop.builders.location import LocationBuilder
from omop_etl.omop.builders.condition_era import ConditionEraBuilder
from omop_etl.omop.builders.drug_era import DrugEraBuilder
from omop_etl.omop.builders.dose_era import DoseEraBuilder
from omop_etl.omop.core.id_generator import RowIdGenerator
from omop_etl.omop.core.io import OmopTableExporter
from omop_etl.omop.core.linkage import PolymorphicTarget
from omop_etl.omop.core.pre_load_gate import validate_integrity
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.vocabulary import Vocabulary


class OmopService:
    """
    Entry point for building OMOP CDM rows from patient data.

    Takes mapped patient data and constructs OMOP-compliant rows using
    table-centric builders. Builders emitting context consumed downstream
    (like visit_occurrence) are built first.
    """

    def __init__(
        self,
        concepts: ConceptLookupService,
        vocabulary: Vocabulary,
        concept_ancestor: pl.DataFrame,
        athena_version: str,
        outdir: Path,
    ):
        self._concepts = concepts
        self._vocabulary = vocabulary
        self._concept_ancestor = concept_ancestor
        self._athena_version = athena_version
        self._outdir = outdir
        self._row_id_generator = RowIdGenerator()
        self._builders: list[OmopBuilder] = [
            VisitOccurrenceBuilder(concepts, self._row_id_generator),
            PersonBuilder(concepts, self._row_id_generator),
            ObservationPeriodBuilder(concepts, self._row_id_generator),
            DrugExposureBuilder(concepts, self._row_id_generator),
            ConditionOccurrenceBuilder(concepts, self._row_id_generator),
            ProcedureOccurrenceBuilder(concepts, self._row_id_generator),
            MeasurementBuilder(concepts, self._row_id_generator),
            ObservationBuilder(concepts, self._row_id_generator),
            DeathBuilder(concepts, self._row_id_generator),
            EpisodeBuilder(concepts, self._row_id_generator),
            EpisodeEventBuilder(concepts, self._row_id_generator),
            CohortBuilder(concepts, self._row_id_generator),
        ]

    def build(self, patients: Sequence[Patient], meta: RunMetadata) -> OmopTables:
        """
        Build all OMOP tables from patient data. Validates PK/FK/concept
        integrity, then writes each populated table to a CSV under `outdir`.
        """
        tables = OmopTables()
        polymorphic_targets: list[PolymorphicTarget] = []

        for patient in patients:
            # skips all patients without birth date
            if patient.date_of_birth is None:
                continue

            person_id = self._row_id_generator.generate("person", patient.patient_id)

            ctx = BuildContext(patient=patient, person_id=person_id)

            for builder in self._builders:
                rows = builder.build_and_populate(ctx)
                tables.extend(builder.table_name, list(rows))

            polymorphic_targets.extend(ctx.polymorphic_targets)

        # singleton metadata row
        tables.add(OmopTables.CDM_SOURCE, CdmSourceBuilder(self._concepts, self._athena_version).build())

        # cross-patient reference: one cohort_definition per distinct arm observed
        tables.extend(
            OmopTables.COHORT_DEFINITION,
            CohortDefinitionBuilder(self._concepts, self._row_id_generator).build(patients),
        )

        # cross-patient reference: one location per distinct trial country
        tables.extend(
            OmopTables.LOCATION,
            LocationBuilder(self._concepts, self._row_id_generator).build(patients),
        )

        # derived era tables: pure transforms over already-built rows
        tables.extend(
            OmopTables.CONDITION_ERA,
            ConditionEraBuilder(self._row_id_generator).build(tables.condition_occurrence),
        )
        tables.extend(
            OmopTables.DRUG_ERA,
            DrugEraBuilder(self._row_id_generator).build(tables.drug_exposure, self._concept_ancestor, self._vocabulary),
        )
        tables.extend(
            OmopTables.DOSE_ERA,
            DoseEraBuilder(self._row_id_generator).build(tables.drug_exposure, self._concept_ancestor, self._vocabulary, self._concepts),
        )

        # validate PK/FK integrity & write output
        validate_integrity(tables=tables, vocabulary=self._vocabulary, polymorphic_targets=polymorphic_targets)
        OmopTableExporter(self._outdir).write(tables, meta)

        return tables
