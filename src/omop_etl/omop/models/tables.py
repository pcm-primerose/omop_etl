from dataclasses import dataclass
from typing import List

from omop_etl.omop.models.rows import PersonRow, ObservationPeriodRow, CdmSourceRow


@dataclass(frozen=True, slots=True)
class OmopTables:
    """Built OMOP tables"""

    person: List[PersonRow]
    observation_period: List[ObservationPeriodRow]
    cdm_source: CdmSourceRow

    meta: None | dict = None

    def __getitem__(self, item: str):
        if hasattr(self, item):
            return getattr(self, item)
        raise KeyError(...)
