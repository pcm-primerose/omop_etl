from dataclasses import dataclass, asdict
from typing import List

from omop_etl.omop.models.rows import PersonRow, ObservationPeriodRow


@dataclass(frozen=True, slots=True)
class OmopTables:
    """Built OMOP tables"""

    person: List[PersonRow]
    observation_period: List[ObservationPeriodRow]

    meta: None | dict = None

    def __getitem__(self, item) -> "OmopTables":
        if item in asdict(self):
            return item
        raise KeyError(f"Key: {item} not in {OmopTables.__name__}")
