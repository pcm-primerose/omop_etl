from dataclasses import dataclass, asdict
from typing import List

from omop_etl.omop.models.rows import PersonRow


@dataclass(frozen=True, slots=True)
class OmopTables:
    """Stores all tables from OMOP builder"""

    person: List[PersonRow]
    meta: None | dict = None

    def __getitem__(self, item):
        if item in asdict(self):
            print(f"asdict: {asdict(self)}")
            print("")
            return asdict(self)[item]
        raise KeyError(f"Key: {item} not in {OmopTables.__name__}")
