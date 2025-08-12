from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Mapping, Sequence
import pandas as pd


@dataclass(frozen=True)
class SheetConfig:
    key: str
    usecols: Sequence[str]


@dataclass
class SheetData:
    key: str
    data: pd.DataFrame
    input_path: Optional[Path] = None


@dataclass
class EcrfConfig:
    configs: list[SheetConfig]
    data: list[SheetData] | None = None
    trial: str | None = None
    source_type: str | None = None

    @classmethod
    def from_mapping(cls, m: Mapping[str, list[str]]) -> "EcrfConfig":
        return cls([SheetConfig(key=k.upper(), usecols=v) for k, v in m.items()])
