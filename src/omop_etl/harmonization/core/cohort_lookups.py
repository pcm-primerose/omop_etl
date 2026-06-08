from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files as pkg_files
from logging import getLogger

import polars as pl

log = getLogger(__name__)

_BASE = pkg_files("omop_etl.resources") / "cohort_harmonization"


@dataclass(frozen=True, slots=True)
class CohortLookups:
    """
    Cross-source cohort-harmonization dictionaries mapping a raw source value
    to its harmonized form. Keys are normalized, values keep their canonical casing.

    These harmonize cohort *parts* before OMOP mapping. OMOP concept mapping
    (e.g. drugs) is downstream and handled by mappers.
    """

    biomarker: dict[str, str]
    cancer_type: dict[str, str]


def _read_lookup(filename: str, key_col: str, val_col: str) -> dict[str, str]:
    """
    Read a two-column harmonization CSV into {normalized_source: harmonized}.

    Last non-null mapping for a given normalized key wins.
    """
    path = _BASE / filename
    with path.open("rb") as f:
        df = pl.read_csv(f)
    for col in (key_col, val_col):
        if col not in df.columns:
            raise ValueError(f"cohort lookup {filename!r} missing column {col!r}; has {df.columns}")
    out: dict[str, str] = {}
    for raw_key, raw_val in zip(df[key_col].to_list(), df[val_col].to_list()):
        if raw_key is None or raw_val is None:
            continue
        key = str(raw_key).strip().lower()
        val = str(raw_val).strip()
        if not key or not val:
            continue
        out[key] = val
    return out


@lru_cache(maxsize=1)
def load_cohort_lookups() -> CohortLookups:
    """Load the cohort harmonization dictionaries."""
    return CohortLookups(
        biomarker=_read_lookup(
            "harmonized_target_biomarkers.csv",
            key_col="source_biomarker_name",
            val_col="harmonized_biomarker_name",
        ),
        cancer_type=_read_lookup(
            "harmonized_tumor_types.csv",
            key_col="source_tumor_type_name",
            val_col="harmonized_tumor_type_name",
        ),
    )
