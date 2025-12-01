from pathlib import Path
import polars as pl
from dataclasses import dataclass

from src.omop_etl.harmonization.datamodels import HarmonizedData

# todo: for just make it basic and simple


@dataclass(slots=True)
class SemanticResult:
    query: str
    result: str


def load_braf_semantics() -> pl.LazyFrame:
    # todo: unsure if i want lazyframes, maybe mem mapped json is faster
    #   or some other data structure is better for bm25 etc
    semantics_file = Path(__file__).parents[3] / ".data" / "semantic_mapping" / "mapped" / "braf_non-v600_mapped.csv"
    return pl.scan_csv(semantics_file, infer_schema=0)


def query_semantics(semantics: pl.LazyFrame) -> SemanticResult:
    pass


# todo: either use polars; find columns with matches as boolean series,
#   then use these to find mapped term
#   or use

# 1. need to load semantic mapped data
# 2. need to access correct instances/fields of dataclass
# 3. need to query the semantic data with this data
# 4.


def query_semantic_data(query):
    pass


def do_mapping(mapped_data: pl.LazyFrame, harmonized_data: HarmonizedData):
    for patient in harmonized_data.patients:
        for ae in patient.adverse_events:
            term = ae.term.lower()
            mapped_term = mapped_data.select(pl.any_horizontal(pl.col("source_term") == term))
            print(f"mapped AE term: {mapped_term.collect()}")
            break
        break

    # index into fields and map the ones in need of semantic mapping
    # can do field comparison in polars and join
    # I can load patient data from output files or keep in mem,
    # if in mem it's annoying to convert to formats
    # for now it's fair to assume all harmonized data is just HarmonizedData
    #   - can write read from json, csv, etc later and create HarmonizedData instance from that
    # or load as dict from json output?
    # what should be the output?
    # i could overload and inherit from HarmonizedData
    # and make new output class
    # but this will grow in scope over time and need many structs for intermediate data

    # I think I'm tired of polars? also don't want to spend time on converting types and ds between polars/other things
    # or numpy? or just native python maybe idk
    # look at BM25 implementations, maybe I can implement it myself in base numpy or in polars idk
    #
    pass
