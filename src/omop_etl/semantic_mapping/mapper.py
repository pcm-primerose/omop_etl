from pathlib import Path
import polars as pl

from src.omop_etl.harmonization.datamodels import HarmonizedData

# todo: for just make it basic and simple


def load_braf_semantics() -> pl.LazyFrame:
    # todo: unsure if i want lazyframes, maybe mem mapped json is faster
    #   or some other data structure is better for bm25 etc
    semantics_file = (Path.__file__.parents[2] / ".data" / "semantic_mapping" / "mapped" / "braf_non--v600_mapped.csv").resolve()
    return pl.scan_csv(semantics_file, infer_schema=0)


def do_mapping():
    for patient in HarmonizedData.patients:
        pass

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
