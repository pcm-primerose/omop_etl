from pathlib import Path
import polars as pl
from src.pre_processing.impress.impress_ecrf import impress_preprocessor
from src.harmonization.harmonizers.impress import ImpressHarmonizer


def output_dir_preprocessing() -> Path:
    outpath = Path(__file__).parents[1] / ".data" / "preprocessing"
    if not outpath.exists():
        outpath.mkdir(parents=True, exist_ok=True)

    return outpath


def raw_impress_data() -> Path:
    return Path(__file__).parents[2] / "ecrf_mocker" / "output"


def config_path() -> Path:
    return Path(__file__).parents[1] / "configs" / "impress_ecrf_variables.json"


def main():
    # pre-process synthetic data to one file
    pre_processed_data: Path = impress_preprocessor(
        input_path=raw_impress_data(),
        output_path=output_dir_preprocessing() / "preprocessed_impress_synthetic.csv",
        config_path=config_path(),
    )

    # harmonize synthetic data
    impress_df = pl.read_csv(pre_processed_data)
    harmonizer = ImpressHarmonizer(data=impress_df, trial_id="IMPRESS")
    harmonized_data = harmonizer.process()
    print(harmonized_data)


if __name__ == "__main__":
    main()


# basic structure from workshop:
# def some_query_function():
#     # actual SQL query should just
#     # be stateless, some dependancy injection mb of sessions in SQL alchemy
#     # and qeuries either as raw SQL or just SQLalchemy
#     pass


# def some_table_class(session):
#     # also have file to semantic mappings etc
#     # and db paths etc
#     # call sql functions here (modular, nice)
#     # log stuff
#     # use SQLalchemy
#     pass


# def main():
#     # all run state etc in main
#     # call all person table classes here
#     pass


# maybe something like this, using SQL for mapping and merging and python for higher abstraction,
# but retaining python objects as outputs
# (to get better structure, type-safety, error handling, logging, field-validation, modularity),
# this is similar to the work-shop where they ised SQL for inner logic but SQLAlchemy to handle session state and table-logic:
# class SourceDataLoader:
#     def load_and_validate(self, source_file: Path) -> DataFrame:
#         # input resolution and validation
#         pass


# class HarmonizedData:
#     def __init__(self, data: DataFrame):
#         self.validate_schema()
#         # pydantic models for each OMOP table


# class PersonTable(BaseModel):
#     def __init__(self, age: int, year_of_birth: int):
#         self.age = age
#         self.year_of_birth = year_of_birth


# class SemanticMapper:
#     def __init__(self, vocabulary_connection: SQLConnection):
#         self.vocab_connection = vocabulary_connection
#
#     def map_concepts(self, harmonized_data: HarmonizedData) -> MappedData:
#         # use SQL for concept mapping but keep results in Python objects
#         pass
#
#
# class CDMWriter:
#     def write_to_cdm(self, mapped_data: MappedData, cdm_connection: SQLConnection):
#         # final SQL operations to populate CDM
#         pass
