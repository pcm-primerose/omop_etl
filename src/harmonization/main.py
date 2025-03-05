import polars as pl
from pathlib import Path
from src.harmonization.datamodels import HarmonizedData
from src.harmonization.harmonizers.impress import ImpressHarmonizer
from src.harmonization.harmonizers.drup import DrupHarmonizer


# Each abstract method harmonized towards one PRIME-ROSE variable, and return instances of that model?
# need to update the data models in any case, implement IMPRESS first if we can export the anonymized data,
# if not start with DRUP even though it's missing some QoL stuff.
# Main file for harmonizing in this package.
# Each subclass reads in respective data from each trial, instantiate input file
# need input class, we can easily infer trial name from data nad call the correct subclass from main
# ending in validated dataclasses contianing proper strucutre (datamodels, immutable dataclasses with field validation).
# These will then be passed as structs to the next part of the pipeline, doing sematic mapping and then structural mapping.
# probably easiest to just keep using pandas or polars since we have tabular data and we probably get most/all data as csv files
# or I could extract from csv to struct but that seems kind of unneccesary.
# after mapping, we can use SQL Alchemy (but avoid too much state) and raw SQL to do inner logic,
# and populate the empty OMOP CDM. We then need to link each struct to the respective table in the database.
# if I can export the pseduo-anonymized data I can test all of this locally, just use the Athena data and make a CDM
# from that to do mapping (or keep as files), then query that with AutOMOP, make mapping files, log unmapped vars,
# and contruct emopty DB with same structure as CDM, use structural mapping with linkage and logic from struct to tables
# and instantiate, something like that.
# Should also have a method on the top-level that finds unique values for all fields per trial, and writes to a vocabulary file
# to be used for sematic mapping (in the semantic mapping stage).
# Do final patient ID renaming after processing data across all trials (just easier to get total patients first)

# So pydantic dataclasses should just mirror the OMOP CDM tables I need to use.
# so that'll make things a lot easier. Then I'll use Polars in harmonization and to process the data
# and keep each dataclass modular by using patient ID as a foreign key.
# The end result can then be composition of all patients. Unsure if I should make more modular, using patient_id as foreign key per dataclass
# but for now just implement using normal composition and if needed later make more modular.

# TODO:
#   [x] Implement basic example to harmonize cohort name (ignore upstream I/O, factory etc)
#   [x] Implement patient ID (need to know trial - best way to do this? just use dependancy injection and worry about that later)
#   [x] Test these with output (make fixtures) and if that works extend and think about best way to design this

# TODO:
#   [ ] Implement rest of methods for DRUP and IMPRESS data (will take some time)
#   [ ] Figure out best way to create new IDs without making all dataclasses mutable (currently they are mutable for dev but should be frozen)
#   [ ] Make nice methods for filtering finished objects (get only specific cohort, patient etc)
#   [ ] Make upstream IO resolve function (maybe infer trial type unless specified - easy to check)
#   [ ] Fix faulty ECOG parsing (find out if its formatting issue or some char split thing in pre-processing)

# TODO:
#   [ ] Probably best to refactor to normalized approach and keep things modular and as explicit as possible but finish first harmonization iteration


def drup_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def impress_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def process_impress(file: Path) -> HarmonizedData:
    data = impress_data(file)
    harmonizer = ImpressHarmonizer(data, trial_id="IMPRESS")
    return harmonizer.process()


# def process_drup(file: Path) -> HarmonizedData:
#     data = drup_data(file)
#     harmonizer = DrupHarmonizer(data=data, trial_id="DRUP")
#     return harmonizer.process()


if __name__ == "__main__":
    # drup_file = Path(__file__).parents[2] / ".data" / "drup_dummy_data.csv"
    impress_file = (
        Path(__file__).parents[2] / ".data" / "impress_mockdata_2025-02-18.csv"
    )
    impress = process_impress(impress_file)
    print("\n")
    # drup = process_drup(drup_file)
