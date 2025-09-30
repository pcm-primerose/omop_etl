from pathlib import Path

from omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer
from omop_etl.preprocessing.api import list_trials, make_ecrf_config, preprocess_trial
from omop_etl.preprocessing.core.models import PreprocessingRunOptions, PreprocessResult
import polars as pl

# TODO: should only import from API
#   need to expose more methods


def mock_main():
    # 1. preprocess a trial using the preprocessing API

    # validate source
    available_trials = list_trials()
    for source in available_trials:
        if source not in available_trials:
            raise KeyError(f"Source {source} not available")

    # create configs
    impress_run_options = PreprocessingRunOptions(filter_valid_cohort=True)
    impress_config = make_ecrf_config(trial="IMPRESS")

    # run pipeline through API, it writes the file
    impress_preprocessed: PreprocessResult = preprocess_trial(
        trial="IMPRESS",
        input_path=Path("impress_input_file.xlsx"),
        config=impress_config,
        run_options=impress_run_options,
        fmt="csv",
        combine_key="SubjectId",
    )

    # todo: file writing is not exposed in API:
    # impress_preprocessed.write_output(format="csv", path="some_path")

    # 2. harmonize data

    # create contexts (in the future, load from config file)
    # todo: run context is not exposed to API:
    # drup_run_context = make_run_context(trial)

    # get data from non-pre-processed files as well
    # drup_data = Path("some_path.csv")
    # finprove_data = Path("some_other_path.csv")
    impress_data = impress_preprocessed.output_path
    impress_df = pl.read_csv(impress_data)

    impress_harmonizer = ImpressHarmonizer(impress_df, trial_id="IMPRESS")
    harmonized_impress = impress_harmonizer.process()

    # TODO: want to be able to just run from a directory containing files
    # make pipeline layer
    # harmonized_data: HarmonizedData = harmonize(
    #     path=Path("dir_with_all_data"),
    #     combine_trials=True
    # )

    # other processors not yet implemented:
    # drup_harmonizer = DrupHarmonizer(impress_data, trial_id="DRUP")
    # harmonized_drup = drup_harmonizer.process()
    # finprove_harmonizer = FinproveHarmonizer(finprove_data, trial_id="FINPROVE")
    # harmonized_finprove = finprove_harmonizer.process()

    # do any number of filters, can be chained
    # filtered_impress = harmonized_impress.filter(predicate=lambda p: p.has_any_adverse_events == True)

    # write output
    # normalized_impress = filtered_impress.to_frames_normalized()

    # below not yet implemented, something like this?
    # output formatter not finished:
    # OutputFormatter.write_csv(normalized)

    # 3. use harmonized data downsteeam (as obj in mem, or from files): NOT IMPLEMENTED YET
    # use harmonized data in downstream processing (not yet implemented)
    # semantic_mapper = SemanticMapper()
    # mapped_impress = semantic_mapper.map(data=harmonized_impress)

    # load to database (not yet implemented)
    # connect or create DB
    # db_connection = DatabaseConnector(port=default_port)
    # validate_database_schema(db_connection)
    # database_loader = DatabaseLoader()
    # database_loader.map_to_omop_tables(mapped_impress, replace=True)
    pass
