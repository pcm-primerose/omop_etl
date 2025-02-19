from dataclasses import dataclass
import pandas as pd  # type: ignore
from pathlib import Path
from typing import Optional, List, Dict, Set
import logging as logging
import argparse
from datetime import datetime
import sys
import json

# configure logger
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def setup_logging(log_path: Optional[Path] = None) -> None:
    """Configure logging for the application.
    Only use if provided as arg to main."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # create formatters and handlers
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # file handler if log_path is provided
    handlers: List[logging.Handler] = [console_handler]
    if log_path:
        file_handler = logging.FileHandler(log_path / f"ecrf_processing_{timestamp}.log")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # configure root logger
    logging.basicConfig(level=logging.INFO, handlers=handlers)


@dataclass
class SheetConfig:
    """Stores what data is extracted from which sheet/file"""

    key: str
    usecols: List[str]


@dataclass
class SheetData:
    """Stores input data"""

    key: str
    data: pd.DataFrame
    input_path: Optional[Path] = None


@dataclass
class EcrfConfig:
    """Stores all configs with their data"""

    configs: List[SheetConfig]
    data: Optional[List[SheetData]] = None
    trial: Optional[str] = None
    source_type: Optional[str] = None

    def get_config(self, key: str) -> Optional[SheetConfig]:
        """Get configuration for a specific key"""
        return next((config for config in self.configs if config.key == key), None)

    def validate_data(self, data: Dict[str, pd.DataFrame]) -> None:
        """Validate that data matches configurations"""
        for config in self.configs:
            if config.key not in data:
                raise ValueError(f"Missing data for key: {config.key}")
            for col in config.usecols:
                if col not in data[config.key].columns:
                    raise ValueError(f"Column: {col} not found in data for key: {config.key}")

    def add_data(self, sheet_data: SheetData) -> None:
        """Add processed data for a sheet"""
        if self.data is None:
            self.data = []
        self.data.append(sheet_data)

    def get_all_data(self) -> List[SheetData]:
        """Ensure data is loaded and return the self.data"""
        if self.data is None:
            raise ValueError("No data has been loaded into the config")
        return self.data

    def get_sheet_data(self, key: str) -> SheetData:
        """Get SheetData instance by key or raise if not found"""
        if self.data is None:
            raise ValueError("No data has been loaded into the config")

        sheet_data = next((d for d in self.data if d.key == key), None)
        if sheet_data is None:
            raise ValueError(f"Data for key '{key}' not found")

        return sheet_data

    @property
    def is_data_loaded(self) -> bool:
        """Check if data has been loaded into the config"""
        return self.data is not None and len(self.data) > 0

    @classmethod
    def from_json(cls, ecrf_config: Path) -> "EcrfConfig":
        """Create confiug instance based on json config file"""
        if not ecrf_config.exists():
            raise FileNotFoundError(f"Config file not found at path: {ecrf_config}")

        with ecrf_config.open("r") as config:
            config_data = json.load(config)

        # instantiate SheetConfig
        configs = [SheetConfig(key=key.upper(), usecols=columns) for key, columns in config_data.items()]

        return cls(configs=configs)


class InputResolver:
    def __init__(self, input_path: Path, ecrf_config: EcrfConfig):
        """
        Initialize InputResolver with input path and ECRF configuration.

        Args:
            input_path: Path to input (Excel file or directory of CSVs)
            ecrf_config: Configuration containing sheet configs and optional data
        """
        self.input_path = input_path
        self.ecrf_config = ecrf_config
        self.source_type = self._determine_source_type()

    def resolve(self) -> EcrfConfig:
        """
        Load data according to configs and return updated EcrfConfig with loaded data.
        """
        try:
            if self.source_type == "excel":
                self._load_excel()
            else:
                self._load_csv()

            return self.ecrf_config

        except Exception as e:
            logger.error(f"Error resolving input data: {e}")
            raise

    def _determine_source_type(self) -> str:
        """Determine input type based on path"""
        if self.input_path.is_file() and self.input_path.suffix in {".xls", ".xlsx"}:
            return "excel"
        elif self.input_path.is_dir():
            return "csv"
        raise ValueError(f"Unsupported input type: {self.input_path}")

    def _load_excel(self) -> None:
        """Load data from Excel sheets based on configs"""
        excel_file = pd.ExcelFile(self.input_path)
        print(excel_file.sheet_names)

        for config in self.ecrf_config.configs:
            if config.key not in excel_file.sheet_names:
                logger.warning(f"Sheet '{config.key}' not found in Excel file")
                continue

            try:
                df = pd.read_excel(excel_file, sheet_name=config.key, skiprows=[0])
                df = self._reorder_rename_df(df, config.usecols)
                self.ecrf_config.add_data(SheetData(key=config.key, data=df))
                logger.info(f"Loaded sheet: {config.key}")

            except Exception as e:
                logger.error(f"Failed to load sheet '{config.key}': {e}")

    def _load_csv(self) -> None:
        """Load data from CSV files based on configs"""
        for config in self.ecrf_config.configs:
            matching_files = [file for file in self.input_path.iterdir() if file.is_file() and file.suffix.lower() == ".csv" and config.key == file.stem.split("_")[-1]]

            if not matching_files:
                logger.warning(f"No CSV file found for key: {config.key}")
                continue

            file_path = matching_files[0]

            try:
                # Read the CSV fully without skiprows if the header is in the first row.
                df = pd.read_csv(file_path, skiprows=[0])

                # Use the helper function to reorder and rename the DataFrame columns.
                df = self._reorder_rename_df(df, config.usecols)

                self.ecrf_config.add_data(SheetData(key=config.key, data=df))
                logger.info(f"Loaded CSV: {file_path.name}")

            except Exception as e:
                logger.error(f"Failed to load '{file_path}': {e}")

    @staticmethod
    def _reorder_rename_df(df: pd.DataFrame, expected_cols: list) -> pd.DataFrame:
        """
        Reorder and rename the DataFrame columns based on the expected columns.
        """
        # mapping from uppercase version of actual column names to the actual column name
        actual_mapping = {col.upper(): col for col in df.columns}

        ordered_actual_columns = []
        for expected in expected_cols:
            key = expected.upper()
            if key in actual_mapping:
                ordered_actual_columns.append(actual_mapping[key])
                print(f"Ordered actual cols: {ordered_actual_columns}")

            else:
                logger.warning(f"Expected column '{expected}' not found in data.")

        # reindex the DataFrame
        df_reordered = df.reindex(columns=ordered_actual_columns)
        df_reordered.columns = expected_cols

        print(f"Reordered cols: {df_reordered}")

        return df_reordered


class DataCombiner:
    def __init__(self, ecrf_config: EcrfConfig):
        self.ecrf_config = ecrf_config

    def combine(self) -> pd.DataFrame:
        """
        Combine all sheets into one DataFrame, preserving SubjectId relationships
        and multiple rows per subject where they exist.
        """
        logger.info("Starting data combination process...")

        processed_dfs = []
        for sheet_data in self.ecrf_config.get_all_data():
            # add sheet prefix to all columns except SubjectId
            df = sheet_data.data.copy()
            cols_to_rename = [col for col in df.columns if col != "SubjectId"]
            df = df.rename(columns={col: f"{sheet_data.key}_{col}" for col in cols_to_rename})

            processed_dfs.append(df)
            logger.info(f"Processed {sheet_data.key}: {len(df)} rows")

        # combine all dataframes and sort by SubjectId
        combined_df = pd.concat(processed_dfs, axis=0, ignore_index=True)
        combined_df = combined_df.sort_values("SubjectId").reset_index(drop=True)
        print(f"Combined df: {combined_df}")
        logger.info(f"Combination complete. Final dataset has {len(combined_df)} rows " f"for {combined_df['SubjectId'].nunique()} unique subjects")

        return combined_df

    @staticmethod
    def get_summary(df: pd.DataFrame) -> dict:
        """Generate summary of the combined data"""
        return {
            "total_rows": len(df),
            "unique_patients": df["SubjectId"].nunique(),
            "rows_per_patient": df.groupby("SubjectId").size().describe().to_dict(),
            "sheet_columns": {
                sheet: [col for col in df.columns if col.startswith(f"{sheet}_")]
                for sheet in {col.split("_")[0] for col in df.columns if "_" in col and col != "ProcessingTimestamp"}
            },
        }


class OutputFormatter:
    """
    Class to process combined data to final pre-processed output, updating in place.
    Handles patient data aggregation with conflict detection and resolution.
    """

    def __init__(self, combined_data: pd.DataFrame):
        self.combined_data = combined_data.copy()
        self.original_shape = combined_data.shape

    def run(self) -> pd.DataFrame:
        """
        Apply filtering and transformation in sequence.
        Prints progress information at each step.
        """
        print(f"Initial shape: {self.original_shape}")

        steps = [self._process_cohort_name, self._process_ecog, self._process_trial_id, self._process_subject_id, self._aggregate_on_id, self._reorder_columns]

        for step in steps:
            step()

        return self.combined_data

    def _process_cohort_name(self):
        """
        Filters out all rows for any patient (SubjectId) that never has a valid COHORTNAME.
        A valid COHORTNAME is one that is not NaN, not an empty string after stripping,
        and not "NA" (case-insensitive).
        """
        # create mask for valid cohort names
        valid_mask = (
            pd.notna(self.combined_data["COH_COHORTNAME"])
            & (self.combined_data["COH_COHORTNAME"].str.strip() != "")
            & (~self.combined_data["COH_COHORTNAME"].str.upper().eq("NA"))
        )

        # get subjects with at least one valid cohort name
        valid_subjects = self.combined_data.groupby("SubjectId")["COH_COHORTNAME"].transform(lambda x: valid_mask[x.index].any())

        self.combined_data = self.combined_data[valid_subjects]
        print(f"After filtering by cohort: {self.combined_data.shape}")

    def _process_ecog(self):
        """
        For rows that have ECOG data, only keep rows where ECOG_EventId is "V00".
        Rows without any ECOG data (i.e. ECOG_EventId is NaN) are kept.
        """
        mask = self.combined_data["ECOG_EventId"].isna() | (self.combined_data["ECOG_EventId"] == "V00")
        self.combined_data = self.combined_data[mask]
        print(f"After filtering ECOG rows: {self.combined_data.shape}")

    def _process_trial_id(self):
        """Adds/updates a column "Trial" with the trial name."""
        self.combined_data["Trial"] = "IMPRESS"

    def _process_subject_id(self):
        """
        Replace the original SubjectId values with new IMPRESS IDs.
        For example, if a subject was "X_1234_1", it becomes "IMPRESS-X_1234_1".
        """
        self.combined_data["SubjectId"] = "IMPRESS-" + self.combined_data["SubjectId"]

    def _process_tumor_type(self):
        """
        Maybe combine the drop-down tumor type CD lists to one - since they are complementary (COHTTYPECD and COHTTYPE_2CD).
        """
        # self.combined_data["COHHTYPECD_MERGED"] = self.combined_data["COHTTYPECD"] & self.combined_data["COHTTYPE__2CD"]
        # I think it's better to handle all processing downstream of extraction, and just extract here instead (?)
        pass

    def _aggregate_on_id(self):
        """
        For each SubjectId, collapse the group into a single row if possible.
        Rows are combined only if there are no conflicts in non-null values across all columns.
        """

        def can_merge_group(group: pd.DataFrame) -> bool:
            """Check if a group of rows can be merged (no conflicts in non-null values)."""
            for col in group.columns:
                if col == "SubjectId":
                    continue
                # get unique non-null values
                unique_vals = group[col].dropna().unique()
                if len(unique_vals) > 1:
                    return False
            return True

        def merge_group(group: pd.DataFrame) -> pd.DataFrame:
            """Merge a group of rows by taking first non-null value for each column."""
            if not can_merge_group(group):
                return group

            # for each column, take the first non-null value
            merged_row = {}
            for col in group.columns:
                non_null_vals = group[col].dropna()
                merged_row[col] = non_null_vals.iloc[0] if len(non_null_vals) > 0 else None

            return pd.DataFrame([merged_row])

        # process each group and concatenate results
        result_dfs = [merge_group(group) for _, group in self.combined_data.groupby("SubjectId")]

        self.combined_data = pd.concat(result_dfs, ignore_index=True)
        print(f"After aggregation on SubjectId: {self.combined_data.shape}")

    def _reorder_columns(self):
        """
        Reorder columns to ensure SubjectId is first and Trial is second.
        Remaining columns follow in their original order.
        """
        # get all columns except SubjectId and Trial
        other_cols = [col for col in self.combined_data.columns if col not in ["SubjectId", "Trial"]]

        # create new column order
        new_order = ["SubjectId", "Trial"] + other_cols

        # reorder columns, handling case where Trial might not exist
        existing_cols = [col for col in new_order if col in self.combined_data.columns]
        self.combined_data = self.combined_data[existing_cols]


class Output:
    """Handles writing of processed dataframes to various output formats."""

    SUPPORTED_FORMATS: Set[str] = {"csv", "txt"}

    def __init__(self, output_path: Path, data: pd.DataFrame, output_format: str = "csv"):
        self.output_path = output_path
        self.data = data
        self.format = output_format.lower()  # normalize format string

    def write_output(self) -> None:
        """Write the dataframe to the specified output format."""
        try:
            self._validate_format()
            self._validate_path()

            logger.info(f"Writing output to {self.output_path} in {self.format} format...")

            if self.format == "csv":
                self._write_csv()
            elif self.format == "txt":
                self._write_tsv()

            logger.info(f"Successfully wrote {len(self.data)} rows to {self.output_path}")

        except Exception as e:
            logger.error(f"Failed to write output: {str(e)}")
            raise

    def _write_csv(self) -> None:
        """Write data to CSV file."""
        try:
            self.data.to_csv(
                self.output_path,
                index=False,
                na_rep="NA",
                date_format="%Y-%m-%d",
            )
        except Exception as e:
            logger.error(f"Error writing CSV file: {str(e)}")
            raise

    def _write_tsv(self) -> None:
        """Write data to tab-separated text file."""
        try:
            self.data.to_csv(
                self.output_path,
                sep="\t",
                index=False,
                na_rep="NA",
                date_format="%Y-%m-%d",
            )
        except Exception as e:
            logger.error(f"Error writing TXT file: {str(e)}")
            raise

    def _validate_format(self) -> None:
        """Validate that the specified format is supported."""
        if self.format not in self.SUPPORTED_FORMATS:
            msg = f"Output format '{self.format}' not supported. Supported formats are: {', '.join(self.SUPPORTED_FORMATS)}"
            logger.error(msg)
            raise ValueError(msg)

    def _validate_path(self) -> None:
        """Validate the output path."""
        # check if directory exists
        if not self.output_path.parent.exists():
            msg = f"Output directory does not exist: {self.output_path.parent}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        # check if file already exists
        if self.output_path.exists():
            logger.warning(f"Output file already exists and will be overwritten: {self.output_path}")

        # validate file extension matches format
        expected_extension = f".{self.format}"
        if self.output_path.suffix != expected_extension:
            logger.warning(f"Output file extension '{self.output_path.suffix}' doesn't match format '{self.format}'. " f"Expected extension: '{expected_extension}'")


def validate_paths(input_path: Path, output_path: Path) -> None:
    """Validate input and output paths."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    if not output_path.parent.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output_path.parent}")


def main(
    input_path: Path,
    output_path: Path,
    config_path: Path,
    log_path: Optional[Path] = None,
    output_format: str = "csv",
) -> None:
    """
    Process eCRF data for the IMPRESS trial and write the merged DataFrame to a file.

    Args:
        input_path (Path): Path to the input data (Excel file or CSV directory).
        output_path (Path): Path to save the output file.
        log_path (Optional[Path]): Path to save log files. If None, logs only to console.
        output_format (str): Format of output file ('csv' or 'txt'). Defaults to 'csv'.

    Raises:
        FileNotFoundError: If input path doesn't exist or output directory is invalid.
        ValueError: If sheet configurations are invalid or data processing fails.
        Exception: For other unexpected errors during processing.
    """
    try:
        # set up logging
        setup_logging(log_path)
        logger.info(f"Starting eCRF data processing at {datetime.now()}")

        # validate paths
        validate_paths(input_path, output_path)

        # log configuration
        logger.info(f"Input path: {input_path}")
        logger.info(f"Output path: {output_path}")
        logger.info(f"Output format: {output_format}")

        # load config, instantiate SheetConfig and EcrfConfig
        logger.info("Loading config data from json..")
        ecrf_config = EcrfConfig.from_json(config_path)
        ecrf_config.trial = "impress"

        # use InputResolver to load data based on configs
        logger.info("Resolving input data...")
        resolver = InputResolver(input_path=input_path, ecrf_config=ecrf_config)
        ecrf_config = resolver.resolve()

        # combine data
        logger.info("Combining data from all sheets...")
        combiner = DataCombiner(ecrf_config)
        combined_data = combiner.combine()

        # get and log summary statistics
        summary = combiner.get_summary(combined_data)
        logger.info("Data combination summary:")
        logger.info(f"Total rows: {summary['total_rows']}")
        logger.info(f"Unique patients: {summary['unique_patients']}")

        # filter data
        formatter = OutputFormatter(combined_data)
        output_data = formatter.run()

        # write output
        logger.info(f"Writing output to {output_path}...")
        output = Output(output_path=output_path, data=output_data, output_format=output_format)
        output.write_output()

        logger.info("Processing completed successfully!")

    except FileNotFoundError as e:
        logger.error(f"File not found error: {e}")
        raise
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        raise
    finally:
        logger.info(f"Processing finished at {datetime.now()}")


# TODO: Move this when extracting to spearate packages
def get_config_path(custom_config_path: Optional[Path] = None) -> Path:
    """
    Get configuration file path, either provided at runtime or default ones based on trial ID?
    """
    if custom_config_path:
        if not custom_config_path.exists():
            raise FileNotFoundError(f"Custom config file not found: {custom_config_path}")
        return custom_config_path

    default_config: Path = Path(__file__).parents[3] / "configs" / "impress_ecrf_variables.json"
    print(f"Default config: {default_config}")
    with open(default_config) as f:
        config_data = json.load(f)
    print(config_data)
    if not default_config.exists():
        raise FileNotFoundError(f"Default config file not found: {default_config}")

    return default_config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process eCRF data for clinical trials.")
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Path to input data (Excel file or CSV directory)",
    )
    parser.add_argument("-o", "--output", type=Path, required=True, help="Path for output file")
    parser.add_argument("-t", "--trial", type=str, required=True, help="Trial ID (e.g., 'impress')")
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        help="Optional: Path to custom JSON configuration file (overrides default)",
    )
    parser.add_argument("-l", "--log-path", type=Path, help="Path for log files (optional)")
    parser.add_argument(
        "-f",
        "--format",
        choices=["csv", "txt"],
        default="csv",
        help="Output format (default: csv)",
    )

    args = parser.parse_args()
    config_path = get_config_path(custom_config_path=args.config)

    main(
        input_path=args.input,
        output_path=args.output,
        config_path=config_path,
        log_path=args.log_path,
        output_format=args.format,
    )
