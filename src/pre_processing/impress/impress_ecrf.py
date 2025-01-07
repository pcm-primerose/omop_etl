# import openpyxl as px  # type: ignore
from dataclasses import dataclass, asdict, field
import pandas as pd  # type: ignore
from pathlib import Path
from typing import Optional, List, Dict, Set
import logging as logging
import argparse
from datetime import datetime
import sys

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


# NOTE: three different documents that don't always agree on what to extract.
# approach here is completionist: just extract everything, easy to update/ignore downstream.
# NOTE: idea is to make this configurable and scaleable to minimize time spent adding support
# for new trials. adding new configs uses a common interface without changing the implementation,
# all that's needed is writing trial-specific pre-processing functions and making the config.
# NOTE: need to flesh out when (if) we get access to other ecrf systems, in that case:
# TODO: add support all data in oe file, dockerize this, split into seprate modules,
# make trial-specific processing modular, use class to get processing logic and apply it per trial,
# should actually refactor to use dataclasses instead of pandas, would make things much cleaner,
# map sheet/filename to each dataclass, something like this (but then I might as well harmonize!?

# NOTE: Found clinical benefit at week 16, but only over telephone, and not for all patients,
# but only place where this info is stored:
# RESP: EventName = Week16, Telephone, RESPEV = Partial Response

# TODO Fix stubs for pandas and openpyxl
# TODO just avoid aggregation and downstream grab unique IDs and instantiate dataclasses based on key


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


@dataclass(frozen=True)
class ImpressColumns:
    """Stores column configurations for IMPRESS eCRF data"""

    coh: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "COHORTNAME",
            "ICD10COD",
            "ICD10DES",
            "COHALLO1",
        ]
    )
    ecog: List[str] = field(default_factory=lambda: ["SubjectId", "EventId", "ECOGS", "ECOGSCD"])
    dm: List[str] = field(default_factory=lambda: ["SubjectId", "BRTHDAT", "SEX", "SEXCD"])
    ct: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "CTTYPE",
            "CTTYPECD",
            "CTTYPESP",
            "CTSTDAT",
            "CTSPID",
            "CTENDAT",
            "SQCTYN",
            "SQCTYNCD",
            "CTSTDAT",
        ]
    )
    eos: List[str] = field(default_factory=lambda: ["SubjectId", "DEATHDTC", "EOSDAT"])
    fu: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "FUPDEDAT",
            "FUPALDAT",
            "FUPDEDAT",
            "FUPSSTCD",
            "FUPSST",
            "FUPSSTCD",
        ]
    )
    tr: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "TRTNO",
            "TRC1_DT",
            "TRCNO1",
            "TRIVDS1",
            "TRIVU1",
            "TRIVDELYN1",
            "TRDSDEL1",
        ]
    )
    eot: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventDate",
            "EOTPROGDTC",
            "EOTDAT",
            "EOTREOTCD",
            "EOTREOT",
        ]
    )
    cm: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "CMTRT",
            "CMMHYN",
            "CMSTDAT",
            "CMONGO",
            "CMENDAT",
            "CMAEYN",
            "CMAENO",
        ]
    )
    ae: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "AETOXGRECD",
            "AECTCAET",
            "AESTDAT",
            "AEENDAT",
            "AEOUT",
            "AESPID",
            "AESERCD",
            "AETRT1",
            "AEREL1",
            "AETRT2",
            "AEREL2",
            "SAESDAT",
            "SAEEXP1",
            "SAEEXP1CD",
            "AETRTMM1",
            "SAEEXP2",
            "SAEEXP2CD",
            "AETRTMM2",
        ]
    )
    vi: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventDate",
            "VITUMA",
            "VITUMA_2",
            "VITUMACD",
            "VITUMA__2CD",
        ]
    )
    ra: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventDate",
            "RARECBAS",
            "RARECCUR",
            "RARECNAD",
            "RABASECH",
            "RATIMRES",
            "RARECCH",
            "RAiMOD",
            "RNALBASE",
            "RNALBASECD",
        ]
    )
    rnrsp: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventDate",
            "TERNTBAS",
            "TERNTB",
            "TERNAD",
            "TERNCFB",
            "TERNCFN",
            "RNRSPCL",
            "RNRSPLC",
            "RNRSPCLCD",
            "RNRSPNL",
            "RNRSPNLCD",
        ]
    )
    lugrsp: List[str] = field(default_factory=lambda: ["SubjectId", "EventDate", "LUGOVRL"])
    emlrsp: List[str] = field(default_factory=lambda: ["SubjectId", "EventDate", "EMLRESP"])
    br: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "BRRESP",
            "BRRESPCD",
            "BRCPRDAT",
            "BRPDDAT",
        ]
    )
    resp: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventName",
            "RESPDAT",
            "RESPDATCD",
            "RESPEV",
        ]
    )
    eqd5: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventName",
            "EventDate",
            "EQ5D1",
            "EQ5D2",
            "EQ5D3",
            "EQ5D4",
            "EQ5D5",
        ]
    )
    c30: List[str] = field(
        default_factory=lambda: [
            "SubjectId",
            "EventName",
            "EventDate",
            "C30_Q1",
            "C30_Q1CD",
            "C30_Q2",
            "C30_Q2CD",
            "C30_Q3",
            "C30_Q3CD",
            "C30_Q4",
            "C30_Q4CD",
            "C30_Q5",
            "C30_Q5CD",
            "C30_Q6",
            "C30_Q6CD",
            "C30_Q7",
            "C30_Q7CD",
            "C30_Q8",
            "C30_Q8CD",
            "C30_Q9",
            "C30_Q9CD",
            "C30_Q10",
            "C30_Q10CD",
            "C30_Q11",
            "C30_Q11CD",
            "C30_Q12",
            "C30_Q12CD",
            "C30_Q13",
            "C30_Q13CD",
            "C30_Q14",
            "C30_Q14CD",
            "C30_Q15",
            "C30_Q15CD",
            "C30_Q16",
            "C30_Q16CD",
            "C30_Q17",
            "C30_Q17CD",
            "C30_Q18",
            "C30_Q18CD",
            "C30_Q19",
            "C30_Q19CD",
            "C30_Q20",
            "C30_Q20CD",
            "C30_Q21",
            "C30_Q21CD",
            "C30_Q22",
            "C30_Q22CD",
            "C30_Q23",
            "C30_Q23CD",
            "C30_Q24",
            "C30_Q24CD",
            "C30_Q25",
            "C30_Q25CD",
            "C30_Q26",
            "C30_Q26CD",
            "C30_Q27",
            "C30_Q27CD",
            "C30_Q28",
            "C30_Q28CD",
            "C30_Q29",
            "C30_Q29CD",
            "C30_Q30",
            "C30_Q30CD",
        ]
    )

    def populate_config(self):
        """Convert column definitions to list of SheetConfigs"""
        return [SheetConfig(key=field_name.upper(), usecols=columns) for field_name, columns in asdict(self).items()]


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

        for config in self.ecrf_config.configs:
            if config.key not in excel_file.sheet_names:
                logger.warning(f"Sheet '{config.key}' not found in Excel file")
                continue

            try:
                df = pd.read_excel(
                    excel_file,
                    sheet_name=config.key,
                    usecols=config.usecols,
                    # if formatting issues, test: engine="calamine"
                )
                self.ecrf_config.add_data(SheetData(key=config.key, data=df))
                logger.info(f"Loaded sheet: {config.key}")

            except Exception as e:
                logger.error(f"Failed to load sheet '{config.key}': {e}")

    def _load_csv(self) -> None:
        """Load data from CSV files based on configs"""
        for config in self.ecrf_config.configs:
            # look for files like: prefix_123_COH.csv
            matching_files = [
                file
                for file in self.input_path.iterdir()
                if file.is_file() and file.suffix.lower() == ".csv" and config.key == file.stem.split("_")[-1]
            ]

            if not matching_files:
                logger.warning(f"No CSV file found for key: {config.key}")
                continue

            file_path = matching_files[0]
            try:
                # read headers first to check columns
                actual_cols = pd.read_csv(file_path, nrows=0, skiprows=[0]).columns.tolist()
                col_mapping = {col.upper(): col for col in actual_cols}

                # map requested columns to actual columns in case-insensitive manner
                requested_cols = [col_mapping.get(col.upper()) for col in config.usecols]

                # log missing columns
                if None in requested_cols:
                    missing = [col for col in config.usecols if col.upper() not in col_mapping]
                    logger.error(f"Missing columns in {file_path.name}: {missing}")
                    continue

                # read data with mapped columns
                df = pd.read_csv(file_path, usecols=requested_cols, skiprows=[0])
                df.columns = config.usecols  # and normalize column names

                self.ecrf_config.add_data(SheetData(key=config.key, data=df))
                logger.info(f"Loaded CSV: {file_path.name}")

            except Exception as e:
                logger.error(f"Failed to load '{file_path}': {e}")


class ImpressProcessor:
    def __init__(self, ecrf_config: EcrfConfig):
        if not ecrf_config.is_data_loaded:
            raise ValueError("Cannot process EcrfConfig with no data loaded")
        self.ecrf_config = ecrf_config
        self.valid_subjects = None

    def process(self) -> EcrfConfig:
        """Process all sheets in place and return updated config"""
        # process COH first to get valid subjects
        self._process_coh()
        self._process_ecog()

        # process remaining sheets
        for sheet_data in self.ecrf_config.get_all_data():
            if sheet_data.key != "COH":
                logger.info(f"Processing {sheet_data.key} data...")
                # and filter by valid SubjectId
                self._filter_valid_subjects(sheet_data)

        return self.ecrf_config

    def _filter_valid_subjects(self, sheet_data: SheetData) -> None:
        """Filter dataframe to only include valid subjects"""
        if self.valid_subjects is None:
            raise ValueError("Valid subjects not set - process COH first")

        original_count = len(sheet_data.data)

        sheet_data.data = sheet_data.data[sheet_data.data["SubjectId"].isin(self.valid_subjects)]

        filtered_count = len(sheet_data.data)
        if filtered_count < original_count:
            logger.info(
                f"Filtered {original_count - filtered_count} rows from {sheet_data.key} that didn't match valid SubjectIds"
            )

    def _process_coh(self):
        """Process COH data and establish valid subjects"""
        coh = self.ecrf_config.get_sheet_data("COH")

        # drop rows where COHORTNAME is empty string, NaN, or None (i.e. patient not in a cohort)
        coh.data = coh.data[
            (coh.data["COHORTNAME"].notna()) & (coh.data["COHORTNAME"] != "") & (coh.data["COHORTNAME"].str.strip() != "")
        ]

        self.valid_subjects = set(coh.data["SubjectId"])

    def _process_ecog(self):
        """Process ECOG data - filter for baseline (V00) assessments only"""
        ecog = self.ecrf_config.get_sheet_data("ECOG")

        # filter for baseline (V00) assessments only
        original_count = len(ecog.data)
        ecog.data = ecog.data[ecog.data["EventId"] == "V00"]
        filtered_count = len(ecog.data)

        # drop EventId column since we only have V00 events now
        ecog.data = ecog.data.drop(columns=["EventId"])

        logger.info(f"Filtered {original_count - filtered_count} non-baseline ECOG assessments")


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

            # TODO rename? maybe not as it leads to inconsistencies in naming
            cols_to_rename = [col for col in df.columns if col != "SubjectId"]
            df = df.rename(columns={col: f"{sheet_data.key}_{col}" for col in cols_to_rename})

            processed_dfs.append(df)
            logger.info(f"Processed {sheet_data.key}: {len(df)} rows")

        # TODO add metadata? use class
        # combined_df["ProcessingTimestamp"] = pd.Timestamp.now()

        # combine all dataframes
        combined_df = pd.concat(processed_dfs, axis=0, ignore_index=True)

        # sort by SubjectId
        combined_df = combined_df.sort_values("SubjectId").reset_index(drop=True)

        logger.info(
            f"Combination complete. Final dataset has {len(combined_df)} rows "
            f"for {combined_df['SubjectId'].nunique()} unique subjects"
        )

        return combined_df

    def get_summary(self, df: pd.DataFrame) -> dict:
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


class Output:
    """Handles writing of processed dataframes to various output formats."""

    SUPPORTED_FORMATS: Set[str] = {"csv", "txt"}

    def __init__(self, output_path: Path, data: pd.DataFrame, format: str = "csv"):
        self.output_path = output_path
        self.data = data
        self.format = format.lower()  # normalize format string

    def write_output(self) -> None:
        """Write the dataframe to the specified output format."""
        try:
            self._validate_format()
            self._validate_path()

            logger.info(f"Writing output to {self.output_path} in {self.format} format...")

            if self.format == "csv":
                self._write_csv()
            elif self.format == "txt":
                self._write_txt()

            logger.info(f"Successfully wrote {len(self.data)} rows to {self.output_path}")

        except Exception as e:
            logger.error(f"Failed to write output: {str(e)}")
            raise

    def _write_csv(self) -> None:
        """Write data to CSV file."""
        try:
            self.data.to_csv(
                self.output_path,
                index=False,  # Don't write row numbers
                na_rep="",  # Empty string for missing values
                date_format="%Y-%m-%d",  # ISO format for dates
            )
        except Exception as e:
            logger.error(f"Error writing CSV file: {str(e)}")
            raise

    def _write_txt(self) -> None:
        """Write data to tab-separated text file."""
        try:
            self.data.to_csv(
                self.output_path,
                sep="\t",  # Tab separator
                index=False,  # Don't write row numbers
                na_rep="",  # Empty string for missing values
                date_format="%Y-%m-%d",  # ISO format for dates
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
            logger.warning(
                f"Output file extension '{self.output_path.suffix}' doesn't match format '{self.format}'. "
                f"Expected extension: '{expected_extension}'"
            )


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Process eCRF data and merge into a single CSV.")
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=True,
        help="Path to the eCRF input data (Excel file or CSV directory).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Path to save the output CSV file.",
    )
    return parser.parse_args()


def validate_paths(input_path: Path, output_path: Path) -> None:
    """Validate input and output paths."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    if not output_path.parent.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output_path.parent}")


def main(
    input_path: Path,
    output_path: Path,
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

        # create column configuration and populate SheetConfigs
        logger.info("Initializing column configurations...")
        impress_cols = ImpressColumns()
        sheet_configs = impress_cols.populate_config()

        # initialize EcrfConfig with the sheet configs
        logger.info("Creating eCRF configuration...")
        ecrf_config = EcrfConfig(configs=sheet_configs, data=None, trial="Impress", source_type="csv")

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

        # write output
        logger.info(f"Writing output to {output_path}...")
        output = Output(output_path=output_path, data=combined_data, format=output_format)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process eCRF data for the IMPRESS trial.")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to input data (Excel file or CSV directory)",
    )
    parser.add_argument("--output", type=Path, required=True, help="Path for output file")
    parser.add_argument("--log-path", type=Path, help="Path for log files (optional)")
    parser.add_argument(
        "--format",
        choices=["csv", "txt"],
        default="csv",
        help="Output format (default: csv)",
    )

    args = parser.parse_args()

    main(
        input_path=args.input,
        output_path=args.output,
        log_path=args.log_path,
        output_format=args.format,
    )
