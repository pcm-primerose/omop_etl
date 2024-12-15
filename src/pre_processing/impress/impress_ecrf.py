import openpyxl as px
from dataclasses import dataclass
from pydantic import BaseModel
import pandas as pd
from pathlib import Path 
from typing import Optional, List, Callable, Dict
import logging 
import argparse

# configure logger 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# NOTE: three different documents that don't always agree on what to extract. 
# approach here is completonist: just extract everything, easy to update/ignore downstream. 
# NOTE: idea is to make this configurable and scaleable to minimize time spent adding support 
# for new trials. adding new configs uses a common interface without changing the implementation,
# all that's needed is writing trial-specific pre-processing functions and making the config.  
# NOTE: need to flesh out when (if) we get access to other ecrf systems, in that case: 
# TODO: add support all data in oe file, dockerize this, split into seprate modules, 
# make trial-specific processing modular, use class to get processing logic and apply it per trial, 
# should actually refactor to use dataclasses instead of pandas, would make things much cleaner,
# map sheet/filename to each dataclass, something like this (but then I might as well harmonize!?

# NOTE: Found clinical benefit at week 16, but only over telephone, and not for all patients, but only place where this info is stored: 
# RESP: EventName = Week16, Telephone, RESPEV = Partial Response 



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
    input_path: Optional[Path]

@dataclass 
class EcrfConfig: 
    """Stores all configs with their data"""
    data: List[SheetData]
    configs: List[SheetConfig]
    trial: Optional[str]
    source_type: Optional[str]

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

    def get_data(self, key: str) -> Optional[SheetData]:
        """Get SheetData instance by key"""
        if not self.is_data_loaded:
            raise ValueError("No data has been loaded into the config")
        return next((d for d in self.data if d.key == key), None)

    @property
    def is_data_loaded(self) -> bool:
        """Check if data has been loaded into the config"""
        return self.data is not None and len(self.data) > 0
    

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
                file for file in self.input_path.iterdir()
                if file.is_file() 
                and file.suffix.lower() == ".csv"
                and config.key == file.stem.split("_")[-1]
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
                requested_cols = [
                    col_mapping.get(col.upper()) 
                    for col in config.usecols
                ]
                
                # log missing columns 
                if None in requested_cols:
                    missing = [
                        col for col in config.usecols 
                        if col.upper() not in col_mapping
                    ]
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
        # process COH first to get valid patients placed in cohorts
        self._process_coh()
        
        # and then process remaining data 
        for sheet_data in self.ecrf_config.data:
            if sheet_data.key != "COH":
                logger.info(f"Processing {sheet_data.key} data...")
                process_method = getattr(self, f"_process_{sheet_data.key.lower()}", 
                                      self._default_process)
                process_method(sheet_data)
        
        return self.ecrf_config
    
    def _default_process(self, sheet_data: SheetData):
        """Default processing for sheets without specific processing"""
        sheet_data.data = sheet_data.data[
            sheet_data.data['SubjectId'].isin(self.valid_subjects)
        ]

    def _process_coh(self):
        """Process COH data and establish valid subjects"""
        coh = self.ecrf_config.get_data("COH")
        if coh is None:
            raise ValueError("COH data not found")
            
        # drop rows where COHORTNAME is empty string, NaN, or None (i.e. patient not in cohort)
        coh.data = coh.data[
        (coh.data["COHORTNAME"].notna()) &  
        (coh.data["COHORTNAME"] != "") &    
        (coh.data["COHORTNAME"].str.strip() != "")  
        ]
        
        self.valid_subjects = set(coh.data["SubjectId"])
        self.valid_subjects = set(coh.data["SubjectId"])

    def _process_ecog(self):
        """Process ECOG data"""
        ecog = self.ecrf_config.get_data("ECOG")
        if ecog is None:
            raise ValueError("ECOG data not found")
    
        ecog.data = ecog.data[ecog.data["SubjectId"].isin(self.valid_subjects)]
        
        # filter for V00 events and drop the EventId column
        ecog.data = ecog.data[ecog.data["EventId"] == "V00"]
        ecog.data = ecog.data.drop(columns=["EventId"])

    def _process_ae(self, sheet_data: SheetData):
        """Process AE data"""
        df = sheet_data.data
        df = df[df['SubjectId'].isin(self.valid_subjects)]
        agg_dict = {col: list for col in df.columns if col != "SubjectId"}
        sheet_data.data = df.groupby('SubjectId').agg(agg_dict).reset_index()


def create_impress_config() -> EcrfConfig: 
    configs = [
            SheetConfig(key="COH", 
                        usecols=["SubjectId", "COHORTNAME", "ICD10COD", "COHALLO1", "COHALLO1__2", "COHALLO2", "COHALLO2__2", "COHTMN"]),

            SheetConfig(key="ECOG", 
                        usecols=["SubjectId", "EventId", "ECOGS", "ECOGSCD"]),

            SheetConfig(key="DM", 
                        usecols=["SubjectId", "SEX", "SEXCD"]),

            SheetConfig(key="CT", 
                        usecols=["SubjectId", "CTTYPE", "CTTYPECD", "CTTYPESP", "CTSTDAT", "CTSPID", "CTENDAT"]),

            SheetConfig(key="EOS", 
                        usecols=["SubjectId", "DEATHDTC"]),

            SheetConfig(key="FU", 
                        usecols=["SubjectId", "FUPDEDAT"]),

            SheetConfig(key="TR", 
                        usecols=["SubjectId", "TRC1_DT", "TRCNO1", "TRIVDS1", "TRIVU1", "TRIVDELYN1", "TRDSDEL1"]),

            SheetConfig(key="EOT", 
                            usecols=["SubjectId", "EventDate", "EOTPROGDTC", "EOTDAT", "EOTREOTCD"]),

            SheetConfig(key="CM", 
                            usecols=["SubjectId", "CMTRT", "CMMHYN", "CMSTDAT", "CMONGO", "CMENDAT", "CMAEYN", "CMAENO"]),
            
            SheetConfig(key="AE", 
                        usecols=["SubjectId", "AETOXGRECD", "AECTCAET", "AESTDAT", "AEENDAT", "CMAEYN", "CMAENO", "AEOUT", 
                                 "AESERCD", "AETRT1", "AEREL1", "AETRT2", "AEREL2", "SAEEXP1", "AETRTMM1", "SAEEXP2", "AETRTMM2"]),

            SheetConfig(key="VI",
                        usecols=["SubjectId", "VITUMA", "VITUMA_2"]),

            SheetConfig(key="RA",
                        usecols=["SubjectId", "EventDate", "RARECBAS", "RARECCUR", "RARECNAD", "RABASECH", "RATIMRES", "RARECCH", "RAiMOD"]),

            SheetConfig(key="RNRSP", 
                        usecols=["SubjectId", "EventDate", "TERNTBAS", "TERNTB", "TERNAD", "TERNCFB", "RNRSPCL"]),
            
            SheetConfig(key="LUGRSP", 
                        usecols=["SubjectId", "EventDate", "LUGOVRL"]),

            SheetConfig(key="EMLRSP", 
                        usecols=["SubjectId", "EventDate", "EMLRESP", "RESPEV"]),

            SheetConfig(key="BR", 
                        usecols=["SubjectId", "BRRESP"]),

            SheetConfig(key="EQD5", 
                        usecols=["SubjectId", "EventName", "EventDate", "EQ5D1", "EQ5D2", "EQ5D3", "EQ5D4", "EQ5D5"]),

            SheetConfig(key="C30", 
                        usecols=["SubjectId", "EventDate", "C30_Q1", "C30_Q1CD", "C30_Q2", "C30_Q2CD", "C30_Q3", "C30_Q3CD", "C30_Q4", "C30_Q4CD", "C30_Q5", 
                                 "C30_Q5CD", "C30_Q6", "C30_Q6CD", "C30_Q7", "C30_Q7CD", "C30_Q8", "C30_Q8CD", "C30_Q9", "C30_Q9CD", "C30_Q10", "C30_Q10CD", 
                                 "C30_Q11", "C30_Q11CD", "C30_Q12", "C30_Q12CD", "C30_Q13", "C30_Q13CD", "C30_Q14", "C30_Q14CD", "C30_Q15", "C30_Q15CD", "C30_Q16", 
                                 "C30_Q16CD", "C30_Q17", "C30_Q17CD", "C30_Q18", "C30_Q18CD", "C30_Q19", "C30_Q19CD", "C30_Q20", "C30_Q20CD", "C30_Q21", "C30_Q21CD", 
                                 "C30_Q22", "C30_Q22CD", "C30_Q23", "C30_Q23CD","C30_Q24", "C30_Q24CD", "C30_Q25", "C30_Q25CD", "C30_Q26", "C30_Q26CD", "C30_Q27", 
                                 "C30_Q27CD","C30_Q28", "C30_Q28CD", "C30_Q29", "C30_Q29CD", "C30_Q30", "C30_Q30CD"])
    ]
 
    return EcrfConfig(configs=configs)

def main(input_path: Path, output_path: Path):
    """
    Main function to process eCRF data for the IMPRESS trial and write the merged DataFrame to a CSV file.

    Args:
        input_path (Path): Path to the input data (Excel file or CSV directory).
        output_path (Pah): Path to save the output CSV fil.
    """

    try:
        # create configs
        logger.info("Creating configurations...")
        manager = impress_base_config({}) 
        sheet_configs = manager.get_allconfigs()

        # resolve input with the configs
        logger.info("Resolving input data...")
        resolver = InputResolver(input_path=input_path, sheet_configs=sheet_configs)
        df_dict = resolver.resolve)
        logger.info(f"Loaded {len(df_dict)} DataFrames from input.")

        # configure and process data
        logger.info("Processing data...")
        processor = ProcessImpress(df_dct=df_dict)
        processed_data = processor.process_all_df()
        logger.info(f"Processed data for {len(processed_data)} sheets.")

        # merge dataframes
        logger.info("Merging processed data...")
        merged_df = merge_all_dataframes(procesed_data)
        logger.info(f"Merged DataFrame contains {len(merged_df)} rows and {len(merged_df.columns)} columns.")

        # write Output
        logger.info(f"Writing merged data to {output_path}...")
        merged_df.to_csv(output_path, index=False)
        logger.info("Data successfully written t output file.")

    except Exception as e:
        logger.error(f"Error occurred during processing: {e}")
        raise

def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Process eCRF data and merge into a single CSV.")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to the eCRF input data (Excel file or CSV directory).",
    )
    parser.add_argument(
            "--output",
            type=str,
            required=True,
            help="Path to save the output CSV file.",
        )
    return parser.parse_args()


if __name__=="__main__": 
    args = parse_arguments()
    input_path = Path(args.input)
    output_path = Path(args.output)
    main(input_path, output_path)
