import openpyxl as px # type: ignore 
from dataclasses import dataclass, asdict
from pydantic import BaseModel
import pandas as pd # type: ignore
from pathlib import Path 
from typing import Optional, List, Dict
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

# TODO Fix stubs for pandas and openpyxl 



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

    # TODO go through marias updated list and add all that is missing 


    coh: List[str] = ["SubjectId", "COHORTNAME", "ICD10COD","ICD10DES", "COHALLO1"] # flat 
    ecog: List[str] = ["SubjectId", "EventId", "ECOGS", "ECOGSCD"] # nested, but not after extracting V00 only 
    dm: List[str] = ["SubjectId", "BRTHDAT", "SEX", "SEXCD"] # flat 
    ct: List[str] = ["SubjectId", "CTTYPE", "CTTYPECD", "CTTYPESP", "CTSTDAT", "CTSPID", "CTENDAT", "SQCTYN", "SQCTYNCD", "CTSTDAT"] # nested, missing dates means ongoing 
    eos: List[str] = ["SubjectId", "DEATHDTC", "EOSDAT"] # flat 
    fu: List[str] = ["SubjectId", "FUPDEDAT", "FUPALDAT", "FUPDEDAT", "FUPSSTCD", "FUPSST", "FUPSSTCD"] # nested 
    tr: List[str] = ["SubjectId", "TRTNO", "TRC1_DT", "TRCNO1", "TRIVDS1", "TRIVU1", "TRIVDELYN1", "TRDSDEL1"] # nested, but we only want DoD which is one per patient 
    eot: List[str] = ["SubjectId", "EventDate", "EOTPROGDTC", "EOTDAT", "EOTREOTCD", "EOTREOT"] # flat 
    cm: List[str] = ["SubjectId", "CMTRT", "CMMHYN", "CMSTDAT", "CMONGO", "CMENDAT", "CMAEYN", "CMAENO"] # nested
    ae: List[str] = ["SubjectId", "AETOXGRECD", "AECTCAET", "AESTDAT", "AEENDAT", "AEOUT", # nested 
                     "AESERCD", "AETRT1", "AEREL1", "AETRT2", "AEREL2", "SAEEXP1", "AETRTMM1", "SAEEXP2", "AETRTMM2"] 
    vi: List[str] = ["SubjectId", "VITUMA", "VITUMA_2", "VITUMACD", "VITUMA__2CD"] # nested, but I think we only grab V00VI row so flat (same check as ecog)
    ra: List[str] = ["SubjectId", "EventDate", "RARECBAS", "RARECCUR", "RARECNAD", "RABASECH", "RATIMRES", "RARECCH", "RAiMOD", "RNALBASE", "RNALBASECD"] # nested time-series (extract all?)
    rnrsp: List[str] = ["SubjectId", "EventDate", "TERNTBAS", "TERNTB", "TERNAD", "TERNCFB", "TERNCFN", "RNRSPCL" "RNRSPLC", "RNRSPCLCD"] # nested, same as ra: extract all or only w16 and eot? 
    lugrsp: List[str] = ["SubjectId", "EventDate", "LUGOVRL"] # same as ra and rnrsp: extract all time points? currently empty so doesn't matter yet 
    emlrsp: List[str] = ["SubjectId", "EventDate", "EMLRESP", "RESPEV"] # same as all tumor assessments: what time points to extraxct? 
    br: List[str] = ["SubjectId", "BRRESP", "BRRESPCD", "BRCPRDAT", "BRPDDAT"] # should not be nested but is: do we want EOT or EOS clinical response? and date? 
    resp: List[str] = ["SubjectId", "EventName", "RESPDAT", "RESPDATCD", "RESPEV"]
    eqd5: List[str] = ["SubjectId", "EventName", "EventDate", "EQ5D1", "EQ5D2", "EQ5D3", "EQ5D4", "EQ5D5"] # nested, what time-points? 
    c30: List[str] = ["SubjectId", "EventName", "EventDate", "C30_Q1", "C30_Q1CD", "C30_Q2", "C30_Q2CD", "C30_Q3", "C30_Q3CD", "C30_Q4", "C30_Q4CD", "C30_Q5", 
                      "C30_Q5CD", "C30_Q6", "C30_Q6CD", "C30_Q7", "C30_Q7CD", "C30_Q8", "C30_Q8CD", "C30_Q9", "C30_Q9CD", "C30_Q10", "C30_Q10CD", 
                      "C30_Q11", "C30_Q11CD", "C30_Q12", "C30_Q12CD", "C30_Q13", "C30_Q13CD", "C30_Q14", "C30_Q14CD", "C30_Q15", "C30_Q15CD", "C30_Q16", 
                      "C30_Q16CD", "C30_Q17", "C30_Q17CD", "C30_Q18", "C30_Q18CD", "C30_Q19", "C30_Q19CD", "C30_Q20", "C30_Q20CD", "C30_Q21", "C30_Q21CD", 
                      "C30_Q22", "C30_Q22CD", "C30_Q23", "C30_Q23CD","C30_Q24", "C30_Q24CD", "C30_Q25", "C30_Q25CD", "C30_Q26", "C30_Q26CD", "C30_Q27", 
                      "C30_Q27CD","C30_Q28", "C30_Q28CD", "C30_Q29", "C30_Q29CD", "C30_Q30", "C30_Q30CD"]
    
    def populate_config(self): 
        """Convert column definitions to list of SheetConfigs"""
        return [
            SheetConfig(key=field_name.upper(), usecols=columns)
            for field_name, columns in asdict(self).items()
        ]
    

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
        # process COH first to get valid subjects
        self._process_coh()
        
        # process remaining sheets
        for sheet_data in self.ecrf_config.get_all_data():
            if sheet_data.key != "COH":
                logger.info(f"Processing {sheet_data.key} data...")
                # first filter by valid subjects
                self._filter_valid_subjects(sheet_data)
                
        return self.ecrf_config
    
    def _filter_valid_subjects(self, sheet_data: SheetData) -> None:
        """Filter dataframe to only include valid subjects"""
        if self.valid_subjects is None:
            raise ValueError("Valid subjects not set - process COH first")
        
        sheet_data.data = sheet_data.data[
            sheet_data.data["SubjectId"].isin(self.valid_subjects)
        ]

    def _process_coh(self):
        """Process COH data and establish valid subjects"""
        coh = self.ecrf_config.get_sheet_data("COH")
            
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
        ecog = self.ecrf_config.get_sheet_data("ECOG")    
        ecog.data = ecog.data[ecog.data["SubjectId"].isin(self.valid_subjects)]
        
        # filter for V00 events and drop the EventId column
        ecog.data = ecog.data[ecog.data["EventId"] == "V00"]
        ecog.data = ecog.data.drop(columns=["EventId"])

    
class DataMerger:
    def __init__(self, ecrf_config: EcrfConfig):
        self.ecrf_config = ecrf_config

    def merge(self) -> pd.DataFrame:
        """Merge all processed sheets into one DataFrame"""
        # start with COH
        coh = self.ecrf_config.get_sheet_data("COH")
        merged_df = coh.data.copy()

        # and merge with other dataframes
        for sheet_data in self.ecrf_config.get_all_data():
            if sheet_data.key != "COH":
                merged_df = merged_df.merge(
                    sheet_data.data,
                    on="SubjectId",
                    how="left"
                )

        return merged_df
    
    
def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Process eCRF data and merge into a single CSV.")
    parser.add_argument(
        "--input", "-i",
        type=str,
        required=True,
        help="Path to the eCRF input data (Excel file or CSV directory).",
    )
    parser.add_argument(
            "--output", "-o",
            type=str,
            required=True,
            help="Path to save the output CSV file.",
        )
    return parser.parse_args()


def main(input_path: Path, output_path: Path):
    """
    Main function to process eCRF data for the IMPRESS trial and write the merged DataFrame to a CSV file.

    Args:
        input_path (Path): Path to the input data (Excel file or CSV directory).
        output_path (Pah): Path to save the output CSV fil.
    """
    try:
        # create column configuration and populate SheetConfigs
        impress_cols = ImpressColumns()
        sheet_configs = impress_cols.populate_config()
        
        # initialize EcrfConfig with the sheet configs
        ecrf_config = EcrfConfig(configs=sheet_configs, data=None, trial="Impress", source_type="csv")
        
        # use InputResolver to load data based on configs
        resolver = InputResolver(input_path=input_path, ecrf_config=ecrf_config)
        ecrf_config = resolver.resolve()  

        # merge data 

        # then write to output path 
        
    except Exception as e:
        logger.error(f"Error occurred during processing: {e}")
        raise


if __name__=="__main__": 
    args = parse_arguments()
    input_path = Path(args.input)
    output_path = Path(args.output)
    main(input_path, output_path)
