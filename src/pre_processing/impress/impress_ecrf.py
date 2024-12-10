import pandas as pd
import openpyxl as px
from dataclasses import dataclass
from pydantic import BaseModel
from pathlib import Path 
from typing import Optional, List, Callable
import logging 

# configure logger 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# NOTE: three different documents that don't always agree on what to extract. 
# approach here is completonist: just extract everything, easy to update/ignore later. 
# NOTE: just keep in one script for now, extract and abstract then dockerize later 


# TODO this'll just be for impress for now (maybe ever)
# so just extract data as is, combine and write as one csv? 
# we assume the disjoint input (csv) from 1753 not the combined format 
# and then we can just run this script on 1753 to get the data we want, encrypt it, export and import to 2828 
# and treat it as any other data source 

# TODO 
# 0. collect all sheets (csv files) and make input method that grabs the correct ones,
#    and reports if some are not found (fatal). This can just return dataframes. 
# - should work on any folder containing the csv files *OR* the given xlsx file, 
# - and return the correct data as dataframes regardless 

# TODO
# problems:
# 1. multiple rows per patient e.g. in ecog, how is this represented in other trials? 
# 2. if i start pre-processing i might as well just harmonize here, but thats not the point
# 3. i guess just leave as is, extracting only needed data 
# 4. but make it as similar to other trials as possible? 

# TODO: 
# go through all attributes and confirm existance in eCRF data 
# and if they are needed or not, write down notes for each on logic and location etc 
# also, can there be several different types of tumor assessments per patient? if so, nest it.

# TODO: 
# make typevar/struct/class to contain custom date format: YYYY-MM-DD
    

class SheetConfig:
    def __init__(self, key: str, usecols: List[str], process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None):
        """
        Config for processing a specific eCRF sheet.

        Args:
            key (str): The file key (e.g., "COH").
            usecols (List[str]): Columns to extract from the file.
            process_func (Optional[Callable]): Custom processing logic for the sheet.
        """
        self.key = key
        self.usecols = usecols
        self.process_func = process_func

    def process(self, file_path: Path) -> pd.DataFrame:
        """
        Reads and processes the file based on the config.

        Args:
            file_path (Path): Path to the file.

        Returns:
            pd.DataFrame: Processed DataFrame.
        """
        df = pd.read_csv(file_path, usecols=self.usecols, skiprows=[0])
        if self.process_func:
            df = self.process_func(df)
        return df
    
def process_ecog(df: pd.DataFrame) -> pd.DataFrame:
    """Filter ECOG data for baseline (EventId = 'V00') and drop the EventId column."""
    df = df[df["EventId"] == "V00"]
    return df.drop(columns=["EventId"])

def process_ra():
    # Sheets: RA, RNRSP, LUGRSP, EMLRS = SubjectId, EventDate
    # but need to store the tumor assessment type as well 
    # easier way to do that is just to populate the 
    pass

    """RA, RNRSP, LUGRSP, EMLRSP	EventDate
    RA, RNRSP	RARECBAS, TERNTBAS
    RA, RNRSP	RARECCUR, RARECNAD, RABASECH, RARECCH,     TERNTB, TERNAD, TERNCFB
    RA, RNRSP, LUGRSP, EMLRSP	RATIMRES, RAiMOD,  LUGOVRL, EMLRESP
    EOT	EOTPROGDTC, EOTDAT, EventDate
    EOT	EOTREOTCD
    BR	BRRESP"""

def process_rnrsp(): 
    pass 

def process_lugrsp():
    pass

def process_emlrsp():
    pass 

sheet_configs = {
    "COH": SheetConfig(
        key="COH",
        usecols=["SubjectId", 
                 "COHORTNAME", 
                 "ICD10COD", 
                 "COHALLO1", 
                 "COHALLO1__2", 
                 "COHALLO2", 
                 "COHALLO2__2", 
                 "COHTMN"] 
                 # if missing use __1 and __2 
    ),
    "ECOG": SheetConfig(
        key="ECOG",
        usecols=["SubjectId", 
                 "EventId", 
                 "ECOGS", 
                 "ECOGSCD"],
        process_func=process_ecog
    ),
    "DM": SheetConfig(
        key="DM",
        usecols=["SubjectId", 
                 "SEX", 
                 "SEXCD"]
    ),
    "CT": SheetConfig(
         key="DM",
         usecols=["SubjectId", 
                  "CTTYPE", 
                  "CTTYPECD", 
                  "CTTYPESP", 
                  "CTSTDAT", 
                  "CTSPID", 
                  "CTENDAT"]
    ),
    "EOS": SheetConfig(
         key="EOS",
         usecols=["SubjectId", 
                  "DEATHDTC"]
    ),
    "FU": SheetConfig(
        key="FU",
        usecols=["SubjectId", 
                 "FUPDEDAT"]
    ),
    "TR": SheetConfig(
        key="TR",
        usecols=["SubjectId", 
                 "TRC1_DT", 
                 "TRCNO1", 
                 "TRIVDS1", 
                 "TRIVU1", 
                 "TRIVDELYN1", 
                 "TRDSDEL1"]
    ),
    "EOT": SheetConfig(
        key="EOT",
        usecols=["SubjectId", 
                 "EventDate", 
                 "EOTPROGDTC", 
                 "EOTDAT", 
                 "EOTREOTCD"]
    ),
    "CM": SheetConfig(
        key="CM",
        usecols=["SubjectId", 
                 "CMTRT", 
                 "CMMHYN", 
                 "CMSTDAT", 
                 "CMONGO", 
                 "CMENDAT", 
                 "CMAEYN", 
                 "CMAENO"]
    ),  
    "AE": SheetConfig(
        key="AE", 
        usecols=["SubjectId", 
                 "AETOXGRECD", 
                 "AECTCAET", 
                 "AESTDAT", 
                 "AEENDAT", 
                 "CMAEYN", 
                 "CMAENO", 
                 "AEOUT", 
                 "AESERCD", 
                 "AETRT1", 
                 "AEREL1", 
                 "AETRT2", 
                 "AEREL2",
                 "SAEEXP1", 
                 "AETRTMM1", 
                 "SAEEXP2", 
                 "AETRTMM2"]
    ),
    "VI": SheetConfig(
        key="VI",
        usecols=["SubjectId", 
                 "VITUMA", 
                 "VITUMA_2"]
    ), 
    "RA": SheetConfig( 
        key="RA",
        usecols=["SubjectId", 
                 "EventDate", 
                 "RARECBAS", 
                 "RARECCUR", 
                 "RARECNAD", 
                 "RABASECH", 
                 "RATIMRES", 
                 "RARECCH", 
                 "RAiMOD"],
        process_func=process_ra
    ),
    "RNRSP": SheetConfig(
        key="RNRSP", 
        usecols=["SubjectId", 
                 "EventDate", 
                 "TERNTBAS", 
                 "TERNTB", 
                 "TERNAD", 
                 "TERNCFB", 
                 "RNRSPCL"],
        process_func=process_rnrsp
    ),
    "LUGRSP": SheetConfig(
        key="LUGRSP",
        usecols=["SubjectId", 
                 "EventDate", 
                 "LUGOVRL"],
        process_func=process_lugrsp
    ),
    "EMLRSP": SheetConfig(
        key="EMLRSP",
        usecols=["SubjectId", 
                 "EventDate", 
                 "EMLRESP"],
        process_func=process_emlrsp
    ),
    "BR": SheetConfig(
        key="BR",
        usecols=["SubjectId", 
                 "BRRESP"]
    ), 
    "C30": SheetConfig(
        key="C30",
        usecols=["SubjectId", 
                 "EventDate", 
                 "C30_Q1", "C30_Q1CD", 
                 "C30_Q2", "C30_Q2CD", 
                 "C30_Q3", "C30_Q3CD", 
                 "C30_Q4", "C30_Q4CD", 
                 "C30_Q5", "C30_Q5CD"
                 "C30_Q6", "C30_Q6CD", 
                 "C30_Q7", "C30_Q7CD", 
                 "C30_Q8", "C30_Q8CD", 
                 "C30_Q9", "C30_Q9CD", 
                 "C30_Q10", "C30_Q10CD", 
                 "C30_Q11", "C30_Q11CD",
                 "C30_Q12", "C30_Q12CD", 
                 "C30_Q13", "C30_Q13CD", 
                 "C30_Q14", "C30_Q14CD", 
                 "C30_Q15", "C30_Q15CD", 
                 "C30_Q16", "C30_Q16CD", 
                 "C30_Q17", "C30_Q17CD",
                 "C30_Q18", "C30_Q18CD", 
                 "C30_Q19", "C30_Q19CD", 
                 "C30_Q20", "C30_Q20CD", 
                 "C30_Q21", "C30_Q21CD", 
                 "C30_Q22", "C30_Q22CD", 
                 "C30_Q23", "C30_Q23CD",
                 "C30_Q24", "C30_Q24CD", 
                 "C30_Q25", "C30_Q25CD", 
                 "C30_Q26", "C30_Q26CD", 
                 "C30_Q27", "C30_Q27CD",
                 "C30_Q28", "C30_Q28CD", 
                 "C30_Q29", "C30_Q29CD",
                 "C30_Q30", "C30_Q30CD"]
    ),
    "EQ5D": SheetConfig(
        key="EQ5D",
        usecols=["SubjectId",
                 "EventName",
                 "EventDate",
                 "EQ5D1",
                 "EQ5D2",
                 "EQ5D3", 
                 "EQ5D4",
                 "EQ5D5"]
    )
}











def process_and_merge_files(csv_dir: Path, sheet_configs: dict) -> pd.DataFrame:
    """
    Processes and merges all eCRF sheets based on the given configs.

    Args:
        csv_dir (Path): Directory containing the CSV files.
        sheet_configs (dict): Dictionary of sheet configurations.

    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    dataframes = {}
    for key, config in sheet_configs.items():
        file_path = csv_dir / f"{key}.csv"
        if file_path.exists():
            df = config.process(file_path)
            dataframes[key] = df
        else:
            print(f"Warning: File for key '{key}' not found.")

    # Start merging with the "COH" sheet (assume it's the primary sheet)
    merged_df = dataframes.pop("COH", None)
    if merged_df is None:
        raise ValueError("COH sheet is required for merging but was not found.")

    for key, df in dataframes.items():
        merged_df = merged_df.merge(df, on="SubjectId", how="left")

    return merged_df


# first part, format specific: 
def resolve_input(input_path: Path) -> pd.DataFrame:
    """
    Resolves input and returns a pandas DataFrame for either Excel or CSV input of eCRF data.
    
    Args:
        input_path (Path): Path to the eCRF data, either as Excel input file or a directory containing the CSV files.
    
    Returns:
        pd.DataFrame: Combined data from Excel or CSV input.
    """

    if input_path.is_file():
        result = process_excel(excel_file=input_path)
        logger.info(f"Found Excel input for eCRF data in {input_path}")

    elif input_path.is_dir():
        result = process_csv(csv_dir=input_path)
        logger.info(f"Found CSV input for eCRF data in {input_path}")

    else:
        raise ValueError(f"Input path {input_path} is neither a file nor a directory.")

    return result

@dataclass
class FileMapping:
    key: str
    path: Path

def find_input_files(csv_dir: Path, sheet_keys: List[str]) -> List[FileMapping]:
    """
    Finds all relevant CSV files based on the sheet keys.

    Args:
        csv_dir (Path): Directory containing eCRF CSV files.
        sheet_keys (List[str]): List of keys for which files should be kept.

    Returns:
        List[FileMapping]: A list of FileMapping objects.
    """
    file_mappings = [
        FileMapping(key=file.name.split("_")[-1].split(".")[0], path=file)
        for file in csv_dir.iterdir()
        if file.is_file() and file.name.split("_")[-1].split(".")[0] in sheet_keys
    ]

    # log missing keys
    found_keys = {fm.key for fm in file_mappings}
    for key in sheet_keys:
        if key not in found_keys:
            logger.info(f"Missing input file for key: {key}")

    return file_mappings

