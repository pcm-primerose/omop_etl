import pandas as pd
import openpyxl as px
from dataclasses import dataclass
from pydantic import BaseModel
from pathlib import Path 
from typing import Optional, List
import logging 

# configure logger 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


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

@dataclass
class Ecog: 
    score: int # ECOG: ECOGS
    description: str # ECOG: ECOGSCD

    # TODO only take rows where EventId = V00 (first treatment, only baseline assessment)

@dataclass
class MedicalHistory: 
    treatment_type: str # CT: CTTYPE
    treatment_type_code: int # CT: CTTYPECD
    treatment_specification: str # CT: CTTYPESP
    treatment_start_date: str # CT: CTSTDAT
    treatment_end_date: str # CT: CTENDAT 
    previous_treatment_lines: int # CT: CTSPID 

    # TODO only need: Which treatment lines were undertaken (Yes/No) and dates (YYYY-MM-DD)
    # described as: Treatment lines prior to inclusion in treatment phase, but does that include all these vars? 

@dataclass
class PreviousTreatmentLines: 
    treatment: str 
    end_date: str 

    # TODO Which treatment lines were undertaken (Yes/No) and dates (YYYY-MM-DD)
    # combine with MedicalHistory? 

@dataclass 
class Cycle: 
    start: str # TR: TRC1_DT (date, first day of cycle treatment)
    end: str # EOT:	either EventDate, EOTDAT or EOTPROGDTC (last day of cycle treatment)
    dose_delivered: int # TR: TRIVDS1, TRIVU1, TRIVDELYN1, TRDSDEL1

@dataclass 
class Treatment: 
    start: str # need to calculate from Cycle.start date, first dose in first cycle
    cycles: List[Cycle] 
    last_cycle_start: str # need to calculate from Cycle.start date (first dat in last cycle)
    last_cycle_end: str # need to calculate from Cycle.end date (last dat in last cycle)
    concomitant_medication: str # CM: CMTRT, CMMHYN, CMSTDAT, CMONGO, CMENDAT
    end: str # date of last dose in the last cycle or timepoint for end of treatment. (YYYY-MM-DD). 

    # How is this different (can be several cycles, so first and last cycle day are across all cycles):
    # Treatment start: First dose in first cycle (YYYY-MM-DD)
    # Treatment start cycle: First day of treatment cycle start (YYYY-MM-DD)

    # also, how is this different (it is, there can be several cycles, same as above): 
    # Treatment end cycle: Date of treatment cycle end (YYYY-MM-DD)
    # Treatment start last cycle: First day in last treatment cycle (YYYY-MM-DD)

@dataclass
class AdverseEvent: 
    ctcae_term: str # AE: AECTCAET (AE term)
    start_date: str # AE: AESTDAT
    end_date: str # AE: AEENDAT
    outcome: str # AE: AEOUT
    management: str # CM: CMAEYN, CMAENO - AE treatment/management | discontinuation/dose reductions (TODO make separate attributes)
    serious_event: str # AE: AESERCD (yes/no - perhaps bool with NaN instead?) 
    treatment_related: str #AE: AETRT1, AEREL1, AETRT2, AEREL2 (yes/no/unknown - perhaps bool with NaN instead?) 
    expectedness: str # SUSAR: speciy which drug - but where is this data?? 

@dataclass 
class AdverseEvents:
    occurance: str # (yes/no - what file and column?)
    worst_ae_grade: int # AE: AETOXGRECD (worst grade experienced by patient (grade 3 or higher))
    number_of_aes: int # total number of AEs experienced for a patient (can just count instances/elements of AdverseEvent class)
    all_events: List[AdverseEvent]

@dataclass
class Patient: 
    cohort_name: str # COH: COHORTNAME (format: Target/Tumor Type/Treatment)
    trial_id: str # manually set 
    patient_id: str # SubjectId in all files/sheets 
    tumor_type: str # COH: ICD10COD
    study_drug_1: str # COH: COHALLO1, COHALLO1__2 (INN format)
    study_drug_2: Optional[str] # COH: COHALLO2, COHALLO2__2 (if applicable)
    biomarker: str # COH: COHTMN 
    age: int # DM: BRTHDAT (age at treatment start)
    sex: str # DM: SEX
    ecog: Ecog # (only at baseline)
    medical_history: MedicalHistory
    previous_treatment_lines: PreviousTreatmentLines
    date_of_death: Optional[int] # EOS/FU: DEATHDTC/FUPDEDAT
    date_lost_to_followup: Optional[str] # What file? If applicable. 
    evaluable_for_efficacy_analysis: str # What file? (yes/no)
    treatment_start_first_dose: str # TR: TRC1_DT
    adverse_events: AdverseEvents
    type_of_tumor_assessment: str # VI:	VITUMA, VITUMA_2 (Type of tumor assessment (RECIST, iRECIST, LUAGNO, RANO, AML)
    tumor_assessment_date: str # RA, RNRSP, LUGRSP, EMLRSP: EventDate (file name determines event type, that is, tumor assessment type)
    baseline_evaluation: str # RA, RNRSP: RARECBAS, TERNTBAS
    change_from_baseline: str # RA, RNRSP: RARECCUR, RARECNAD, RABASECH, RARECCH, TERNTB, TERNAD, TERNCFB 
    # (Sum size of target lesion at visit, % change from baseline, evaluation non-target-lesion, new lesions?) - what to extract? 
    response_assessment: str # RA, RNRSP, LUGRSP, EMLRSP: RATIMRES, RAiMOD, RNRSPCL, LUGOVRL, EMLRESP - need nesting: 
    # Response assessment (target lesion, non-target lesion and over-all). 	Response asseessment RECIST. 
    end_of_treatment_date: str # EOT: EOTPROGDTC, EOTDAT, EventDate
    end_of_treatment_reason: str # EOT:	EOTREOTCD
    best_overall_response: str # BR: BRRESP
    clinical_benefit: str # (Clinical benefit - CR, PR or SD after 16 weeks of treatment.) - what file/column? 
    quality_of_life_assessment: str # EQ5D: EventName, EventDate, EQ5D1, EQ5D2, EQ5D3, EQ5D4, EQ5D5 & C30: EventName, EventDate, C30_Q1 - 30
    # is this needed?
    # Health-related Quality of Life (HrQoL)-questionnaires collected in the trial (QLQ-C30, EQ-5D-5L, WISP and Patient Preference).
    



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

def merge_input_files(file_mappings: List[FileMapping]) -> pd.DataFrame:
    """
    Reads and merges files into a single DataFrame based on predefined logic.
    Use SubjectId from COH sheet as key to extract only patients 

    Args:
        file_mappings (List[FileMapping]): A list of FileMapping objects.

    Returns:
        pd.DataFrame: Merged DataFrame containing all processed data.
    """
    dataframes = []
    mapping_dict = {fm.key: fm.path for fm in file_mappings}

    if "COH" in mapping_dict:
        df_coh = pd.read_csv(mapping_dict["COH"], usecols=["SubjectId", "COHORTNAME", "ICD10COD", "COHALLO1", "COHALLO1__2", 
                                                           "COHALLO2", "COHALLO2__2", "COHTMN"], skiprows=[0])
        df_coh = df_coh[df_coh["COHORTNAME"].notna()] # drop patients not in cohorts
        dataframes.append(df_coh)

    # then use patients in COH as key for other dataframes 
    if "DM" in mapping_dict:
        df_dm = pd.read_csv(mapping_dict["DM"], usecols=["SubjectId", "SEX", "SEXCD"], skiprows=[0])
        dataframes.append(df_dm)

    if "ECOG" in mapping_dict:
        ecog_df = pd.read_csv(mapping_dict["ECOG"], usecols=["SubjectId", "EventId", "ECOGS", "ECOGSCD"], skiprows=[0])
        ecog_df = ecog_df[ecog_df["EventId" == "V00"]] # drop all non-baseline rows 
        ecog_df = ecog_df.drop(["EventId"]) # and remove the eventid col after filtering 
        dataframes.append(ecog_df)

    if "CT" in mapping_dict:
        df_ct = pd.read_csv(mapping_dict["CT"], usecols=["SubjectId", "CTTYPE", "CTTYPECD", "CTTYPESP", "CTSTDAT", "CTSPID"], skiprows=[0])
        dataframes.append(df_ct)

    if "EOS" in mapping_dict: 
        eos_df = pd.read_csv(mapping_dict["EOS"], usecols=["DEATHDTC"], skiprows=[0]])
        dataframes.append(eos_df)

    if "FU" in mapping_dict: 
        fu_df = pd.read_csv(mapping_dict["FU"], usecols=["SubjectId", "FUPDEDAT"], skiprows=[0])
        dataframes.append(fu_df)

    if "TR" in mapping_dict: 
        tr_df = pd.read_csv(mapping_dict["TR"], usecols=["SubjectId", "TRC1_DT", "TRCNO1", "TRC1_DT", "TRIVDS1", "TRIVU1", "TRIVDELYN1", "TRDSDEL1"], skiprows=[0])
        dataframes.append(tr_df)

    if "EOT" in mapping_dict: 
        eot_df = pd.read_csv(mapping_dict["EOT"], usecols=["SubjectId", "EventDate", "EOTPROGDTC", "EOTDAT"], skiprows=[0])
        dataframes.append(eot_df)
    
    if "CM" in mapping_dict: 
        cm_df = pd.read_csv(mapping_dict["CM"], usecols=["SubjectId", "CMTRT", "CMMHYN", "CMSTDAT", "CMONGO", "CMENDAT", "CMAEYN", "CMAENO"], skiprows=[0])
        dataframes.append(cm_df)

    if "AE" in mapping_dict: 
        ae_df = pd.read_csv(mapping_dict=["AE"], usecols=["SubjectId", "AETOXGRECD", "AECTCAET", "AESTDAT", "AEENDAT", "CMAEYN", 
                                                          "CMAENO", "AEOUT", "AESERCD", "AETRT1", "AEREL1", "AETRT2", "AEREL2"], skiprows=[0])
        dataframes.append(ae_df)

    if "VI" in mapping_dict:
        vi_df = pd.read_csv(mapping_dict=["VI"], usecols=["VITUMA", "VITUMA_2"], usecols=[0])
        dataframes.append(vi_df)

    if "RA" in mapping_dict: 
        
    
    , RNRSP, LUGRSP, EMLRSP"

       

    # Merge all processed DataFrames
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

    



def process_excel(excel_file: Path) -> pd.DataFrame: 
    pass 

# then do the processing: 
# 1. read in all sheets, use only relevant cols for all sheets, combine to one  
# 2. use patient id as foreign key 
# 3. look at the data - can we just merge on patient id? 
#    or do we need some structure? 
#    ideally flatten everything 
# 4. write as one csv file on 1753 
# 5. manually make high quality mock data 
# 6. save that and export, upload to github 
# 7. now you can make the ETL MVP 

