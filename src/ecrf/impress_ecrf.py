import pandas as pd
import openpyxl as px
from dataclasses import dataclass
from pydantic import BaseModel
from pathlib import Path 

# TODO this'll just be for impress for now (maybe ever)
# so just extract data as is, combine and write as one csv? 
# we assume the disjoint input (csv) from 1753 not the combined format 
# and then we can just run this script on 1753 to get the data we want, encrypt it, export and import to 2828 
# and treat it as any other data source 

class ExtractECRF:
    # entry point for all ecrf processing 
    # later have some entry point that calls the correct classes etc accoring to what trial etc
    pass


class InputController:

    def __init__(self, ecrf_path: Path): 
        self.ecrf_path = ecrf_path

    def resolve_input(self): 
        # given a file or dir
        # return correct data as validated 
        # dataframe to extraction functions 
        pass 

class ImpressEcrf: 
    # run everything based on config 
    # resolve input, pass config/meta state 
    # and invoke each subclass/method to run extraction
    # and return structured objects 
    pass 

# just easiest to grab everything per sheet at once, but we can keep it as modular as possible 
# so we can start with COH 

def extract_adverse_event_name(input: px.sheet | Path): 
    # not sure if we actually get csv or xlsx yet, but for now need to use csv files from p1753 
    # since nothing is installed yet on p2828
    pass 

def extract_cohort_name(): 
    # do eveything here
    # - find path, open file, extract name, return 
    # or just dependancy inject a dataframe so this works regardless of xlsx or csv 
    # so this will get the correct sheet the data is in (e.g. COH) and then we just assume the correct dat is present
    # extract it and then 

    # drop patients with no cohort status 
    # use patient impress ID as patient ID

