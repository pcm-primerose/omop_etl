import pandas as pd
import openpyxl as px
im
from dataclasses import dataclass
from pydantic import BaseModel

class ExtractECRF:
    # later have some entry point that calls the correct classes etc accoring to what trial etc
    pass

def process_impress(csv_mode: bool = True) -> None:
    # call correct functions on impress data
    # make configurable for csv files or for xlsx
    # and pass correct arg to funcs
    # or just wait intil we have python on p28 and use the xlsx eCRF there

def extract_adverse_events(input: px.sheet | Path) ->

