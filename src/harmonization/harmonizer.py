from pydantic import BaseModel
from pathlib import Path
from dataclasses import dataclass

# have some main entry point to take X extracted ecrf files and standardize, then harmonize by instantiating
# dataclasses mirroring variable list struct and have meta for which trials are where, so we can just call this on all data
# and automate processing

class HarmonizeData():
    # based on meta or config process any file from ecrf extraction,
    # some logic to conver stuff etc
    # and ends by instantiating nested pydantic class
    # which can then be passed
    pass


@dataclass
class Meta:
    trial_name: str
    source_ecrf_file: Path
    number_of_patients: int
    # etc
    pass

class AdverseEvents(BaseModel):
    # look at variable sheet and
    pass

class HarmonizedVariables(BaseModel):
    AEs: AdverseEvents
    # etc
    pass
