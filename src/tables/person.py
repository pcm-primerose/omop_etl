import sqlalchemy as sa
from ..sql.sql_logic import process_age 

class ProcessPerson: 

    def __init__(self, source_data, output_data) -> None:
        self.source_data = source_data
        self.output_data = output_data

    def run(self) -> None: 
        # run private methods (if any) to process person 
        # or just call sql methods directly
        # log and use sa session 
        pass 