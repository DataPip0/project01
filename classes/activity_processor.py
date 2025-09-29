import pandas as pd
from datetime import datetime
from ..core.logger import get_logger
logger = get_logger(__name__)

class ActivityProcessor:
    
    def __init__(self, events: dict):
        
        # list out all of the attributes needed from the events list.
        self.events = events
        
        # Create a class workflow that goes through the blow steps.
        
        # STEP 1:
        #     - Does the jorueny, sub-process, step exist? Extract the latest available information and create self. records for the information in dict format.
        # - is it the end of the previous stage, sub-process, journey?
        #     - If it doesnt exist or the previous record was the last: create it and add to the database.
        # Step 2:
        #     - Are there any issues
        # - Update the databases with any information on the master or event tables that capture issue information
        # Step 3:
        #     - What is the status of the step, sub-process, journey?
        # - update the databses with status information on each of the master and event tables.
        # 
        # Step 4:
        #     - Is is the end of the step, sub-process, journey?
        # - Add a field to state the end of the step, sub-process, journey 
        # - Update TATs, Ages, end dates on master tables.