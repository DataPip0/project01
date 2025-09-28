import pandas as pd
from datetime import datetime
from ..logger import get_logger
logger = get_logger(__name__)

class ActivityProcessor:
    
    def __init__(self, activitites, journey_db):
        
        self.df = df.copy()
        self.stage_master = pd.DataFrame(); self.application_master = pd.DataFrame()
        self.build_stage_master(); self.build_application_master()
    
