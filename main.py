from data.database_init import init_db
from .classes.activity_classifier import ActivityClassifier
from .classes.activity_processor import ActivityProcessor

from core import config
from core.transforms.ingest import ingest_from_csv
from core.quality.dq_checks import run_data_quality_checks

from core.transforms.validate import validate_outputs
from core.io.writers import write_csv
from core.logger import get_logger
from .data.database_init import init_db

# SQLite file (auto-creates if missing)
# This initiates the databases.
init_db("sqlite:///data/app.db", echo=False)

logger=get_logger(__name__)



def run():

    # get latest transactions - we can optimize this later.
    latest_events = ingest_from_csv(config.RAW_DATA_PATH)
    
    # Classifier
    # latest_events = ActivityClassifier(latest_events) - This is for later.
    
    # data quality checks - we can optimize this later.
    latest_events = run_data_quality_checks(latest_events)
    
    # split the latest events and group them by journey id - this code isnt right, but it gives you an idea.
    journey_events = {k: g for k, g in latest_events.groupby("journey_id")}
    
    # run each group throught the activity processor.
    for k,g in journey_events: 
        ActivityProcessor(k)
    
    # Add validation to a setparate step - this shouldnt be part of your main file, move it into a separate file for validation.
    validate_outputs(b.stage_master,b.application_master,config.GOLDEN_STAGE_PATH,config.GOLDEN_APP_PATH)
    
    # Additional workflow
    
if __name__=="__main__": run()