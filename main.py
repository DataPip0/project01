from .data.database_setup import db_setup
from .classes.activity_classifier import ActivityClassifier
from .classes.activity_processor import ActivityProcessor

from core import config
from core.transforms.ingest import ingest_from_csv
from core.quality.dq_checks import run_data_quality_checks

from core.transforms.validate import validate_outputs
from core.io.writers import write_csv
from core.logger import get_logger

logger=get_logger(__name__)



def run():
    # set up post-gres databases or dfs (short term for testing)
    journey_db, sub_journey_db, step_db, event_db = db_setup()

    # get latest transactions
    latest_events = ingest_from_csv(config.RAW_DATA_PATH)
    
    # Classifier
    latest_events = ActivityClassifier(latest_events)
    
    # data quality checks
    latest_events = run_data_quality_checks(latest_events)
    
    # run process
    ActivityProcessor(latest_events, event_db, journey_db, sub_journey_db, step_db)
    
    # Add validation to a setparate step
    validate_outputs(b.stage_master,b.application_master,config.GOLDEN_STAGE_PATH,config.GOLDEN_APP_PATH)
    
    # Additional workflow
    
if __name__=="__main__": run()