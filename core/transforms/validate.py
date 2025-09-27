import os, pandas as pd
from ..logger import get_logger
logger = get_logger(__name__)
def validate_outputs(stage,app,g_stage,g_app):
    if not (os.path.exists(g_stage) and os.path.exists(g_app)):
        logger.warning("No golden files."); return
    gs=pd.read_csv(g_stage,parse_dates=["Stage_Start","Stage_End"])
    ga=pd.read_csv(g_app,parse_dates=["Application_Start","Application_End"])
    if not stage.empty and not app.empty:
        diff_s=stage.compare(gs,keep_shape=True,keep_equal=False)
        diff_a=app.compare(ga,keep_shape=True,keep_equal=False)
        if not diff_s.empty or not diff_a.empty: logger.warning("Validation diffs detected.")
        else: logger.info("Validation passed.")
