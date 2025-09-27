from pathlib import Path
import pandas as pd
from ..logger import get_logger
logger = get_logger(__name__)
def write_csv(df: pd.DataFrame, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path,index=False)
    logger.info(f"Saved {path}")
