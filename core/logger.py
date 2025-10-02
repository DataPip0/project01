import logging
from pathlib import Path

def get_logger(name: str="Project01"):
    
    logger = logging.getLogger(name)
    if logger.handlers: return logger
    Path("logs").mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    fh = logging.FileHandler("logs/process_log.log"); fh.setFormatter(fmt)
    ch = logging.StreamHandler(); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)
    return logger