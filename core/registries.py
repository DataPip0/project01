# registries.py
from typing import Dict
from .types import Collector, DatasetStep, RowStep, Classifier

COLLECTORS:   Dict[str, Collector]   = {}
STANDARDIZE:  Dict[str, DatasetStep] = {}
DQ_DATASET:   Dict[str, DatasetStep] = {}
DQ_ROW:       Dict[str, RowStep]     = {}
CLASSIFIERS:  Dict[str, Classifier]  = {}

def register(reg: Dict[str, object], name: str):
    def deco(fn):
        reg[name] = fn
        return fn
    return deco
