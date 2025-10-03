from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Sequence, Protocol
from datetime import datetime
import pandas as pd
from .types import Event


# ---------- Protocols (contracts) ----------
class Mapper(Protocol):
    """Maps external/raw column names/values to internal ones."""
    def map_df(self, df: pd.DataFrame) -> pd.DataFrame: ...

class Standardiser(Protocol):
    """Applies schema normalisation/casting/reshaping after mapping."""
    def standardise(self, df: pd.DataFrame) -> pd.DataFrame: ...

class DQSpec(Protocol):
    """Provides dataset-level and row-level quality steps."""
    def dataset_steps(self) -> Sequence[Callable[[pd.DataFrame], pd.DataFrame]]: ...
    def row_steps(self) -> Sequence[Callable[[Event], Event]]: ...

class Classifier(Protocol):
    """Per-record classifier/derivation."""
    def __call__(self, ev: Event, **kwargs) -> Any: ...