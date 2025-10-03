from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from datetime import datetime
import pandas as pd
from config.config_loader import load_yaml, deep_merge
from pathlib import Path

from ..types import Event
from ..quality.standardiser import StandardiserPipeline


class EventInput:
    """
    Single entrypoint:
      - Initialize with CSV path or DataFrame.
      - Optionally run data-quality steps.
      - Register and run classifiers by name.
    """

    def __init__(
        self,
        customer_id = "customer_a",
        source = Union[str, Path, pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
        *,
        read_csv_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        
        # load config
        base_cfg = load_yaml("config/base.yaml")
        print(f' yaml fil: {base_cfg}')
        cust_cfg = load_yaml(f"config/{customer_id}.yaml")
        self.final_cfg = deep_merge(base_cfg, cust_cfg)
        
        print(f' yaml fil: {self.final_cfg}')
        
        
        # Convert data into pandas dataframe.
        if isinstance(source, (str, Path)):
            df = pd.read_csv(str(source), **(read_csv_kwargs or {}))
        elif isinstance(source, pd.DataFrame):
            df = source.copy()
        elif isinstance(source, dict):
            df = pd.DataFrame([source])
        elif isinstance(source, list) and (not source or isinstance(source[0], dict)):
            df = pd.DataFrame(source)
        else:
            raise TypeError("source must be CSV path/Path, DataFrame, dict, or list[dict]")

        
        df = StandardiserPipeline(self.final_cfg["data_config"]["standardiser"], df=df).df
        # self._dq_steps: List[DQStep] = dq_steps or []
        # self._classifiers: Dict[str, ClassifierFn] = classifiers or {}

        # results
        self.df = df
        print(f'this is the mapping file df {self.df}')
        type(self.df)
        self.to_records()
        # self.classifications: Dict[str, List[Any]] = {}  # name -> per-record outputs

    # ---------- dataset utilities ----------
    def metrics(self) -> Dict[str, Any]:
        numeric = self.df.select_dtypes(include="number")
        return {
            "rows": int(len(self.df)),
            "cols": list(self.df.columns),
            "nulls": self.df.isna().sum().to_dict(),
            "dtypes": {c: str(t) for c, t in self.df.dtypes.items()},
            "numeric_summary": (numeric.describe().to_dict() if not numeric.empty else {}),
        }

    # ---------- data quality ----------
    # def run_quality(self) -> "EventDataset":
    #     for step in self._dq_steps:
    #         self.df = step(self.df)
    #     return self

    # ---------- record materialization ----------
    def to_records(self) -> "EventInput":
        self.records = [Event.from_dict(row._asdict()) for row in self.df.itertuples(index=False)]
        return self

    # ---------- classification ----------
    # def register_classifier(self, name: str, fn: ClassifierFn) -> None:
    #     self._classifiers[name] = fn

    # def classify(self, name: str, **kwargs) -> List[Any]:
    #     if not self.records:
    #         self.to_records()
    #     fn = self._classifiers.get(name)
    #     if fn is None:
    #         raise KeyError(f"classifier '{name}' not registered")
    #     outputs = [fn(ev, **kwargs) if kwargs else fn(ev) for ev in self.records]
    #     self.classifications[name] = outputs
    #     return outputs
