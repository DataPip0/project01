# types.py
from __future__ import annotations
from typing import Protocol, Iterable, Dict, Any, Callable, Sequence, List, Optional
import pandas as pd
from dataclasses import dataclass, field, asdict
from datetime import datetime

# ---------- Record model (captures extras) ----------
@dataclass(frozen=True)
class Event:
    journey_id: str
    # sub_process: Optional[str] = None
    # step_name: Optional[str] = None
    # event_ts: Optional[datetime] = None
    # stage_start_ts: Optional[datetime] = None
    # stage_end_ts: Optional[datetime] = None
    # status_after: Optional[str] = None
    # performed_by: Optional[str] = None
    # risk_grade: Optional[str] = None
    # uw_decision: Optional[str] = None
    # issue_flag: Optional[str] = None
    # issue_code: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "Event":
        # map known fields; bucket everything else into .extra
        known = {f for f in cls.__dataclass_fields__.keys() if f != "extra"}  # type: ignore[attr-defined]
        core = {k: row.get(k) for k in known if k in row}
        extra = {k: v for k, v in row.items() if k not in known}

        # light datetime coercion
        def to_dt(x):
            try:
                return pd.to_datetime(x, errors="coerce").to_pydatetime() if x is not None else None
            except Exception:
                return None

        return cls(**core, extra=extra)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# Protocols for plug points
class Collector(Protocol):
    def __call__(self, **params) -> Iterable[Dict[str, Any]]: ...

Mapper = Callable[[pd.DataFrame], pd.DataFrame]  # df -> df
DatasetStep = Callable[[pd.DataFrame], pd.DataFrame]  # df -> df
RowStep     = Callable[[Event], Event]                # record -> record
Classifier  = Callable[[Event], Any]                  # record -> label/value
