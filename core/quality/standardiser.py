# mapping_pipeline.py
from typing import Callable, Sequence, Dict, Any, Iterable, List, Optional,Union
from pathlib import Path
import pandas as pd
import re
import yaml

# ---------- FieldMapper: DataFrame -> DataFrame ----------
FieldMapper = Callable[[pd.DataFrame], pd.DataFrame]


# ---------- Build steps list from a mapping YAML ----------

def steps_from_cfg(cfg: Union[Dict[str, Any], List]) -> List[FieldMapper]:
    steps: List[FieldMapper] = []

    if cfg.get("to_snake_case"):
        steps.append(to_snake_case())

    # Step 2: rename keys to lower case
    rn = cfg.get("rename") or {}
    if rn:
        rn_lc = {k.lower(): v for k, v in rn.items()}
        steps.append(rename(rn_lc))

    cs = cfg.get("cast") or {}
    if cs:
        cs_lc = {k.lower(): v for k, v in cs.items()}
        steps.append(cast(cs_lc))

    vm = cfg.get("value_map") or {}
    for col, mapping in vm.items():
        if mapping:
            steps.append(value_map(col.lower(), mapping))

    req = cfg.get("require") or []
    if req:
        req_lc = [c.lower() for c in req]
        steps.append(require(req_lc))

    return steps

# ---------- Pipeline runner ----------
class StandardiserPipeline:
    def __init__(self, cfg: Dict[str, Any], df: pd.DataFrame):
        
        self.steps = steps_from_cfg(cfg=cfg)
        print(f'steps output = {self.steps}')
        self.df = self.run(df=df)
        print(f'mapping output = {self.df}')

    def run(self, df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
        out = df.copy()
        for i, step in enumerate(self.steps, 1):
            before = out.shape
            out = step(out)
            if verbose:
                name = getattr(step, "__name__", step.__class__.__name__)
                print(f"[{i}] {name}: {before} -> {out.shape}")
        return out



# ---------- Step builders - functions that can be used within standardisation and then called from the config files ----------
def rename(cols: Dict[str, str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=cols)
    _fn.__name__ = "rename"
    return _fn

def _to_snake_case_label(label: str) -> str:
    # Preserve existing underscores, just normalize casing
    label = label.strip().replace(" ", "_")

    # Step 1: Add underscores before camelCase or PascalCase boundaries
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', label)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)

    # Step 2: Replace multiple underscores with a single underscore
    s3 = re.sub(r'__+', '_', s2)

    return s3.lower()

def to_snake_case() -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = [_to_snake_case_label(col) for col in out.columns]
        return out

    _fn.__name__ = "to_snake_case"
    return _fn

def cast(dtypes: Dict[str, str]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col, dt in dtypes.items():
            if col not in out.columns:
                continue
            if isinstance(dt, str) and dt.startswith("datetime"):
                out[col] = pd.to_datetime(out[col], errors="coerce")
            else:
                try:
                    out[col] = out[col].astype(dt)
                except Exception:
                    pass
        return out
    _fn.__name__ = "cast"
    return _fn

def value_map(column: str, mapping: Dict[Any, Any]) -> FieldMapper:
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        if column not in df.columns:
            return df
        out = df.copy()
        out[column] = out[column].map(mapping).fillna(out[column])
        return out
    _fn.__name__ = f"value_map[{column}]"
    return _fn

# Set a list of required columns that need to be supplied. This could be on the base file and can be applied following the mapping.
def require(columns: Iterable[str]) -> FieldMapper:
    cols = list(columns)
    def _fn(df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in cols if c not in df.columns or df[c].isna().any()]
        if missing:
            raise ValueError(f"Required columns missing/NA: {missing}")
        return df
    _fn.__name__ = "require"
    return _fn


