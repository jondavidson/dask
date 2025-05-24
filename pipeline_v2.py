from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from enum import Enum
from typing import Any, Dict, List

import yaml
from dataclasses_json import dataclass_json, LetterCase, DataClassJsonMixin
from marshmallow import EXCLUDE

# ────────────────── Hierarchical dataset loader  ──────────────────
def flatten_dataset_tree(tree: Dict[str, Any],
                         prefix: list[str] | None = None,
                         inherited: Dict[str, Any] | None = None) -> Dict[str, DatasetSpec]:
    prefix = prefix or []
    inherited = inherited or {}
    flat: Dict[str, DatasetSpec] = {}
    meta_keys = {"_base", "_layout", "_suffix"}

    for key, val in tree.items():
        if key.startswith("_"):
            continue
        if isinstance(val, dict) and "partitioning" not in val:
            # subgroup
            next_inh = {**inherited, **{k: v for k, v in val.items() if k in meta_keys}}
            flat.update(flatten_dataset_tree(val, prefix + [key], next_inh))
        else:
            fq = ".".join(prefix + [key])
            cfg = {**inherited, **val}
            cfg["base_path"] = cfg.pop("_base", cfg.get("base_path", "."))
            cfg["layout"] = cfg.pop("_layout", cfg.get("layout", "{date}"))
            cfg["suffix"] = cfg.pop("_suffix", cfg.get("suffix", ".parquet"))
            flat[fq] = DatasetSpec.from_key_and_cfg(fq, cfg)
    return flat


# ────────────────── Keyed mix-in ──────────────────
@dataclass_json(letter_case=LetterCase.CAMEL, meta={"unknown": EXCLUDE})
@dataclass
class Keyed(DataClassJsonMixin):
    _name: str = field(init=False, repr=False)

    @property
    def name(self) -> str: return self._name

    @classmethod
    def from_dict(cls, key: str, cfg: Dict[str, Any]):      # type: ignore
        if not isinstance(cfg, dict):
            raise TypeError(f"{key}: expected mapping, got {type(cfg)}")
        obj = cls.schema().load(cfg)
        object.__setattr__(obj, "_name", key)
        return obj


# ────────────────── Dataset spec (unchanged) ──────────────────
class Part(str, Enum):
    DATED = "dated"
    TIME = "time"
    SNAPSHOT = "snapshot"


@dataclass_json(letter_case=LetterCase.CAMEL, meta={"unknown": EXCLUDE})
@dataclass
class DatasetSpec(Keyed):
    base_path: str
    layout: str = "{date}"
    partitioning: Part = Part.DATED
    suffix: str = ".parquet"

    def path(self, ts: datetime) -> str:
        values = {"date": ts.date(), "datetime": ts.strftime("%Y%m%dT%H%M"), **self.__dict__}
        return str(Path(self.base_path) / (self.layout.format(**values) + self.suffix))


# ────────────────── Node-level objects ──────────────────
class Policy(str, Enum):
    EXACT = "exact"
    WINDOW = "window"


@dataclass_json(letter_case=LetterCase.CAMEL, meta={"unknown": EXCLUDE})
@dataclass
class Window(DataClassJsonMixin):
    back: int = 0
    fwd: int = 0


@dataclass_json(letter_case=LetterCase.CAMEL, meta={"unknown": EXCLUDE})
@dataclass
class ScriptSpec(DataClassJsonMixin):
    path: str
    dated: bool = True


@dataclass_json(letter_case=LetterCase.CAMEL, meta={"unknown": EXCLUDE})
@dataclass
class NodeSpec(Keyed):
    script: ScriptSpec
    inputs: List[str]
    outputs: List[str]
    policy: Policy = Policy.EXACT
    window: Window = field(default_factory=Window)

    def resolve_dates(self, run_date: date) -> tuple[date, date]:
        if self.policy is Policy.EXACT:
            return run_date, run_date
        w = self.window
        return run_date - timedelta(days=w.back), run_date + timedelta(days=w.fwd)


# ────────────────── Pipeline spec ──────────────────
@dataclass_json(letter_case=LetterCase.CAMEL, meta={"unknown": EXCLUDE})
@dataclass
class PipelineSpec(Keyed):
    nodes: Dict[str, NodeSpec] = field(default_factory=dict)


# ────────────────── Loader ──────────────────
def load_pipeline_yaml(path: str) -> PipelineSpec:
    raw = yaml.safe_load(Path(path).read_text())["pipeline"]
    nodes_raw = raw.pop("nodes")
    pipe = PipelineSpec.from_dict(raw["name"], raw)
    # load nodes
    pipe.nodes = {k: NodeSpec.from_dict(k, v) for k, v in nodes_raw.items()}
    return pipe


# ────────────────── Demo ──────────────────
if __name__ == "__main__":
    yaml_text = """
pipeline:
  name: sales_pipeline
  nodes:
    node_a:
      inputs: []
      outputs: [intraday.trades]
      policy: window
      window: { back: 3 }
      script:
        path: scripts/get_trades.py
        dated: true
    node_b:
      inputs: [intraday.trades]
      outputs: [daily.prices]
      script:
        path: scripts/gen_prices.py
        dated: false
"""
    f = Path("/tmp/pipeline_v2.yaml")
    f.write_text(yaml_text)

    pipeline = load_pipeline_yaml(f)
    node_a = pipeline.nodes["node_a"]
    print("Node-a dates for 2025-05-10:", node_a.resolve_dates(date(2025, 5, 10)))
    print("Node-b script path:", pipeline.nodes["node_b"].script.path)
