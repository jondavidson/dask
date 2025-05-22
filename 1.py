"""
plan_example.py
===============

Everything you need to generate dated task plans from a config file
describing one or more pipelines. Feed the plan into your Dask runner.

Includes:

✔️ Partitioning + Policy + Input Window per node
✔️ Support for multiple pipelines
✔️ One-shot daily runs, backfills, snapshot-only
✔️ Simple __main__ demonstration of usage
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Dict, List, Tuple, Optional
import pprint


# ──────────────────────── Pipeline Config (YAML-style) ────────────────────────

PIPELINES: Dict[str, Dict[str, any]] = {
    "sales_pipeline": {
        "defaults": {
            "partitioning": "dated",
            "policy": "exact",
            "window": {"back": 0, "fwd": 0},
        },
        "nodes": {
            "ingest_trades": {
                "script": "scripts/get_trades.py",
                "dated": True,
                "depends_on": []
            },
            "generate_signals": {
                "script": "scripts/gen_signals.py",
                "dated": True,
                "policy": "window",
                "window": {"back": 7},
                "depends_on": ["ingest_trades"]
            },
            "snapshot_stats": {
                "script": "scripts/gen_stats.py",
                "dated": False,
                "partitioning": "snapshot",
                "policy": "full_range",
                "depends_on": ["generate_signals"]
            }
        }
    },

    "pricing_pipeline": {
        "defaults": {
            "partitioning": "dated",
            "policy": "exact",
            "window": {"back": 0, "fwd": 0},
        },
        "nodes": {
            "load_fx": {
                "script": "scripts/load_fx.py",
                "dated": True,
                "depends_on": []
            },
            "aggregate_fx": {
                "script": "scripts/agg_fx.py",
                "dated": False,
                "partitioning": "snapshot",
                "policy": "full_range",
                "depends_on": ["load_fx"]
            }
        }
    }
}

# ──────────────────────── Enums and Dataclasses ───────────────────────────────

class OutputPartitioning(str, Enum):
    DATED = "dated"
    SNAPSHOT = "snapshot"

class InputRangePolicy(str, Enum):
    EXACT = "exact"
    WINDOW = "window"
    FULL_RANGE = "full_range"

@dataclass(frozen=True)
class InputWindow:
    back: int = 0
    fwd: int = 0

@dataclass
class NodeSpec:
    name: str
    script: str
    dated: bool
    partitioning: OutputPartitioning
    policy: InputRangePolicy
    window: InputWindow
    depends_on: List[str]

@dataclass
class Invocation:
    run_dates: List[date]
    sd: date
    ed: date

@dataclass
class TaskSpec:
    node: str
    logical_date: Optional[date]
    sd: date
    ed: date
    partitioning: OutputPartitioning

# ──────────────────────── Planner Core ─────────────────────────────

class Planner:
    def __init__(self, nodes: Dict[str, NodeSpec]) -> None:
        self.nodes = nodes
        self._topo_layers = self._topological_layers()

    def build_plan(self, inv: Invocation) -> List[TaskSpec]:
        plan: List[TaskSpec] = []
        for D in inv.run_dates:
            for layer in self._topo_layers:
                for node_name in layer:
                    node = self.nodes[node_name]
                    sd, ed = self._resolve_dates(node, D, inv)
                    plan.append(TaskSpec(
                        node=node.name,
                        logical_date=None if node.partitioning == OutputPartitioning.SNAPSHOT else D,
                        sd=sd,
                        ed=ed,
                        partitioning=node.partitioning
                    ))
        for layer in self._topo_layers:
            for node_name in layer:
                node = self.nodes[node_name]
                if node.partitioning == OutputPartitioning.SNAPSHOT:
                    plan.append(TaskSpec(
                        node=node.name,
                        logical_date=None,
                        sd=inv.sd,
                        ed=inv.ed,
                        partitioning=node.partitioning
                    ))
        return plan

    def _resolve_dates(self, node: NodeSpec, D: date, inv: Invocation) -> Tuple[date, date]:
        if node.policy == InputRangePolicy.EXACT:
            return D, D
        if node.policy == InputRangePolicy.WINDOW:
            return D - timedelta(days=node.window.back), D + timedelta(days=node.window.fwd)
        if node.policy == InputRangePolicy.FULL_RANGE:
            return inv.sd, inv.ed
        raise ValueError(f"Unknown policy {node.policy}")

    def _topological_layers(self) -> List[List[str]]:
        indeg = {n: 0 for n in self.nodes}
        for spec in self.nodes.values():
            for dep in spec.depends_on:
                indeg[spec.name] += 1
        ready = [n for n, d in indeg.items() if d == 0]
        layers = []
        while ready:
            layers.append(ready[:])
            nxt = []
            for n in ready:
                for child, spec in self.nodes.items():
                    if n in spec.depends_on:
                        indeg[child] -= 1
                        if indeg[child] == 0:
                            nxt.append(child)
            ready = nxt
        return layers

# ──────────────────────── Helpers ─────────────────────────────

def parse_nodes(config: Dict[str, any]) -> Dict[str, NodeSpec]:
    defaults = config.get("defaults", {})
    nodes = {}
    for name, cfg in config["nodes"].items():
        nodes[name] = NodeSpec(
            name=name,
            script=cfg["script"],
            dated=cfg.get("dated", True),
            partitioning=OutputPartitioning(cfg.get("partitioning", defaults.get("partitioning", "dated"))),
            policy=InputRangePolicy(cfg.get("policy", defaults.get("policy", "exact"))),
            window=InputWindow(**cfg.get("window", defaults.get("window", {}))),
            depends_on=cfg.get("depends_on", [])
        )
    return nodes

def date_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

# ──────────────────────── Main Usage Example ─────────────────────────────

if __name__ == "__main__":
    from datetime import date

    # Example: Live daily run
    print("\n=== Live daily run ===")
    inv = Invocation(
        run_dates=[date.today()],
        sd=date.today(),
        ed=date.today()
    )
    nodes = parse_nodes(PIPELINES["sales_pipeline"])
    plan = Planner(nodes).build_plan(inv)
    pprint.pprint(plan)

    # Example: Backfill 3 days
    print("\n=== Backfill ===")
    inv = Invocation(
        run_dates=date_range(date(2025, 5, 1), date(2025, 5, 3)),
        sd=date(2025, 4, 1),
        ed=date(2025, 5, 3)
    )
    plan = Planner(parse_nodes(PIPELINES["sales_pipeline"])).build_plan(inv)
    pprint.pprint(plan)

    # Example: Snapshot-only
    print("\n=== Snapshot-only run ===")
    inv = Invocation(
        run_dates=[],
        sd=date(2024, 1, 1),
        ed=date(2025, 1, 1)
    )
    plan = Planner(parse_nodes(PIPELINES["pricing_pipeline"])).build_plan(inv)
    pprint.pprint(plan)
