""" Pipeline framework – delayed‑based runner version

• Uses dask.delayed instead of per‑task dummy futures. • All other dataclasses stay intact. • Only PipelineRunner is refactored; public API unchanged. """

from future import annotations

import argparse import json import logging import os import subprocess from collections import defaultdict, deque from datetime import date, datetime, timedelta from pathlib import Path from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from dataclasses import dataclass, field, replace from dataclasses_json import dataclass_json

import dask import dask.delayed as dly from dask.distributed import Client, wait, as_completed, Future import yaml

────────────────────────────── logging ──────────────────────────────────────

_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def setup_logging(level: int = logging.INFO, *, log_file: Optional[Path] = None) -> None: handlers: List[logging.Handler] = [logging.StreamHandler()] if log_file: log_file.parent.mkdir(parents=True, exist_ok=True) handlers.append(logging.FileHandler(log_file, encoding="utf-8")) logging.basicConfig(level=level, format=_LOG_FORMAT, handlers=handlers, datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger("pipeline")

────────────────────────────── dataclasses ──────────────────────────────────

@dataclass_json @dataclass class Script: name: str path: Path | str default_args: Dict[str, str] = field(default_factory=dict) dated: bool = False

def run(self, *, sd: Optional[date] = None, ed: Optional[date] = None, override_args: Optional[Dict[str, str]] = None) -> None:
    args = {**self.default_args, **(override_args or {})}
    cmd: List[str] = ["python", str(self.path)]
    if self.dated:
        if sd is None or ed is None:
            raise ValueError(f"Script '{self.name}' requires --sd / --ed")
        args["--sd"], args["--ed"] = str(sd), str(ed)
    for k, v in args.items():
        cmd.extend([k, v])
    logger.info("RUN %s: %s", self.name, " ".join(cmd))
    subprocess.run(cmd, check=True)

@dataclass_json @dataclass class PipelineNode: name: str script: Script depends_on: List[str] = field(default_factory=list) requires_all_dates: bool = False

@dataclass_json @dataclass class Pipeline: name: str nodes: Dict[str, PipelineNode]

# ─── DAG helpers ──────────────────────────────────────────────────────

def topological_layers(self, subset: Optional[Set[str]] = None) -> List[List[str]]:
    subset = subset or set(self.nodes)
    in_deg = {n: 0 for n in subset}
    for n in subset:
        in_deg[n] = len([d for d in self.nodes[n].depends_on if d in subset])
    ready = deque([n for n, deg in in_deg.items() if deg == 0])
    layers: List[List[str]] = []
    while ready:
        layer = list(ready)
        layers.append(layer)
        ready.clear()
        for n in layer:
            for child in subset:
                if n in self.nodes[child].depends_on:
                    in_deg[child] -= 1
                    if in_deg[child] == 0:
                        ready.append(child)
    if sum(len(l) for l in layers) != len(subset):
        raise RuntimeError("Cycle in DAG subset")
    return layers

def upstream(self, targets: Sequence[str]) -> Set[str]:
    stack, seen = list(targets), set(targets)
    while stack:
        cur = stack.pop()
        for dep in self.nodes[cur].depends_on:
            if dep not in seen:
                seen.add(dep)
                stack.append(dep)
    return seen

def downstream(self, sources: Sequence[str]) -> Set[str]:
    rev: Dict[str, Set[str]] = defaultdict(set)
    for child, node in self.nodes.items():
        for dep in node.depends_on:
            rev[dep].add(child)
    stack, seen = list(sources), set(sources)
    while stack:
        cur = stack.pop()
        for child in rev[cur]:
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen

def extract(self, include: Set[str], *, rewire: bool = True) -> "Pipeline":
    new_nodes: Dict[str, PipelineNode] = {}
    for name in include:
        node = replace(self.nodes[name])  # shallow copy
        if rewire:
            node.depends_on = [d for d in node.depends_on if d in include]
        new_nodes[name] = node
    return Pipeline(name=f"{self.name}_subset", nodes=new_nodes)

─────────────────────────── execution-plan types ────────────────────────────

@dataclass class TaskSpec: node: str sd: date ed: date phase: int  # 1=date partition, 2=snapshot/global

─────────────────────────────── Runner ───────────────────────────────────────

@dataclass class PipelineRunner: pipeline: Pipeline client: Client

# ---------------- build execution plan -------------------------------

def build_plan(self, *, sd: date, ed: date, subset: Optional[Set[str]] = None) -> List[TaskSpec]:
    subset = subset or set(self.pipeline.nodes)
    layers = self.pipeline.topological_layers(subset)
    # phase 1 – per‑date tasks
    plan: List[TaskSpec] = []
    for single in (sd + timedelta(days=i) for i in range((ed - sd).days + 1)):
        for layer in layers:
            for n in layer:
                node = self.pipeline.nodes[n]
                if node.requires_all_dates or n not in subset:
                    continue
                plan.append(TaskSpec(n, single, single, 1))
    # phase 2 – snapshot/global tasks
    for layer in layers:
        for n in layer:
            if n not in subset:
                continue
            node = self.pipeline.nodes[n]
            if node.requires_all_dates or not node.script.dated:
                plan.append(TaskSpec(n, sd, ed, 2))
    return plan

# ---------------- pretty‑printer -------------------------------------

def _print_plan(self, plan: List[TaskSpec]) -> None:
    rows = [dataclasses.asdict(p) for p in plan]
    print(json.dumps(rows, indent=2))

# ---------------- worker helper (delayed) ----------------------------

@staticmethod
def _run_script(script: Script, sd: date, ed: date, overrides: Dict[str, str], *_deps):
    script.run(sd=sd, ed=ed, override_args=overrides)
    return f"{script.name}:{sd}"

# ---------------- main entry ----------------------------------------

def run(
    self,
    *,
    sd: date,
    ed: date,
    subset: Optional[Set[str]] = None,
    override_args: Optional[Dict[str, Dict[str, str]]] = None,
    plan_only: bool = False,
) -> Dict[str, List[Future]]:
    override_args = override_args or {}
    plan = self.build_plan(sd=sd, ed=ed, subset=subset)
    if plan_only:
        self._print_plan(plan)
        return {}

    delayed_nodes: Dict[Tuple[str, date], dask.delayed.Delayed] = {}
    futures_by_node: Dict[str, List[Future]] = defaultdict(list)

    for task in plan:
        node = self.pipeline.nodes[task.node]
        key_date = task.sd if node.script.dated and not node.requires_all_dates else ed
        parents = [delayed_nodes[(dep, key_date)] for dep in node.depends_on if (dep, key_date) in delayed_nodes]
        delayed_task = dly(self._run_script)(
            node.script,
            task.sd,
            task.ed,
            override_args.get(task.node, {}),
            *parents,
        )
        delayed_nodes[(task.node, key_date)] = delayed_task

    # materialise all leaves
    leaves = list(delayed_nodes.values())
    futures = self.client.compute(leaves, sync=False)
    wait(futures)

    # map back to node‑level dict
    for (node_name, _), fut in zip(delayed_nodes.keys(), futures):
        futures_by_node.setdefault(node_name, []).append(fut)
    return futures_by_node

─────────────────────────── YAML loader & CLI ───────────────────────────────

def load_pipeline_from_yaml(path: str | Path) -> Pipeline: with open(path, "r", encoding="utf-8") as f:



my_pipeline/
│
├── core/
│   ├── __init__.py          # re-export Script, PipelineNode, Pipeline
│   ├── script.py            # Script class
│   ├── dag.py               # PipelineNode + Pipeline (+ tree utils)
│   └── logging.py           # setup_logging()
│
├── runner/
│   ├── __init__.py
│   └── runner.py            # PipelineRunner (delayed version)
│
├── io/
│   ├── __init__.py
│   └── yaml_loader.py       # load_pipeline_from_yaml / to_yaml helpers
│
├── cli/
│   ├── __init__.py
│   └── main.py              # argparse entry-point, invokes runner
│
└── tools/
    ├── json2yaml.py         # converter utility
    └── tree_view.py         # UTF-8 tree printer

