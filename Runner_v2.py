"""
Enhanced dataclass‑based pipeline execution framework
====================================================
• Only `dataclasses` + `dataclasses_json` (no Pydantic)
• YAML‑driven DAG definition
• Dask‑powered parallel execution (subprocess‑safe for Polars scripts)
• Rich logging + execution‑plan printer
• Supports selective runs: upstream, downstream, or exact‑subset execution
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from dask.distributed import Client, Future
import yaml

# ──────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ──────────────────────────────────────────────────────────────────────────────

_LOG_FORMAT = "%(""asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO, *, log_file: Optional[Path] = None) -> None:
    """Configure the root logger and optionally tee into *log_file*."""
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=level, format=_LOG_FORMAT, handlers=handlers, datefmt="%Y-%m-%d %H:%M:%S")


logger = logging.getLogger("pipeline")

# ──────────────────────────────────────────────────────────────────────────────
# Data‑classes
# ──────────────────────────────────────────────────────────────────────────────


@dataclass_json
@dataclass
class Script:
    name: str
    path: Path | str
    default_args: Dict[str, str] = field(default_factory=dict)
    dated: bool = False

    # ------------------------------------------------------------------
    def run(
        self,
        *,
        sd: Optional[date] = None,
        ed: Optional[date] = None,
        override_args: Optional[Dict[str, str]] = None,
    ) -> None:
        """Invoke the script synchronously in a subprocess."""
        args = {**self.default_args, **(override_args or {})}
        cmd: List[str] = ["python", str(self.path)]

        if self.dated:
            if sd is None or ed is None:
                raise ValueError(f"Script '{self.name}' requires --sd/--ed")
            args["--sd"], args["--ed"] = str(sd), str(ed)

        for k, v in args.items():
            cmd.extend([k, v])

        logger.info("RUN %s: %s", self.name, " ".join(cmd))
        subprocess.run(cmd, check=True)


@dataclass_json
@dataclass
class PipelineNode:
    name: str
    script: Script
    depends_on: List[str] = field(default_factory=list)
    requires_all_dates: bool = False  # snapshot‑style jobs


@dataclass_json
@dataclass
class Pipeline:
    name: str
    nodes: Dict[str, PipelineNode]

    # ───────────────────────────── DAG utilities ──────────────────────────

    def _edges(self) -> Dict[str, Set[str]]:
        return {n: set(node.depends_on) for n, node in self.nodes.items()}

    def topological_layers(self, subset: Optional[Set[str]] = None) -> List[List[str]]:
        """Kahn‑style layering for the (sub‑)graph."""
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
            raise RuntimeError("Cycle detected in DAG subset")
        return layers

    # --------------------- upstream / downstream selection ---------------------

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
        rev_map: Dict[str, Set[str]] = defaultdict(set)
        for child, node in self.nodes.items():
            for dep in node.depends_on:
                rev_map[dep].add(child)
        stack, seen = list(sources), set(sources)
        while stack:
            cur = stack.pop()
            for child in rev_map[cur]:
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return seen

    def subgraph(self, included: Set[str]) -> "Pipeline":
        return Pipeline(name=f"{self.name}_subset", nodes={n: self.nodes[n] for n in included})


# ──────────────────────────────────────────────────────────────────────────────
# Execution‑plan objects
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class TaskSpec:
    node: str
    sd: date
    ed: date
    phase: int  # 1 = per‑date, 2 = snapshot/global


# ──────────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class PipelineRunner:
    pipeline: Pipeline
    client: Client

    # ---------------------------- planning utilities --------------------------

    def build_plan(
        self,
        *,
        sd: date,
        ed: date,
        subset: Optional[Set[str]] = None,
    ) -> List[TaskSpec]:
        subset = subset or set(self.pipeline.nodes)
        layers = self.pipeline.topological_layers(subset)

        # Phase‑1: dated nodes *without* requires_all_dates
        plan: List[TaskSpec] = []
        for single_date in (sd + timedelta(days=i) for i in range((ed - sd).days + 1)):
            for layer in layers:
                for node_name in layer:
                    node = self.pipeline.nodes[node_name]
                    if node.requires_all_dates or node_name not in subset:
                        continue
                    spec = TaskSpec(node=node_name, sd=single_date, ed=single_date, phase=1)
                    plan.append(spec)
        # Phase‑2: snapshot/global nodes
        for layer in layers:
            for node_name in layer:
                node = self.pipeline.nodes[node_name]
                if node_name not in subset:
                    continue
                if node.requires_all_dates or not node.script.dated:
                    spec = TaskSpec(node=node_name, sd=sd, ed=ed, phase=2)
                    plan.append(spec)
        return plan

    # ---------------------------- actual execution ----------------------------

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

        futures: Dict[str, List[Future]] = defaultdict(list)
        # Track when dependencies complete per phase/date combo
        completed: Dict[Tuple[str, date], Future] = {}

        for task in plan:
            node = self.pipeline.nodes[task.node]
            # collect dependency futures
            wait_for: List[Future] = []
            for dep in node.depends_on:
                key_date = task.sd if node.script.dated and not node.requires_all_dates else ed
                fut = completed.get((dep, key_date))
                if fut:
                    wait_for.append(fut)
            fut = self.client.submit(
                node.script.run,
                sd=task.sd,
                ed=task.ed,
                override_args=override_args.get(task.node),
                pure=False,
                wait_for=wait_for,
            )
            completed[(task.node, task.sd)] = fut
            futures[task.node].append(fut)
        return futures

    # ---------------------------------------------------------------------
    def _print_plan(self, plan: List[TaskSpec]) -> None:
        rows = [
            {
                "phase": t.phase,
                "node": t.node,
                "sd": t.sd.isoformat(),
                "ed": t.ed.isoformat(),
            }
            for t in plan
        ]
        print(json.dumps(rows, indent=2))


# ──────────────────────────────────────────────────────────────────────────────
# YAML loader
# ──────────────────────────────────────────────────────────────────────────────


def load_pipeline_from_yaml(path: str | Path) -> Pipeline:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    nodes: Dict[str, PipelineNode] = {}
    for n_cfg in cfg["nodes"]:
        s_cfg = n_cfg["script"]
        script = Script(
            name=s_cfg["name"],
            path=s_cfg["path"],
            default_args=s_cfg.get("default_args", {}),
            dated=s_cfg.get("dated", False),
        )
        node = PipelineNode(
            name=n_cfg["name"],
            script=script,
            depends_on=n_cfg.get("depends_on", []),
            requires_all_dates=n_cfg.get("requires_all_dates", False),
        )
        nodes[node.name] = node
    return Pipeline(name=cfg["name"], nodes=nodes)


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry‑point (starter)
# ──────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run or inspect a YAML‑defined pipeline via Dask")
    p.add_argument("config", help="Path to YAML config")
    p.add_argument("--sd", required=True, help="Start date YYYY‑MM‑DD")
    p.add_argument("--ed", required=True, help="End date YYYY‑MM‑DD")

    # selective‑run flags
    group = p.add_mutually_exclusive_group()
    group.add_argument("--upstream", nargs="*", metavar="NODE", help="Run only upstream of these nodes")
    group.add_argument("--downstream", nargs="*", metavar="NODE", help="Run only downstream of these nodes")
    group.add_argument("--only", nargs="*", metavar="NODE", help="Run exactly these nodes (plus their mutual deps)")

    p.add_argument("--plan", action="store_true", help="Print execution plan and exit (dry‑run)")
    p.add_argument("--dask-scheduler", help="Connect to an existing Dask scheduler")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    sd = datetime.strptime(args.sd, "%Y-%m-%d").date()
    ed = datetime.strptime(args.ed, "%Y-%m-%d").date()

    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    setup_logging(log_file=Path("logs") / f"{run_id}.log")

    pipe = load_pipeline_from_yaml(args.config)

    # determine subset per flags
    subset: Optional[Set[str]] = None
    if args.upstream:
        subset = pipe.upstream(args.upstream)
    elif args.downstream:
        subset = pipe.downstream(args.downstream)
    elif args.only:
        subset = set(args.only)

    client = Client(address=args.dask_scheduler) if args.dask_scheduler else Client()
    runner = PipelineRunner(pipeline=pipe, client=client)

    runner.run(sd=sd, ed=ed, subset=subset, plan_only=args.plan)
    if not args.plan:
        logger.info("Pipeline submitted — monitor progress on the Dask dashboard.")



# python pipeline_framework.py sales_pipeline.yaml \
# --sd 2025-05-16 --ed 2025-05-19 --plan --upstream sales_snapshot

# …to view the schedule without executing, or drop --plan to run the selected slice. Let me know if you’d like retries, failure-recovery hooks, or integration with your existing log directory layout.

