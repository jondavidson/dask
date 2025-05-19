""" Core dataclass‑based pipeline execution framework driven by config files and executed with Dask.

• Uses only dataclasses + dataclasses_json (no Pydantic) • Spawns external Python scripts that themselves use Polars – avoids in‑process import clashes. • Supports dated outputs via standard --sd / --ed CLI flags, enabling backfills. • Models DAG dependencies and snapshot‑style steps that depend on the full date range. • Provides a clean logging interface and helper utilities for CLI overrides & config loading. """

from future import annotations

import logging import subprocess from pathlib import Path from datetime import date, timedelta from typing import Dict, List, Optional, Iterable

from dataclasses import dataclass, field from dataclasses_json import dataclass_json

from dask.distributed import Client, Future

---------------------------------------------------------------------------

Logging helpers

---------------------------------------------------------------------------

def setup_logging(level: int = logging.INFO) -> None: """Configure root logger with timestamp + level.""" logging.basicConfig( level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S", )

logger = logging.getLogger("pipeline")

---------------------------------------------------------------------------

Script abstraction

---------------------------------------------------------------------------

@dataclass_json @dataclass class Script: """A Python script executed in a subprocess.

Parameters
 ----------
 name: str
     Unique identifier.
 path: str | Path
     Filesystem path to the python file.
 default_args: dict[str, str]
     Default CLI arguments applied to every invocation.
 dated: bool
     If *True*, the script must receive `--sd` and `--ed` flags.
 """

 name: str
 path: Path | str
 default_args: Dict[str, str] = field(default_factory=dict)
 dated: bool = False

 # ---------------------------------------------------------------------
 # Public API
 # ---------------------------------------------------------------------

 def run(self, *, sd: Optional[date] = None, ed: Optional[date] = None,
         override_args: Optional[Dict[str, str]] = None) -> None:
     """Invoke the script in a blocking subprocess call.

     When *dated* is **True** both *sd* and *ed* are required.
     """
     args = {**self.default_args, **(override_args or {})}

     cmd: List[str] = ["python", str(self.path)]

     if self.dated:
         if sd is None or ed is None:
             raise ValueError(f"Script '{self.name}' expects sd & ed.")
         args["--sd"] = str(sd)
         args["--ed"] = str(ed)

     # Expand dict into flat CLI list
     for k, v in args.items():
         cmd.extend([k, v])

     logger.info("Running %s: %s", self.name, " ".join(cmd))
     subprocess.run(cmd, check=True)

---------------------------------------------------------------------------

Pipeline DAG modelling

---------------------------------------------------------------------------

@dataclass_json @dataclass class PipelineNode: """A node in the pipeline DAG."""

name: str
 script: Script
 depends_on: List[str] = field(default_factory=list)
 # If True, this node should run *after* all dates have been processed
 requires_all_dates: bool = False

@dataclass_json @dataclass class Pipeline: """A directed acyclic graph of Script executions."""

name: str
 nodes: Dict[str, PipelineNode]  # keyed by node name

 # ---------------------------------------------------------------------
 # DAG utilities
 # ---------------------------------------------------------------------

 def _topological_sort(self) -> List[str]:
     """Return nodes in dependency order (parents before children)."""
     visited, ordered = set(), []

     def visit(n: str):
         if n in visited:
             return
         visited.add(n)
         for dep in self.nodes[n].depends_on:
             visit(dep)
         ordered.append(n)

     for node_name in self.nodes:
         visit(node_name)
     return ordered

 def execution_layers(self) -> List[List[str]]:
     """Return a list of layers for parallel execution (Kahn style)."""
     remaining = {n for n in self.nodes}
     deps = {n: set(self.nodes[n].depends_on) for n in self.nodes}
     layers: List[List[str]] = []
     while remaining:
         ready = [n for n in remaining if not deps[n]]
         if not ready:
             raise RuntimeError("Cycle detected in pipeline DAG!")
         layers.append(ready)
         for n in ready:
             remaining.remove(n)
             for d in deps.values():
                 d.discard(n)
     return layers

---------------------------------------------------------------------------

Runner

---------------------------------------------------------------------------

@dataclass class PipelineRunner: pipeline: Pipeline client: Client

def run(self, *, sd: date, ed: date, override_args: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, List[Future]]:
     """Run the pipeline for the given date range.

     Returns a mapping node‑>list of Dask futures (one per date or one global).
     """
     override_args = override_args or {}
     futures_by_node: Dict[str, List[Future]] = {}

     # 1️⃣ schedule per‑date tasks first
     date_iter: Iterable[date] = (sd + timedelta(days=i) for i in range((ed - sd).days + 1))

     per_date_futures: Dict[date, Dict[str, Future]] = {}
     for current_date in date_iter:
         per_date_futures[current_date] = {}
         for layer in self.pipeline.execution_layers():
             layer_futures = []
             for node_name in layer:
                 node = self.pipeline.nodes[node_name]
                 if node.requires_all_dates:
                     # Defer snapshots to phase 2
                     continue
                 deps = [per_date_futures[current_date][d] for d in node.depends_on]

                 fut = self.client.submit(
                     node.script.run,
                     sd=current_date,
                     ed=current_date,
                     override_args=override_args.get(node_name),
                     pure=False,
                     wait_for=deps,
                 )
                 per_date_futures[current_date][node_name] = fut
                 futures_by_node.setdefault(node_name, []).append(fut)

     # 2️⃣ schedule snapshot‑style tasks that need the full range
     for node_name, node in self.pipeline.nodes.items():
         if not node.requires_all_dates:
             continue
         deps = [f for futs in per_date_futures.values() for f in futs.values() if futs]
         fut = self.client.submit(
             node.script.run,
             sd=sd,
             ed=ed,
             override_args=override_args.get(node_name),
             pure=False,
             wait_for=deps,
         )
         futures_by_node.setdefault(node_name, []).append(fut)

     return futures_by_node

---------------------------------------------------------------------------

Config loading helpers (YAML)

---------------------------------------------------------------------------

def load_pipeline_from_yaml(path: Path | str) -> Pipeline: """Create Pipeline from a YAML file.""" import yaml

with open(path, "r", encoding="utf-8") as f:
     cfg = yaml.safe_load(f)

 nodes: Dict[str, PipelineNode] = {}
 for node_cfg in cfg["nodes"]:
     script_cfg = node_cfg["script"]
     script = Script(
         name=script_cfg["name"],
         path=script_cfg["path"],
         default_args=script_cfg.get("default_args", {}),
         dated=script_cfg.get("dated", False),
     )
     node = PipelineNode(
         name=node_cfg["name"],
         script=script,
         depends_on=node_cfg.get("depends_on", []),
         requires_all_dates=node_cfg.get("requires_all_dates", False),
     )
     nodes[node.name] = node

 return Pipeline(name=cfg["name"], nodes=nodes)

---------------------------------------------------------------------------

CLI entry‑point helper (optional)

---------------------------------------------------------------------------

if name == "main": import argparse from datetime import datetime

setup_logging()

 parser = argparse.ArgumentParser(description="Run a configured data pipeline via Dask")
 parser.add_argument("config", help="Path to pipeline YAML config")
 parser.add_argument("--sd", required=True, help="Start date YYYY‑MM‑DD")
 parser.add_argument("--ed", required=True, help="End date YYYY‑MM‑DD")
 parser.add_argument("--dask‑scheduler", default=None, help="Address of existing scheduler")
 args = parser.parse_args()

 sd = datetime.strptime(args.sd, "%Y-%m-%d").date()
 ed = datetime.strptime(args.ed, "%Y-%m-%d").date()

 pipeline = load_pipeline_from_yaml(args.config)

 client = Client(address=args.dask_scheduler) if args.dask_scheduler else Client()
 runner = PipelineRunner(pipeline=pipeline, client=client)

 runner.run(sd=sd, ed=ed)
 logger.info("Pipeline submitted — use the Dask dashboard to monitor progress.")

