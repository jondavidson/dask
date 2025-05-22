semantic_time_plan.py

from future import annotations from dataclasses import dataclass, field from datetime import date, datetime, timedelta, time, timezone from enum import Enum from typing import Dict, List, Optional, Tuple

────────────── Enums ──────────────

class OutputPartitioning(str, Enum): DATED = "dated" SNAPSHOT = "snapshot"

class InputRangePolicy(str, Enum): EXACT = "exact" WINDOW = "window" FULL_RANGE = "full_range" AS_OF_EXPAND = "as_of_expand"

class TimeMode(str, Enum): NONE = "none" DERIVE_FROM_LOGICAL = "derive_from_logical" DERIVE_FROM_DATES = "derive_from_dates"

────────────── Dataclasses ──────────────

@dataclass(frozen=True) class InputWindow: back: int = 0 fwd: int = 0

@dataclass class NodeSpec: name: str script: str dated: bool partitioning: OutputPartitioning policy: InputRangePolicy window: InputWindow = InputWindow() time_mode: TimeMode = TimeMode.NONE timezone: str = "UTC" depends_on: List[str] = field(default_factory=list)

@dataclass class Invocation: run_datetimes: List[datetime]         # high-resolution run points sd: date ed: date

@dataclass class TaskSpec: node: str logical_datetime: Optional[datetime] sd: date ed: date st: Optional[datetime] = None et: Optional[datetime] = None partitioning: OutputPartitioning = OutputPartitioning.DATED

@property
def key(self) -> str:
    if self.logical_datetime:
        return f"{self.node}:{self.logical_datetime.isoformat()}"
    return self.node

────────────── Planner ──────────────

class Planner: def init(self, nodes: Dict[str, NodeSpec]) -> None: self.nodes = nodes self._topo_layers = self._topological_layers()

def build_plan(self, inv: Invocation) -> List[TaskSpec]:
    plan: List[TaskSpec] = []
    for T in inv.run_datetimes:
        for layer in self._topo_layers:
            for node_name in layer:
                node = self.nodes[node_name]
                sd, ed = self._resolve_dates(node, T.date(), inv)
                st, et = self._resolve_times(node, T, sd, ed)
                plan.append(TaskSpec(
                    node=node.name,
                    logical_datetime=None if node.partitioning == OutputPartitioning.SNAPSHOT else T,
                    sd=sd,
                    ed=ed,
                    st=st,
                    et=et,
                    partitioning=node.partitioning
                ))
    for layer in self._topo_layers:
        for node_name in layer:
            node = self.nodes[node_name]
            if node.partitioning == OutputPartitioning.SNAPSHOT:
                st, et = self._resolve_times(node, None, inv.sd, inv.ed)
                plan.append(TaskSpec(
                    node=node.name,
                    logical_datetime=None,
                    sd=inv.sd,
                    ed=inv.ed,
                    st=st,
                    et=et,
                    partitioning=node.partitioning
                ))
    return plan

def _resolve_dates(self, node: NodeSpec, D: date, inv: Invocation) -> Tuple[date, date]:
    if node.policy == InputRangePolicy.EXACT:
        return D, D
    elif node.policy == InputRangePolicy.WINDOW:
        return D - timedelta(days=node.window.back), D + timedelta(days=node.window.fwd)
    elif node.policy == InputRangePolicy.FULL_RANGE:
        return inv.sd, inv.ed
    elif node.policy == InputRangePolicy.AS_OF_EXPAND:
        return inv.sd, D
    raise ValueError(f"Unknown policy: {node.policy}")

def _resolve_times(
    self,
    node: NodeSpec,
    logical: Optional[datetime],
    sd: date,
    ed: date
) -> Tuple[Optional[datetime], Optional[datetime]]:
    if node.time_mode == TimeMode.NONE:
        return None, None
    tz = timezone.utc
    if node.time_mode == TimeMode.DERIVE_FROM_DATES:
        return datetime.combine(sd, time.min, tz), datetime.combine(ed, time.max, tz)
    if node.time_mode == TimeMode.DERIVE_FROM_LOGICAL and logical:
        step = timedelta(minutes=15)  # optionally parameterized
        return logical, logical + step
    return None, None

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

────────────── Utility ──────────────

def datetime_range(start: datetime, end: datetime, step_minutes: int) -> List[datetime]: out = [] curr = start while curr <= end: out.append(curr) curr += timedelta(minutes=step_minutes) return out

────────────── Example ──────────────

if name == "main": from pprint import pprint

nodes = {
    "tick_ingest": NodeSpec(
        name="tick_ingest",
        script="scripts/tick_ingest.py",
        dated=True,
        partitioning=OutputPartitioning.DATED,
        policy=InputRangePolicy.EXACT,
        time_mode=TimeMode.DERIVE_FROM_LOGICAL
    ),
    "resample_ticks": NodeSpec(
        name="resample_ticks",
        script="scripts/resample.py",
        dated=False,
        partitioning=OutputPartitioning.SNAPSHOT,
        policy=InputRangePolicy.AS_OF_EXPAND,
        time_mode=TimeMode.DERIVE_FROM_DATES,
        depends_on=["tick_ingest"]
    )
}

run_start = datetime(2025, 5, 1, 9, 0)
run_end   = datetime(2025, 5, 1, 10, 0)
run_points = datetime_range(run_start, run_end, step_minutes=15)

inv = Invocation(
    run_datetimes=run_points,
    sd=date(2025, 4, 1),
    ed=run_end.date()
)

plan = Planner(nodes).build_plan(inv)
for task in plan:
    print(task.key, task.sd, task.ed, task.st, task.et, task.partitioning)

