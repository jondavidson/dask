# run_modes.py

from enum import Enum

class RunMode(str, Enum):
    """Defines how the pipeline interprets and iterates over sd/ed."""
    SINGLE = "single"         # Run once with sd == ed
    PASS_THROUGH = "pass_through"  # Scripts handle their own sd/ed range
    DATE_ITER = "date_iter"   # Pipeline expands into one run per date in sd..ed


class DatePolicy(str, Enum):
    """Controls how each node consumes dates passed from the runner."""
    EXACT = "exact"       # Use sd = ed = logical_date
    WINDOW = "window"     # Use a window (e.g., sd = D-7, ed = D)
    RANGE = "range"       # Use sd/ed from the overall pipeline args

@dataclass
class PipelineNode:
    name: str
    executable: Executable
    depends_on: List[str]
    requires_all_dates: bool = False
    date_policy: DatePolicy = DatePolicy.EXACT
    date_window: Tuple[int, int] = (0, 0)  # (back, forward) in days


@dataclass
class Task:
    node: str
    sd: date
    ed: date
    logical_date: Optional[date] = None  # for graph keys, backtracking


if run_mode == RunMode.DATE_ITER:
    plan = []
    for d in date_range(sd, ed):
        for node in self.pipeline.nodes.values():
            if node.date_policy == DatePolicy.EXACT:
                task_sd, task_ed = d, d
            elif node.date_policy == DatePolicy.WINDOW:
                back, fwd = node.date_window
                task_sd = d - timedelta(days=back)
                task_ed = d + timedelta(days=fwd)
            elif node.date_policy == DatePolicy.RANGE:
                task_sd, task_ed = sd, ed
            else:
                raise ValueError(f"Unknown date policy for node {node.name}")

            plan.append(Task(
                node=node.name,
                sd=task_sd,
                ed=task_ed,
                logical_date=d
            ))


  name = f"{context.label}:{node.name}:{task.logical_date or task.sd}"
delayed_task = delayed(self._exec_with_retry, name=name)(...)




import json from pathlib import Path from datetime import datetime import argparse

def list_runs(log_root: Path, pipeline: str = None, limit: int = 10, as_json: bool = False): runs = []

for run_dir in sorted(log_root.glob(f"{pipeline or '*'}_*_*"), reverse=True):
    summary_path = run_dir / "run_summary.json"
    if not summary_path.exists():
        continue
    try:
        with summary_path.open() as f:
            summary = json.load(f)

        failed_tasks = [t for t in summary.get("tasks", []) if t.get("status") != "success"]

        runs.append({
            "run_id": summary.get("run_id"),
            "start": summary.get("start_time"),
            "end": summary.get("end_time"),
            "status": summary.get("status"),
            "log_dir": str(run_dir),
            "fail_count": len(failed_tasks),
            "failed_tasks": failed_tasks,
            "all_tasks": summary.get("tasks", [])
        })
    except Exception as e:
        print(f"Warning: Failed to read {summary_path}: {e}")

runs = sorted(runs, key=lambda r: r["start"], reverse=True)

if as_json:
    print(json.dumps(runs[:limit], indent=2))
else:
    for r in runs[:limit]:
        start = datetime.fromisoformat(r["start"]).strftime("%Y-%m-%d %H:%M:%S")
        fail_note = f" | {r['fail_count']} failed" if r['fail_count'] else ""
        print(f"[{r['status']:<16}] {start} | {r['run_id']}{fail_note} | logs: {r['log_dir']}")

def compare_runs(run_dir1: Path, run_dir2: Path): try: summary1 = json.load((run_dir1 / "run_summary.json").open()) summary2 = json.load((run_dir2 / "run_summary.json").open()) tasks1 = {t['name']: t for t in summary1.get("tasks", [])} tasks2 = {t['name']: t for t in summary2.get("tasks", [])}

all_keys = sorted(set(tasks1) | set(tasks2))
    print("Task Comparison:")
    for key in all_keys:
        s1 = tasks1.get(key, {}).get("status", "missing")
        s2 = tasks2.get(key, {}).get("status", "missing")
        print(f"{key:<20}: {s1:<12} → {s2:<12}")
except Exception as e:
    print(f"Failed to compare runs: {e}")

def explain_run_plan(plan_file: Path): try: with plan_file.open() as f: plan = json.load(f) print("Planned Task Execution:") for task in plan: logical = task.get("logical_date", task.get("sd")) print(f"[{task['node']:<20}] logical: {logical}, sd: {task['sd']}, ed: {task['ed']}") except Exception as e: print(f"Failed to explain plan: {e}")

if name == "main": parser = argparse.ArgumentParser(description="List recent pipeline runs.") parser.add_argument("--log-dir", default="logs", help="Root logs/ directory") parser.add_argument("--pipeline", help="Filter by pipeline name") parser.add_argument("--limit", type=int, default=10) parser.add_argument("--json", action="store_true", help="Output in machine-readable JSON format") parser.add_argument("--diff", nargs=2, metavar=('RUN1', 'RUN2'), help="Compare two run folders") parser.add_argument("--explain", type=str, help="Explain a saved run plan JSON") args = parser.parse_args()

if args.diff:
    compare_runs(Path(args.diff[0]), Path(args.diff[1]))
elif args.explain:
    explain_run_plan(Path(args.explain))
else:
    list_runs(Path(args.log_dir), pipeline=args.pipeline, limit=args.limit, as_json=args.json)

""" Expected directory structure:

logs/ ├── sales_pipeline_20250521T141210_zebra-mango-haze/ │   ├── get_trades.log │   ├── enrich_orders.log │   ├── sync.log │   └── run_summary.json ├── pricing_pipeline_20250520T104400_echo-raven-blitz/ │   ├── get_prices.log │   ├── generate_yields.log │   └── run_summary.json ...

Each run is isolated under logs/{run_id}/

One .log file per script (matching node name)

One run_summary.json per run """




- name: generate_signals
  date_policy: window
  date_window: [7, 0]       # 7-day historical input for each run
- name: ingest_fx
  date_policy: exact
- name: backfill_loader
  date_policy: range


"""
pipeline_service.py
-------------------
A *lightweight control-plane* for your Dask-based pipelines.

Features
========
* POST /runs            – launch a new Run (background thread)
* GET  /runs            – list recent runs (JSON)
* GET  /runs/{id}       – status & metadata
* DELETE /runs/{id}     – cancel an active run
* GET  /logs/{id}/{task} – stream task logfile
  • supports ?tail=50   – start N lines from end
  • supports ?q=error   – substring filter

The service stores minimal metadata in **SQLite** (file `runs.db`)
and relies on per-run folders `logs/{run_id}/…` that your
`PipelineRunner` already produces.

RunIDs are generated via `RunContext.create()` and include the
human-readable three-word label.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from uvicorn import run as uv_run

# ─── import your pipeline modules ──────────────────────────────────────────
# adjust these imports to match your package layout
from run_context import RunContext
from pipeline_framework import load_pipeline_from_yaml, PipelineRunner
from common_logging import setup_logging
from run_modes import RunMode

# ───────────────────────────────────────────────────────────────────────────


DB_FILE = Path("runs.db")
LOG_ROOT = Path("logs")
DB_LOCK = threading.Lock()  # SQLite is not fully thread-safe without a lock


def _init_db() -> None:
    """Ensure SQLite schema exists."""
    with DB_LOCK, sqlite3.connect(DB_FILE) as con:
        con.executescript(
            """
        create table if not exists runs (
            run_id text primary key,
            pipeline text,
            status text,
            start_time text,
            end_time text,
            log_dir text
        );
        """
        )
        con.commit()


@dataclass
class RunHandle:
    context: RunContext
    thread: threading.Thread
    status: str = "running"
    end_time: Optional[str] = None


RUNS: Dict[str, RunHandle] = {}  # active runs in memory


# ─── FastAPI models ────────────────────────────────────────────────────────
class LaunchPayload(BaseModel):
    pipeline_file: str
    sd: str
    ed: str
    mode: RunMode = RunMode.SINGLE


# ─── helper: background runner ─────────────────────────────────────────────
def _run_pipeline(payload: LaunchPayload, ctx: RunContext) -> None:
    """Background thread target."""
    setup_logging("runner", log_dir=ctx.log_dir)
    # override log dir for all child scripts
    os.environ["PIPELINE_LOG_DIR"] = str(ctx.log_dir)
    try:
        pipeline = load_pipeline_from_yaml(payload.pipeline_file)
        runner = PipelineRunner(pipeline, client=None)  # assume Client inside
        runner.run(
            context=ctx,
            sd=datetime.fromisoformat(payload.sd).date(),
            ed=datetime.fromisoformat(payload.ed).date(),
            mode=payload.mode,
        )
        status = "success"
    except Exception as exc:
        status = f"failed: {exc}"
    finally:
        RUNS[ctx.run_id].status = status
        RUNS[ctx.run_id].end_time = datetime.utcnow().isoformat()
        _persist_run(RUNS[ctx.run_id])


def _persist_run(handle: RunHandle) -> None:
    """Save (or update) a run row in SQLite."""
    with DB_LOCK, sqlite3.connect(DB_FILE) as con:
        con.execute(
            """
        insert or replace into runs (run_id, pipeline, status, start_time, end_time, log_dir)
        values (:run_id, :pipeline, :status, :start, :end, :log_dir)
        """,
            {
                "run_id": handle.context.run_id,
                "pipeline": handle.context.pipeline_name,
                "status": handle.status,
                "start": handle.context.start_time.isoformat(),
                "end": handle.end_time,
                "log_dir": str(handle.context.log_dir),
            },
        )
        con.commit()


# ─── FastAPI app ───────────────────────────────────────────────────────────
app = FastAPI(title="Pipeline Run-Service")


@app.on_event("startup")
def _startup() -> None:
    _init_db()


# ---- REST endpoints -------------------------------------------------------
@app.post("/runs", status_code=201)
def launch_run(payload: LaunchPayload) -> Dict[str, str]:
    pipeline_name = Path(payload.pipeline_file).stem
    ctx = RunContext.create(pipeline_name=pipeline_name, base_log_dir=LOG_ROOT)
    thread = threading.Thread(target=_run_pipeline, args=(payload, ctx), daemon=True)
    RUNS[ctx.run_id] = RunHandle(context=ctx, thread=thread)
    _persist_run(RUNS[ctx.run_id])
    thread.start()
    return {"run_id": ctx.run_id, "label": ctx.label}


@app.get("/runs")
def list_runs(limit: int = 20) -> JSONResponse:
    with DB_LOCK, sqlite3.connect(DB_FILE) as con:
        cur = con.execute(
            "select run_id, pipeline, status, start_time, end_time from runs order by start_time desc limit ?",
            (limit,),
        )
        rows = [
            dict(
                run_id=r[0],
                pipeline=r[1],
                status=r[2],
                start=r[3],
                end=r[4],
            )
            for r in cur.fetchall()
        ]
    return JSONResponse(rows)


@app.get("/runs/{run_id}")
def run_status(run_id: str) -> JSONResponse:
    with DB_LOCK, sqlite3.connect(DB_FILE) as con:
        cur = con.execute("select * from runs where run_id = ?", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="run_id not found")

    keys = ["run_id", "pipeline", "status", "start_time", "end_time", "log_dir"]
    return JSONResponse(dict(zip(keys, row)))


@app.delete("/runs/{run_id}")
def cancel_run(run_id: str) -> JSONResponse:
    handle = RUNS.get(run_id)
    if not handle:
        raise HTTPException(status_code=404, detail="active run not found")
    # attempt to cancel futures via context (not shown) or just stop thread (coarse)
    handle.status = "cancelled"
    handle.end_time = datetime.utcnow().isoformat()
    _persist_run(handle)
    return {"status": "cancelling"}


# ---- Log streaming with search/tail ---------------------------------------
def _tail_f(path: Path, tail: int, q: Optional[str]) -> "asyncio.AsyncGenerator[str, None]":  # noqa: quotes
    async def gen():
        # open file in blocking mode + seek to last N lines
        with path.open("r", encoding="utf-8", errors="replace") as f:
            if tail:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                block = min(4096, size)
                data = ""
                while size > 0 and tail > 0:
                    size -= block
                    f.seek(max(size, 0))
                    data = f.read(block) + data
                    tail -= data.count("\n")
                lines = data.splitlines()[-tail:]
                for ln in lines:
                    if not q or q in ln:
                        yield ln + "\n"
            # continuous follow
            while True:
                ln = f.readline()
                if ln:
                    if not q or q in ln:
                        yield ln
                else:
                    await asyncio.sleep(0.5)

    return gen()


@app.get("/logs/{run_id}/{task_name}")
def stream_log(
    run_id: str,
    task_name: str,
    tail: int = 100,
    q: Optional[str] = None,
):
    log_path = LOG_ROOT / run_id / f"{task_name}.log"
    if not log_path.exists():
        raise HTTPException(404, "log not found")

    return StreamingResponse(
        _tail_f(log_path, tail, q),
        media_type="text/plain",
    )


# ─── main entry-point (for uvicorn) ─────────────────────────────────────────
if __name__ == "__main__":
    uv_run("pipeline_service:app", reload=True, port=8000)
