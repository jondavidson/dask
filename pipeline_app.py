# pipeline_app.py
"""
Consolidated dynamic‑pipeline runner + live dashboard.

Assumptions
-----------
* You already have a `Pipeline` object whose `.scripts` attribute yields
  `Script` instances (see previous snippets for that class).
* Install dependencies: `dask[distributed] dash dash‑cytoscape cloudpickle`.
* Start with `python pipeline_app.py` — the dashboard appears on
  http://localhost:8051 while tasks execute on a local Dask cluster.
"""

from __future__ import annotations

import logging
import queue
import threading
import multiprocessing as mp
import subprocess, shlex, os, inspect, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Callable

import cloudpickle  # subprocess helper
import dash
import dash_cytoscape as cyto
from dash import dcc, html, Output, Input, State
from dask import annotate
from dask.distributed import Client, LocalCluster, as_completed, Future

# ---------------------------------------------------------------------------
# 0.  GLOBAL LOGGING (central Queue)
# ---------------------------------------------------------------------------
LOG_QUEUE: "queue.Queue[str]" = queue.Queue(maxsize=100_000)

root = logging.getLogger()
root.setLevel(logging.INFO)
# stream everything into an in‑memory queue that the Dash GUI will tail
root.addHandler(logging.handlers.QueueHandler(LOG_QUEUE))
# and echo to console for good measure
root.addHandler(logging.StreamHandler())


# ---------------------------------------------------------------------------
# 1.  OPTIONAL helper: run a Python callable in a fresh OS process
# ---------------------------------------------------------------------------

def run_callable_in_subprocess(fn: Callable[..., Any], **kwargs) -> Any:
    """Execute *fn* in a brand‑new interpreter, return its result or raise."""
    def _child(pipe, blob):
        fn, kwargs = cloudpickle.loads(blob)
        import traceback
        try:
            res = fn(**kwargs)
            pipe.send(("ok", res))
        except Exception as exc:  # noqa: BLE001
            pipe.send(("err", (exc, traceback.format_exc())))
        finally:
            pipe.close()

    parent, child = mp.Pipe()
    ctx = mp.get_context("spawn")  # portable, isolated
    p = ctx.Process(target=_child, args=(child, cloudpickle.dumps((fn, kwargs))))
    p.start(); child.close()

    status, payload = parent.recv()
    p.join()

    if status == "ok":
        return payload
    exc, tb = payload
    raise RuntimeError(tb) from exc


# ---------------------------------------------------------------------------
# 2.  Internal worker‑side wrapper so we capture every byte of output
# ---------------------------------------------------------------------------

def _script_runner(script, **kwargs):
    """Runs on the Dask *worker* process — wraps Script.run()."""
    import logging, io, contextlib, json, time

    logger = logging.getLogger(script.name)
    logger.setLevel(logging.INFO)

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(
        json.dumps({
            "ts": "%(asctime)s",
            "lvl": "%(levelname)s",
            "script": script.name,
            "msg": "%(message)s",
        })
    ))
    logger.addHandler(handler)

    with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
        start = time.time()
        logger.info("started")
        result = script.run(**kwargs)
        logger.info("finished in %.2fs", time.time() - start)

    # ship logs back to scheduler — driver’s QueueHandler picks them up
    stream.seek(0)
    for line in stream:
        logging.getLogger("distributed.scriptlogs").info(line.rstrip())

    return result


# ---------------------------------------------------------------------------
# 3.  DynamicPipelineRunner — submits / expands DAG on‑the‑fly
# ---------------------------------------------------------------------------

class DynamicPipelineRunner:
    """Orchestrate Script objects and allow dynamic fan‑out."""

    def __init__(self, pipeline, client: Client):
        self.client = client
        self.scripts: Dict[str, Any] = {s.name: s for s in pipeline.scripts}
        self.futures: Dict[str, Future] = {}
        self._validate()

    # ------------------------------------------------------------------ utils
    def _validate(self) -> None:
        for s in self.scripts.values():
            missing = set(getattr(s, "dependencies", [])) - self.scripts.keys()
            if missing:
                raise ValueError(f"{s.name} depends on unknown scripts {missing}")

    @staticmethod
    def _looks_like_scripts(obj: Any) -> bool:
        try:
            return bool(obj) and all(hasattr(x, "run") and hasattr(x, "name") for x in obj)
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------ run
    def run(self) -> None:
        """Blocks until every (current *and* future) script finishes."""
        self._submit_ready(initial=True)
        for fut in as_completed(self.futures.values()):
            name = fut.key
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                logging.getLogger("runner").error("%s failed: %s", name, exc, exc_info=exc)
                continue

            if self._looks_like_scripts(result):
                for s in result:
                    self.add_script(s, parent=name)
            else:
                self.scripts[name]._result = result

            self._submit_ready()

    # ------------------------------------------------------------------ mutate
    def add_script(self, script, parent: str | None = None) -> None:
        if script.name in self.scripts:
            raise ValueError(f"Duplicate script name {script.name}")
        self.scripts[script.name] = script
        if parent and parent not in script.dependencies:
            script.dependencies.append(parent)

    # ------------------------------------------------------------------ submit
    def _submit_ready(self, *, initial: bool = False) -> None:
        """Submit scripts whose dependencies are satisfied."""
        for s in self.scripts.values():
            if s.name in self.futures:  # already running or finished
                continue
            if all(dep in self.futures and self.futures[dep].done() for dep in getattr(s, "dependencies", [])):
                kw = {}
                if "parent_results" in inspect.signature(s.run).parameters:
                    kw["parent_results"] = {d: self.scripts[d]._result for d in s.dependencies}

                resources = getattr(s, "resources", None) or {}
                with annotate(resources=resources):
                    self.futures[s.name] = self.client.submit(_script_runner, s, **kw, key=s.name)

                if initial:
                    logging.getLogger("runner").info("submitted %s", s.name)


# ---------------------------------------------------------------------------
# 4.  Dash dashboard (DAG + live logs)
# ---------------------------------------------------------------------------

def create_dashboard(client: Client, log_queue: "queue.Queue[str]") -> dash.Dash:
    cyto.load_extra_layouts()
    app = dash.Dash(__name__)
    app.layout = html.Div([
        html.H2("Pipeline Dashboard"),
        html.Div(id="progress"),
        cyto.Cytoscape(id="dag", layout={"name": "dagre"}, style={"height": "500px"}),
        html.H3("Logs"),
        dcc.Textarea(id="logbox", style={"width": "100%", "height": "300px"}),
        dcc.Interval(id="interval", interval=1000, n_intervals=0),
    ])

    # ---------------- helpers ----------------------------------------
    def current_elements():
        info = client.scheduler_info()
        tasks = info.get("tasks", {})
        nodes, edges = [], []
        for key, t in tasks.items():
            state = t["state"]
            colour = {
                "memory": "green",
                "executing": "orange",
                "error": "red",
            }.get(state, "grey")
            nodes.append({"data": {"id": key, "label": key}, "style": {"background-color": colour}})
            for dep in t["dependencies"]:
                edges.append({"data": {"source": dep, "target": key}})
        return nodes + edges, sum(t["state"] == "memory" for t in tasks.values()), len(tasks)

    # ---------------- callbacks --------------------------------------
    @app.callback(
        Output("dag", "elements"),
        Output("progress", "children"),
        Input("interval", "n_intervals"),
    )
    def refresh_graph(_):
        elements, done, total = current_elements()
        return elements, f"{done}/{total} tasks complete"

    @app.callback(
        Output("logbox", "value"),
        Input("interval", "n_intervals"),
        State("logbox", "value"),
        prevent_initial_call=True,
    )
    def stream_logs(_, current):
        lines: list[str] = []
        try:
            while True:
                lines.append(log_queue.get_nowait())
        except queue.Empty:
            pass
        if not lines:
            return dash.no_update
        return (current or "") + "\n".join(lines) + "\n"

    return app


# ---------------------------------------------------------------------------
# 5.  __main__ — glue everything together
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # ------------------------------------------------------------------ cluster
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=1,
        processes=True,           # each worker its own process
        memory_limit="4GB",
    )
    client = Client(cluster)

    # ------------------------------------------------------------------ pipeline
    try:
        # You need to provide a "pipeline" object in pipeline_definition.py
        from pipeline_definition import pipeline  # noqa: F401  # type: ignore
    except ImportError as exc:
        raise SystemExit("No pipeline found. Create pipeline_definition.py with a `pipeline` variable.") from exc

    # ------------------------------------------------------------------ runner
    runner = DynamicPipelineRunner(pipeline, client=client)
    orchestrator = threading.Thread(target=runner.run, daemon=True)
    orchestrator.start()

    # ------------------------------------------------------------------ dashboard
    app = create_dashboard(client, LOG_QUEUE)
    app.run_server(debug=True, port=8051)
