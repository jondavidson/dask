# pipeline_service.py
"""Unified dynamic‑pipeline server & CLI.

Run one of three sub‑commands:

```bash
# 1) Start the long‑running service (scheduler + workers + dashboard)
python pipeline_service.py serve --n-workers 8 --port 8051 &

# 2) Submit a pipeline definition (any .py that exposes `pipeline`)
python pipeline_service.py submit /path/to/pipeline_definition.py

# 3) Gracefully shut down the service
python pipeline_service.py stop
```

• The **service** spins up a `LocalCluster`, a Dash dashboard, and writes
  its connection details to `~/.pipeline_server.json` so later `submit`
  commands (e.g. from cron) know where to connect.
• Each `submit` loads the user file, pulls out the `pipeline` variable
  (containing `Pipeline` + `Script` objects), and runs it on the already‑
  existing cluster via `DynamicPipelineRunner`.
• `stop` sends SIGTERM to the server PID and cleans up the state file.

Dependencies
------------
```bash
pip install "dask[distributed]" dash dash-cytoscape cloudpickle
```
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import queue
import signal
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Callable

import cloudpickle
import dash
import dash_cytoscape as cyto
from dash import dcc, html, Output, Input, State
from dask import annotate
from dask.distributed import Client, LocalCluster, Future, as_completed

###############################################################################
# 0.  GLOBAL LOGGING (QueueHandler so GUI can tail everything)                #
###############################################################################
LOG_Q: "queue.Queue[str]" = queue.Queue(maxsize=100_000)
root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(logging.handlers.QueueHandler(LOG_Q))
root.addHandler(logging.StreamHandler())

###############################################################################
# 1.  Helper – execute a Python callable in its *own* interpreter if desired  #
###############################################################################

def _run_callable_subproc(fn: Callable[..., Any], **kwargs):
    """Serialize *fn* → run → return result (or raise) in a fresh process."""
    import multiprocessing as mp, traceback

    def _child(pipe, blob: bytes):
        fn, kwargs = cloudpickle.loads(blob)
        try:
            res = fn(**kwargs)
            pipe.send(("ok", res))
        except Exception as exc:  # noqa: BLE001
            pipe.send(("err", (exc, traceback.format_exc())))
        finally:
            pipe.close()

    parent, child = mp.Pipe()
    ctx = mp.get_context("spawn")  # safest across OSes
    p = ctx.Process(target=_child, args=(child, cloudpickle.dumps((fn, kwargs))))
    p.start(); child.close()

    status, payload = parent.recv(); p.join()
    if status == "ok":
        return payload
    exc, tb = payload
    raise RuntimeError(tb) from exc

###############################################################################
# 2.  Worker‑side wrapper – captures stdout/stderr/logs                      #
###############################################################################

def _script_worker(script, **kwargs):
    import io, contextlib, json as _json, logging as _logging, time as _time

    logger = _logging.getLogger(script.name)
    logger.setLevel(_logging.INFO)
    buf = io.StringIO()
    handler = _logging.StreamHandler(buf)
    handler.setFormatter(_logging.Formatter(_json.dumps({
        "ts": "%(asctime)s", "lvl": "%(levelname)s", "script": script.name, "msg": "%(message)s"}))
    )
    logger.addHandler(handler)

    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        t0 = _time.time(); logger.info("started")
        result = script.run(**kwargs)
        logger.info("done in %.2fs", _time.time() - t0)

    buf.seek(0)
    for line in buf:
        _logging.getLogger("distributed.scriptlogs").info(line.rstrip())
    return result

###############################################################################
# 3.  DynamicPipelineRunner – supports on‑the‑fly DAG expansion               #
###############################################################################
class DynamicPipelineRunner:
    """Executes a Pipeline on a Dask cluster, allowing dynamic fan‑out."""
    def __init__(self, pipeline, client: Client):
        self.client = client
        self.scripts: Dict[str, Any] = {s.name: s for s in pipeline.scripts}
        self.futures: Dict[str, Future] = {}
        self._validate()

    def _validate(self):
        for s in self.scripts.values():
            bad = set(getattr(s, "dependencies", [])) - self.scripts.keys()
            if bad:
                raise ValueError(f"{s.name} depends on {bad}")

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _is_script_iter(obj):
        try:
            return bool(obj) and all(hasattr(x, "run") and hasattr(x, "name") for x in obj)
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------ main run
    def run(self):
        self._submit_ready(initial=True)
        for fut in as_completed(self.futures.values()):
            name = fut.key
            try:
                res = fut.result()
            except Exception as exc:  # noqa: BLE001
                logging.getLogger("runner").error("%s failed: %s", name, exc, exc_info=exc)
                continue
            if self._is_script_iter(res):
                for s in res:          # dynamically add scripts
                    self.add_script(s, parent=name)
            else:
                self.scripts[name]._result = res
            self._submit_ready()

    # ------------------------------------------------------------------ submit
    def _submit_ready(self, *, initial=False):
        for s in self.scripts.values():
            if s.name in self.futures:
                continue
            if all(dep in self.futures and self.futures[dep].done() for dep in getattr(s, "dependencies", [])):
                kw = {}
                sig = inspect.signature(s.run)
                if "parent_results" in sig.parameters:
                    kw["parent_results"] = {d: self.scripts[d]._result for d in s.dependencies}
                resrc = getattr(s, "resources", {})
                with annotate(resources=resrc):
                    self.futures[s.name] = self.client.submit(_script_worker, s, **kw, key=s.name)
                if initial:
                    logging.getLogger("runner").info("submitted %s", s.name)

    # ------------------------------------------------------------------ add
    def add_script(self, script, parent=None):
        if script.name in self.scripts:
            raise ValueError(f"duplicate {script.name}")
        self.scripts[script.name] = script
        if parent and parent not in script.dependencies:
            script.dependencies.append(parent)

###############################################################################
# 4.  Dash dashboard – lives with the service                                #
###############################################################################

def make_dashboard(client: Client, log_queue: "queue.Queue[str]", port: int):
    cyto.load_extra_layouts()
    app = dash.Dash(__name__)
    app.layout = html.Div([
        html.H2("Pipeline Dashboard"),
        html.Div(id="progress"),
        cyto.Cytoscape(id="dag", layout={"name": "dagre"}, style={"height": "500px"}),
        html.H3("Logs"),
        dcc.Textarea(id="logbox", style={"width": "100%", "height": "300px"}),
        dcc.Interval(id="int", interval=1000, n_intervals=0),
    ])

    def _elements():
        tasks = client.scheduler_info().get("tasks", {})
        nodes, edges = [], []
        for k, t in tasks.items():
            colour = {"memory": "green", "error": "red", "executing": "orange"}.get(t["state"], "grey")
            nodes.append({"data": {"id": k, "label": k}, "style": {"background-color": colour}})
            edges += [{"data": {"source": d, "target": k}} for d in t["dependencies"]]
        return nodes + edges, sum(t["state"] == "memory" for t in tasks.values()), len(tasks)

    @app.callback(Output("dag", "elements"), Output("progress", "children"), Input("int", "n_intervals"))
    def refresh(_):
        els, done, total = _elements()
        return els, f"{done}/{total} done"

    @app.callback(Output("logbox", "value"), Input("int", "n_intervals"), State("logbox", "value"), prevent_initial_call=True)
    def tail(_, cur):
        lines = []
        try:
            while True:
                lines.append(log_queue.get_nowait())
        except queue.Empty:
            pass
        if not lines:
            return dash.no_update
        return (cur or "") + "\n".join(lines) + "\n"

    app.run_server(debug=False, port=port)

###############################################################################
# 5.  CLI entry point                                                        #
###############################################################################
STATE_FILE = Path.home() / ".pipeline_server.json"

def _serve(args):
    cluster = LocalCluster(n_workers=args.n_workers, threads_per_worker=1, processes=True, memory_limit="4GB")
    info = {"scheduler": cluster.scheduler_address, "pid": os.getpid(), "port": args.port}
    STATE_FILE.write_text(json.dumps(info))
    client = Client(cluster)
    dashboard = threading.Thread(target=make_dashboard, args=(client, LOG_Q, args.port), daemon=True)
    dashboard.start()
    logging.info("Service up – scheduler=%s dashboard=http://localhost:%s", info["scheduler"], args.port)
    try:
        while True:  # keep process alive
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


def _submit(args):
    if not STATE_FILE.exists():
        raise SystemExit("Service not running – start with `serve` first.")
    info = json.loads(STATE_FILE.read_text())
    client = Client(info["scheduler"])

    mod_path = Path(args.pipeline_file).resolve()
    spec = importlib.util.spec_from_file_location("user_pipeline", mod_path)
    if spec is None or spec.loader is None:
        raise SystemExit("Cannot import pipeline file")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "pipeline"):
        raise SystemExit("pipeline_file must expose a `pipeline` variable")
    pipeline = module.pipeline

    DynamicPipelineRunner(pipeline, client).run()
    logging.info("Run complete")


def _stop(_):
    if not STATE_FILE.exists():
        print("No state file – already stopped?")
        return
    info = json.loads(STATE_FILE.read_text())
    os.kill(info["pid"], signal.SIGTERM)
    STATE_FILE.unlink(missing_ok=True)
    print("Service stopped")


def main():
    p = argparse.ArgumentParser(prog="pipeline_service")
    sp = p.add_subparsers(dest="cmd", required=True)

    srv = sp.add_parser("serve", help="start cluster + dashboard")
    srv.add_argument("--n-workers", type=int, default=4)
    srv.add_argument("--port", type=int, default=8051)

    sub = sp.add_parser("submit", help="submit a pipeline run")
    sub.add_argument("pipeline_file", help=".py file exposing `pipeline`")

    stp = sp.add_parser("stop", help="terminate the running service")

    args = p.parse_args()
    {
        "serve": _serve,
        "submit": _submit,
        "stop": _stop,
    }[args.cmd](args)


if __name__ == "__main__":
    main()
