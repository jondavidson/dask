# pipeline_service.py
"""Unified dynamic‑pipeline server & CLI – **Streamlit** edition.

Sub‑commands
------------
```bash
# 1) long‑running background service (scheduler + workers + Streamlit UI)
python pipeline_service.py serve --n-workers 8 --port 8501 &

# 2) submit a pipeline definition (needs a `pipeline` variable)
python pipeline_service.py submit /path/to/pipeline_def.py

# 3) stop the service gracefully
python pipeline_service.py stop
```

• The *service* spins up a local `dask.distributed` cluster, starts a
  Streamlit dashboard, and writes its connection info to
  `~/.pipeline_server.json` so later `submit` calls (e.g. from **cron**)
  can find the scheduler.
• Each *submit* loads the user file, extracts `pipeline` (built from your
  `Pipeline` & `Script` objects) and executes it via `DynamicPipelineRunner`.
• *stop* sends SIGTERM to the service PID and deletes the state file.

Dependencies
------------
```bash
pip install "dask[distributed]" streamlit cloudpickle graphviz
```

> **Note** Graph‑viz is used for a quick DAG rendering. Install the
> system package (e.g. `apt‑get install graphviz`) if the chart is blank.
"""
from __future__ import annotations

###############################################################################
# 0.  Imports & global logging                                               #
###############################################################################

import argparse
import importlib.util
import inspect
import json
import logging
import os
import queue
import signal
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict

import cloudpickle
from dask import annotate
from dask.distributed import Client, LocalCluster, Future, as_completed

###############################################################################
# logging: everything goes to console + file + in‑memory Queue (for UI)      #
###############################################################################
LOG_Q: "queue.Queue[str]" = queue.Queue(maxsize=100_000)
LOG_FILE = Path.home() / "pipeline_service.log"
root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(logging.handlers.QueueHandler(LOG_Q))
root.addHandler(logging.StreamHandler())
root.addHandler(logging.FileHandler(LOG_FILE))

###############################################################################
# 1.  (Optional) execute Python callable in its *own* process                #
###############################################################################

def _run_callable_subproc(fn: Callable[..., Any], **kwargs):
    """Serialize *fn* → run in fresh interpreter → return result / raise."""
    import multiprocessing as mp, traceback

    def _child(pipe, blob):
        fn, kwargs = cloudpickle.loads(blob)
        try:
            out = fn(**kwargs)
            pipe.send(("ok", out))
        except Exception as exc:  # noqa: BLE001
            pipe.send(("err", (exc, traceback.format_exc())))
        finally:
            pipe.close()

    parent, child = mp.Pipe()
    ctx = mp.get_context("spawn")
    p = ctx.Process(target=_child, args=(child, cloudpickle.dumps((fn, kwargs))))
    p.start(); child.close()

    status, payload = parent.recv(); p.join()
    if status == "ok":
        return payload
    exc, tb = payload
    raise RuntimeError(tb) from exc

###############################################################################
# 2.  Worker‑side task wrapper – captures logs                               #
###############################################################################

def _script_worker(script, **kwargs):
    import io, contextlib, json as _j, logging as _l, time as _t
    logger = _l.getLogger(script.name); logger.setLevel(_l.INFO)

    buf = io.StringIO()
    handler = _l.StreamHandler(buf)
    handler.setFormatter(_l.Formatter(_j.dumps({
        "ts": "%(asctime)s", "lvl": "%(levelname)s", "script": script.name, "msg": "%(message)s"})))
    logger.addHandler(handler)

    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        t0 = _t.time(); logger.info("started")
        res = script.run(**kwargs)
        logger.info("finished in %.2fs", _t.time() - t0)

    buf.seek(0)
    for line in buf:
        _l.getLogger("distributed.scriptlogs").info(line.rstrip())
    return res

###############################################################################
# 3.  DynamicPipelineRunner – allows DAG expansion at run‑time               #
###############################################################################
class DynamicPipelineRunner:
    """Submit + monitor a Pipeline on a Dask cluster (dynamic)."""

    def __init__(self, pipeline, client: Client):
        self.client = client
        self.scripts: Dict[str, Any] = {s.name: s for s in pipeline.scripts}
        self.futures: Dict[str, Future] = {}
        self._validate()

    # ---- helper -----------------------------------------------------------
    def _validate(self):
        for s in self.scripts.values():
            bad = set(getattr(s, "dependencies", [])) - self.scripts.keys()
            if bad:
                raise ValueError(f"{s.name} depends on missing {bad}")

    @staticmethod
    def _is_script_iter(obj):
        try:
            return bool(obj) and all(hasattr(x, "run") and hasattr(x, "name") for x in obj)
        except Exception:  # noqa: BLE001
            return False

    # ---- main ------------------------------------------------------------
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
                for s in res:
                    self.add_script(s, parent=name)
            else:
                self.scripts[name]._result = res
            self._submit_ready()

    # ---- private ---------------------------------------------------------
    def _submit_ready(self, *, initial=False):
        for s in self.scripts.values():
            if s.name in self.futures:
                continue
            if all(dep in self.futures and self.futures[dep].done() for dep in getattr(s, "dependencies", [])):
                kw = {}
                sig = inspect.signature(s.run)
                if "parent_results" in sig.parameters:
                    kw["parent_results"] = {d: self.scripts[d]._result for d in s.dependencies}
                resources = getattr(s, "resources", {})
                with annotate(resources=resources):
                    self.futures[s.name] = self.client.submit(_script_worker, s, **kw, key=s.name)
                if initial:
                    logging.getLogger("runner").info("submitted %s", s.name)

    # ---- external --------------------------------------------------------
    def add_script(self, script, parent=None):
        if script.name in self.scripts:
            raise ValueError(f"duplicate {script.name}")
        self.scripts[script.name] = script
        if parent and parent not in script.dependencies:
            script.dependencies.append(parent)

###############################################################################
# 4.  Streamlit dashboard launcher                                           #
###############################################################################

def _launch_streamlit(client: Client, port: int):
    """Generate a dashboard script and launch Streamlit in a background process."""

    # Use scheduler address via public API; avoids attribute errors on remote clusters
    scheduler_address = client.scheduler_info()["address"]

    ui_code = f"""
import streamlit as st, time, graphviz
from pathlib import Path
from dask.distributed import Client

SCHED = {repr(scheduler_address)}
LOG_FILE = Path({repr(str(LOG_FILE))})
client = Client(SCHED)

st.set_page_config(page_title="Pipeline Dashboard", layout="wide")
st.title("Pipeline Dashboard (Streamlit)")
refresh = st.sidebar.slider("Refresh interval (s)", 1, 10, 2)

def _build_graph():
    tasks = client.scheduler_info().get("tasks", {})
    dot = ["digraph g {"]
    done = 0
    for k, t in tasks.items():
        colour = {"memory":"green","error":"red","executing":"orange"}.get(t["state"],"grey")
        if t["state"] == "memory":
            done += 1
        dot.append(f'  "{k}" [style=filled fillcolor={colour}];')
        for d in t["dependencies"]:
            dot.append(f'  "{d}" -> "{k}";')
    dot.append("}")
    return "\n".join(dot), done, len(tasks)

if "last_pos" not in st.session_state:
    st.session_state.last_pos = 0

graph_ph = st.empty()
progress_ph = st.empty()
log_ph = st.empty()

while True:
    dot, done, total = _build_graph()
    graph_ph.graphviz_chart(dot)
    progress_ph.markdown(f"**Completed:** {done}/{total}")

    if LOG_FILE.exists():
        with LOG_FILE.open() as f:
            f.seek(st.session_state.last_pos)
            new = f.read()
            st.session_state.last_pos = f.tell()
        if new:
            log_ph.text_area("Logs", value=new + log_ph.text_area("Logs"), height=250)
    time.sleep(refresh)
"""

    tmp = Path(tempfile.gettempdir()) / f"pipeline_dashboard_{port}.py"
    tmp.write_text(ui_code)

    subprocess.Popen([
        "streamlit", "run", str(tmp),
        "--server.port", str(port),
        "--server.headless", "true",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

###############################################################################
# 5.  CLI                                                                    #
###############################################################################
STATE = Path.home() / ".pipeline_server.json"

# ------------------------------------------------------------------ serve
def _cmd_serve(args):
    cluster = LocalCluster(
        n_workers=args.n_workers,
        threads_per_worker=1,
        processes=True,
        memory_limit="4GB",
    )
    info = {"scheduler": cluster.scheduler_address,
            "pid": os.getpid(),
            "port": args.port}
    STATE.write_text(json.dumps(info))

    client = Client(cluster)

    ui_path = Path(args.ui_file).expanduser().resolve()
    if not ui_path.exists():
        raise SystemExit(f"UI file {ui_path} not found")
    _launch_streamlit(ui_path, args.port)

    logging.info(
        "Service ready – UI http://localhost:%s  scheduler=%s",
        args.port, info["scheduler"]
    )

    # Keep the service process alive until SIGINT/SIGTERM
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.info("Interrupted; shutting down …")

# ------------------------------------------------------------------ submit
def _cmd_submit(args):
    if not STATE.exists():
        raise SystemExit("Service not running – start with `serve`.")

    info = json.loads(STATE.read_text())
    client = Client(info["scheduler"])

    py_path = Path(args.pipeline_file).expanduser().resolve()
    spec = importlib.util.spec_from_file_location("user_pipeline", py_path)
    if spec is None or spec.loader is None:
        raise SystemExit("Cannot import pipeline file")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)                     # type: ignore[arg-type]

    if not hasattr(module, "pipeline"):
        raise SystemExit("pipeline file must define a `pipeline` variable")

    # Execute the run on the live cluster
    DynamicPipelineRunner(module.pipeline, client).run()
    logging.info("Run complete")


# ------------------------------------------------------------------ stop
def _cmd_stop(_):
    if not STATE.exists():
        print("Service not running")
        return
    info = json.loads(STATE.read_text())
    os.kill(info["pid"], signal.SIGTERM)
    STATE.unlink(missing_ok=True)
    print("Service stopped")

# ------------------------------------------------------------------ main
def main():
    parser = argparse.ArgumentParser(prog="pipeline_service")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="start cluster + Streamlit UI")
    p_serve.add_argument("--n-workers", type=int, default=4)
    p_serve.add_argument("--port", type=int, default=8501)
    p_serve.add_argument("--ui-file",
                         default="pipeline_dashboard.py",
                         help="Streamlit dashboard file")

    p_sub = sub.add_parser("submit", help="submit a pipeline run")
    p_sub.add_argument("pipeline_file",
                       help=".py file exposing a `pipeline` variable")

    sub.add_parser("stop", help="stop the running service")

    args = parser.parse_args()
    {
        "serve": _cmd_serve,
        "submit": _cmd_submit,
        "stop":  _cmd_stop,
    }[args.cmd](args)

if __name__ == "__main__":
    main()

