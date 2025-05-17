from __future__ import annotations
from typing import Dict, List, Any, Iterable
from dask.distributed import Client, Future, as_completed
import logging, uuid, inspect

class DynamicPipelineRunner:
    """
    Orchestrates Script objects that *may* spawn more scripts at run-time.
    Each Script.run() can either return a plain result **or** an iterable
    of new Script objects to be scheduled.
    """
    def __init__(
        self,
        pipeline: "Pipeline",
        client: Client | None = None,
        client_kwargs: dict | None = None,
    ):
        self.client_owner = client is None
        self.client = client or Client(**(client_kwargs or {}))
        self.scripts: Dict[str, "Script"] = {s.name: s for s in pipeline.scripts}
        self.futures: Dict[str, Future] = {}
        self._validate_deps()

    # ---------- public API --------------------------------------------------
    def run(self) -> Dict[str, Any]:
        """Block until every (current or future) Script finishes."""
        self._submit_ready_scripts(initial=True)

        for fut in as_completed(self.futures.values()):
            name = fut.key
            try:
                result = fut.result()
            except Exception as exc:           # noqa: BLE001
                logging.getLogger("runner").error("%s failed: %s", name, exc, exc_info=exc)
                continue

            # If the Script returned new scripts → merge & schedule them.
            if self._looks_like_scripts(result):
                for s in result:
                    self.add_script(s, parent=name)
            else:
                self.scripts[name]._result = result        # stash for later

            self._submit_ready_scripts()

        if self.client_owner:          # tear down local cluster if we made it
            self.client.close()

        return {n: getattr(s, "_result", None) for n, s in self.scripts.items()}

    def add_script(self, script: "Script", parent: str | None = None) -> None:
        if script.name in self.scripts:
            raise ValueError(f"Duplicate script name {script.name}")
        self.scripts[script.name] = script
        # late-bound dependency on the parent (often useful but optional)
        if parent and script.name not in script.dependencies:
            script.dependencies.append(parent)

    # ---------- internals ---------------------------------------------------
    def _validate_deps(self) -> None:
        for s in self.scripts.values():
            missing = set(s.dependencies) - self.scripts.keys()
            if missing:
                raise ValueError(f"{s.name} depends on unknown scripts {missing}")

    def _submit_ready_scripts(self, *, initial: bool = False) -> None:
        """Find scripts whose parents are done and submit them."""
        for s in self.scripts.values():
            if s.name in self.futures:                         # already running / done
                continue
            if not s.dependencies or all(d in self.futures and self.futures[d].done()
                                         for d in s.dependencies):
                # pack parents’ results as kwargs if the Script wants them
                kw = {}
                if "parent_results" in inspect.signature(s.run).parameters:
                    kw["parent_results"] = {d: self.scripts[d]._result for d in s.dependencies}

                self.futures[s.name] = self.client.submit(
                    _script_runner, s, **kw, key=s.name
                )

                if initial:
                    logging.getLogger("runner").info("submitted %s", s.name)

    @staticmethod
    def _looks_like_scripts(obj: Any) -> bool:
        try:
            return bool(obj) and all(hasattr(x, "run") and hasattr(x, "name") for x in obj)
        except Exception:     # noqa: BLE001
            return False


def _script_runner(script: "Script", **kwargs):
    """
    Wrapper executed on the worker.
    We *also* capture stdout / stderr so they appear in central logs.
    """
    import sys, io, contextlib, logging, json, time

    logger = logging.getLogger(script.name)
    logger.setLevel(logging.INFO)

    # Send everything into a JSON-lines stream ⟶ Dask scheduler logger
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(
        json.dumps({
            "ts": "%(asctime)s",
            "lvl": "%(levelname)s",
            "script": script.name,
            "msg": "%(message)s"
        })
    ))
    logger.addHandler(handler)

    with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
        start = time.time()
        logger.info("started")
        out = script.run(**kwargs)          # user code
        logger.info("finished in %.2fs", time.time() - start)

    # flush the buffer so scheduler side picks it up
    stream.seek(0)
    for line in stream:
        logging.getLogger("distributed.scriptlogs").info(line.rstrip())

    return out
