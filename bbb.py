from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Sequence, Mapping, Any, Iterable
import subprocess, shlex, os, logging, json, inspect


@dataclass
class Script:
    """
    A single executable step in the pipeline.

    Parameters
    ----------
    name : str
        Unique identifier inside one pipeline run.
    command : str | Callable[..., Any]
        • *str*  – a shell command, e.g. `"python train.py --epochs 5"`  
        • *callable* – any picklable function or bound method; its
          signature can include the **special** parameter `parent_results`
          (a dict of completed-parent outputs injected by the runner).
    cwd : str | Path | None, default None
        Working directory.  `None` ⇒ inherit worker’s CWD.
    env : Mapping[str, str] | None, default None
        Extra environment vars (merged over `os.environ`).
    dependencies : Sequence[str], default ()
        Names of scripts that must finish **before** this one starts.
    resources : dict | None, default None
        Dask resource annotations, e.g. `{"GPU": 1, "CPU": 4}`.
    log_json : bool, default True
        If *True*, each log record is wrapped as JSON so the GUI can
        parse it easily.
    meta : dict, default {}
        Free-form metadata (shown in the GUI tooltip, persisted, …).
    """
    name: str
    command: str | Callable[..., Any]
    cwd: str | Path | None = None
    env: Mapping[str, str] | None = None
    dependencies: Sequence[str] = field(default_factory=tuple)
    resources: Mapping[str, int] | None = None
    log_json: bool = True
    meta: Mapping[str, Any] = field(default_factory=dict)

    # populated by the runner (you don’t set these yourself)
    _result: Any = field(init=False, repr=False, default=None)
    _status: str = field(init=False, repr=False, default="pending")

    # --------------------------------------------------------------------- #
    # public API invoked by DynamicPipelineRunner on the worker
    # --------------------------------------------------------------------- #
    def run(self, *, parent_results: dict[str, Any] | None = None) -> Any | Iterable["Script"]:
        """
        Execute the script.

        Returns
        -------
        • *Any* – a plain result object (serialisable).  
        • *Iterable[Script]* – new Script objects to be scheduled
          dynamically (see DynamicPipelineRunner).

        Notes
        -----
        *Everything* happens inside the worker process, so keep heavy
        imports inside this method rather than at module-top if startup
        time matters.
        """
        logger = self._setup_logger()

        logger.info("starting (parents=%s)", list(parent_results or {}))
        self._status = "running"

        # ------------------------------------------------ dispatch -------- #
        if isinstance(self.command, str):
            result = self._run_shell(logger)
        else:  # callable
            result = self._run_callable(logger, parent_results or {})

        self._status = "done"
        logger.info("finished OK")
        return result

    # --------------------------------------------------------------------- #
    # internals                                                             #
    # --------------------------------------------------------------------- #
    def _run_shell(self, logger):
        """Execute *command* with subprocess and stream output to logs."""
        cmd = self.command if isinstance(self.command, str) else ""
        logger.debug("shell command: %s", cmd)

        proc = subprocess.Popen(
            shlex.split(cmd),
            cwd=self.cwd,
            env={**os.environ, **(self.env or {})},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        out_lines = []
        for line in proc.stdout:   # real-time streaming to central logger
            line = line.rstrip()
            out_lines.append(line)
            logger.info(line)

        proc.wait()
        if proc.returncode:
            logger.error("exit-code=%s => failing", proc.returncode)
            raise subprocess.CalledProcessError(proc.returncode, cmd)

        return "\n".join(out_lines)

    def _run_callable(self, logger, parent_results):
        """Call the Python function with injected kwargs if it asks for them."""
        fn: Callable[..., Any] = self.command  # type: ignore[assignment]
        sig = inspect.signature(fn)
        if "parent_results" in sig.parameters:
            result = fn(parent_results=parent_results)
        else:
            result = fn()
        logger.debug("callable returned type=%s", type(result))
        return result

    # --------------------------------------------------------------------- #
    def _setup_logger(self) -> logging.Logger:
        """
        Give each script its own logger whose records flow into the global
        QueueHandler configured by main().
        """
        logger = logging.getLogger(f"script.{self.name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:  # protect against double-installation
            fmt = (
                json.dumps({
                    "ts": "%(asctime)s",
                    "lvl": "%(levelname)s",
                    "script": self.name,
                    "msg": "%(message)s",
                })
                if self.log_json
                else "%(asctime)s — %(levelname)s — %(script)s — %(message)s"
            )
            handler = logging.StreamHandler()        # inherits QueueHandler up-chain
            handler.setFormatter(logging.Formatter(fmt))
            logger.addHandler(handler)
            logger.propagate = True
        return logger

    # convenient helper for visualisation ---------------------------------- #
    def to_cytoscape_node(self):
        colour = {
            "pending": "grey",
            "running": "orange",
            "done": "green",
            "failed": "red",
        }[self._status]
        return {
            "data": {
                "id": self.name,
                "label": self.name,
                **self.meta,
            },
            "style": {"background-color": colour},
            "classes": self._status,
        }
