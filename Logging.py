# === common_logging.py ===

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

def setup_logging(
    name: Optional[str] = None,
    *,
    level: int = logging.INFO,
    to_stdout: bool = True,
    log_dir: Path = Path("logs"),
    max_bytes: int = 5_000_000,
    backup_count: int = 3
) -> logging.Logger:
    name = name or Path(sys.argv[0]).stem
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    if to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}.log"
    fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


# === run_context.py ===

import uuid
import random
import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

_WORDLIST = [
    "echo", "zebra", "frost", "gala", "tango", "blitz", "orbit", "quartz", "delta", "hazel",
    "lunar", "mango", "oxide", "pixel", "raven", "sable", "terra", "vapor", "willow", "yodel"
]

def generate_human_readable_id() -> str:
    return "-".join(random.choices(_WORDLIST, k=3))

def timestamp_id() -> str:
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

@dataclass
class RunContext:
    pipeline_name: str
    run_id: str
    label: str
    start_time: datetime.datetime
    log_dir: Path

    @classmethod
    def create(
        cls,
        pipeline_name: str,
        base_log_dir: Path = Path("logs"),
        label: Optional[str] = None
    ) -> "RunContext":
        now = datetime.datetime.utcnow()
        ts = timestamp_id()
        uid = generate_human_readable_id()
        label = label or uid
        run_id = f"{pipeline_name}_{ts}_{uid}"
        log_dir = base_log_dir / run_id
        return cls(
            pipeline_name=pipeline_name,
            run_id=run_id,
            label=label,
            start_time=now,
            log_dir=log_dir
        )


# === run_example.py ===

from pathlib import Path
from datetime import date
from dask.distributed import Client, LocalCluster

from common_logging import setup_logging
from run_context import RunContext

# Minimal Script/Pipeline/Runner classes for demo
class Script:
    def __init__(self, name, path, dated=False, default_args=None):
        self.name = name
        self.path = path
        self.dated = dated
        self.default_args = default_args or {}

    def run(self, sd=None, ed=None, override_args=None, context=None):
        log_dir = context.log_dir if context else Path("logs")
        log_file = log_dir / f"{self.name}.log"
        log_dir.mkdir(parents=True, exist_ok=True)
        cmd = ["python", str(self.path)]
        with open(log_file, "w", encoding="utf-8") as f:
            import subprocess
            subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, text=True, check=True)

class PipelineNode:
    def __init__(self, name, executable, depends_on=None, requires_all_dates=False):
        self.name = name
        self.executable = executable
        self.depends_on = depends_on or []
        self.requires_all_dates = requires_all_dates

class Pipeline:
    def __init__(self, name, nodes):
        self.name = name
        self.nodes = nodes

class PipelineRunner:
    def __init__(self, pipeline, client):
        self.pipeline = pipeline
        self.client = client

    def run(self, context, sd, ed, subset=None, plan_only=False):
        from dask import delayed
        tasks = {}
        for name, node in self.pipeline.nodes.items():
            @delayed
            def task(exec=node.executable, context=context):
                exec.run(context=context)
                return f"{context.run_id}:{exec.name}"
            tasks[name] = task()
        futures = self.client.compute(list(tasks.values()))
        from dask.distributed import wait
        wait(futures)

# Create dummy script
script_path = Path("scripts/hello_world.py")
script_path.parent.mkdir(parents=True, exist_ok=True)
script_path.write_text("""\
from common_logging import setup_logging
logger = setup_logging("hello_world")
logger.info("Hello from subprocess!")
""")

# Create pipeline with single node
script = Script(name="hello", path=script_path)
node = PipelineNode(name="hello", executable=script)
pipeline = Pipeline(name="test_pipeline", nodes={"hello": node})

# Start Dask
cluster = LocalCluster(n_workers=1, threads_per_worker=1, dashboard_address=":8787")
client = Client(cluster)
print("Dashboard:", client.dashboard_link)

# Create run context
context = RunContext.create(pipeline_name=pipeline.name)
print("Run ID:", context.run_id)
print("Log dir:", context.log_dir)

# Log from runner itself
setup_logging("runner", log_dir=context.log_dir)
print("Running pipeline...")

# Execute pipeline
runner = PipelineRunner(pipeline, client)
runner.run(context=context, sd=date.today(), ed=date.today())

print("Done. Check logs in:", context.log_dir)
