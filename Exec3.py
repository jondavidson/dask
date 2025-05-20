# === In Script.run() ===

def run(
    self,
    *,
    sd: Optional[date] = None,
    ed: Optional[date] = None,
    override_args: Optional[Dict[str, Any]] = None,
    context: Optional[RunContext] = None
) -> None:
    import subprocess, sys
    from pathlib import Path

    args = {**self.default_args, **(override_args or {})}
    cmd = [sys.executable, str(self.path)]

    if self.dated:
        if not sd or not ed:
            raise ValueError(f"{self.name} requires sd/ed")
        args["--sd"], args["--ed"] = str(sd), str(ed)

    for k, v in args.items():
        cmd.extend([str(k), str(v)])

    log_dir = context.log_dir if context else Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{self.name}.log"

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc, log_path.open("a", encoding="utf-8") as log_file:
        for line in proc.stdout or []:
            print(line, end="")         # visible in Dask GUI
            log_file.write(line)
        proc.wait()
        if proc.returncode:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


# === In PipelineRunner._exec_with_retry() ===

@staticmethod
def _exec_with_retry(
    exec_obj: Executable,
    sd: date,
    ed: date,
    overrides: Dict[str, Any],
    max_retries: int,
    backoff: float,
    context: RunContext,
    *_parents,
) -> str:
    import time
    from distributed import get_worker

    attempt = 0
    try:
        worker = get_worker()
    except ValueError:
        worker = None

    while True:
        try:
            if worker:
                worker.log_event(
                    key=f"{context.label}:{exec_obj.name}:{sd}",
                    msg="Starting script run"
                )

            exec_obj.run(sd=sd, ed=ed, override_args=overrides, context=context)

            if worker:
                worker.log_event(
                    key=f"{context.label}:{exec_obj.name}:{sd}",
                    msg="Finished script successfully"
                )
            return f"{exec_obj.name}:{sd}"

        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                if worker:
                    worker.log_event(
                        key=f"{context.label}:{exec_obj.name}:{sd}",
                        msg=f"Failed after {attempt} attempts: {exc}"
                    )
                raise
            if worker:
                worker.log_event(
                    key=f"{context.label}:{exec_obj.name}:{sd}",
                    msg=f"Retry {attempt} after error: {exc}"
                )
            time.sleep(backoff)

# === In PipelineRunner.run(), building the delayed task ===

from dask import delayed

delayed_task = delayed(self._exec_with_retry, name=f"{context.label}:{node.name}:{task.sd}")(
    node.executable,
    task.sd,
    task.ed,
    override_args.get(task.node, {}),
    self.max_retries,
    self.backoff_seconds,
    context,
    *parents
)
## OR...

@delayed(name=f"{context.label}:{node.name}:{task.sd}")
def task_wrapper():
    return self._exec_with_retry(...)

delayed_task = task_wrapper(*parents)

# json summary 
import json

def run(...):
    ...
    results = []
    for (node, _), fut in zip(delayed_map.keys(), futures):
        try:
            result = fut.result()
            results.append({"name": node, "status": "success", "result": result})
        except Exception as e:
            results.append({"name": node, "status": "failed", "error": str(e)})

    summary = {
        "run_id": context.run_id,
        "start_time": context.start_time.isoformat(),
        "end_time": datetime.utcnow().isoformat(),
        "status": "success" if all(r["status"] == "success" for r in results) else "partial_failure",
        "tasks": results,
    }

    summary_path = context.log_dir / "run_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    return futures_by_node

  
