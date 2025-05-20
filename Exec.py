# ---------- inside PipelineRunner (replace the old helper) ------------------

import subprocess, sys, os

@staticmethod
def _exec_with_retry(
    exec_obj: Executable,
    sd: date,
    ed: date,
    overrides: Dict[str, Any],
    max_retries: int,
    backoff: float,
    context: RunContext,
    *_parents,          # dependencies (ignored, enforce order only)
) -> str:
    """
    Execute *any* Executable in an isolated subprocess.

    Benefits of the Popen approach
    ------------------------------
    • **Process isolation**        – frees Dask worker memory immediately
    • **No thread contention**     – Polars, BLAS, Mosek stay in their own proc
    • **Crash containment**        – a seg-fault or OOM kills only the child
    • **Live log streaming**       – lines are piped to the runner + dashboard
    """
    attempt = 0
    # Build the command to run this node
    # We write a tiny shim to import the Executable and call .run() inside the child
    shim = (
        "import importlib, pickle, sys;"
        "exec_obj, sd, ed, overrides, ctx = pickle.load(sys.stdin.buffer);"
        "exec_obj.run(sd=sd, ed=ed, override_args=overrides, context=ctx)"
    )

    # Pack everything needed by the child
    payload = pickle.dumps((exec_obj, sd, ed, overrides, context))

    while True:
        try:
            proc = subprocess.Popen(
                [sys.executable, "-c", shim],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            assert proc.stdin is not None
            proc.stdin.write(payload.decode("latin1"))
            proc.stdin.close()

            # Stream child output -> parent stdout (visible in dashboard)
            for line in proc.stdout or []:
                print(line, end="")

            proc.wait()
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, shim)

            return f"{exec_obj.name}:{sd}"

        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                raise
            logger.warning(
                "Retry %s (%d/%d) after error: %s",
                exec_obj.name, attempt, max_retries, exc
            )
            time.sleep(backoff)
