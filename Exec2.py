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
    from distributed import get_worker
    try:
        worker = get_worker()
    except ValueError:
        worker = None

    attempt = 0
    while True:
        try:
            if worker:
                worker.log_event(
                    key=f"{context.label}:{exec_obj.name}:{sd}",
                    msg="Starting task"
                )

            exec_obj.run(sd=sd, ed=ed, override_args=overrides, context=context)

            if worker:
                worker.log_event(
                    key=f"{context.label}:{exec_obj.name}:{sd}",
                    msg="Finished task successfully"
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
