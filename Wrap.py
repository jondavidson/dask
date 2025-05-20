@delayed(name=f"{context.label}:{node.name}:{task.sd}")
def wrapped():
    return self._exec_with_retry(
        node.executable,
        task.sd,
        task.ed,
        override_args.get(task.node, {}),
        self.max_retries,
        self.backoff_seconds,
        context,
    )

delayed_task = wrapped(*parents)
