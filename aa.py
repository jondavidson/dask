from __future__ import annotations
from typing import Dict, List, Any
from dask.distributed import Client
from dask import delayed

def run_pipeline(
    pipeline: "Pipeline",
    client: Client | None = None,
    client_kwargs: dict | None = None,
) -> Dict[str, Any]:
    """
    Execute all Script objects in *pipeline.scripts* with their declared
    dependencies using Dask Distributed.

    Parameters
    ----------
    pipeline : Pipeline
        An object that exposes an iterable ``pipeline.scripts``.
    client : dask.distributed.Client, optional
        Re-use an existing client. If *None*, a temporary in-process cluster
        is started with **client_kwargs**.
    client_kwargs : dict, optional
        Extra kwargs for ``dask.distributed.Client`` when we create it.

    Returns
    -------
    Dict[str, Any]
        Mapping ``{script_name: result}`` in the order scripts were executed.
    """
    # ------------------------------------------------------------------ setup
    owns_client = False
    if client is None:
        client = Client(**(client_kwargs or {}))  # e.g. {"n_workers": 4}
        owns_client = True

    scripts = {s.name: s for s in pipeline.scripts}

    # Sanity-check dependencies
    for s in scripts.values():
        unknown = set(getattr(s, "dependencies", [])) - scripts.keys()
        if unknown:
            raise ValueError(f"Script {s.name!r} depends on unknown scripts {unknown}")

    # ------------------------------------------------------------------ DAG
    delayed_nodes: Dict[str, "delayed"] = {}

    def build_node(script: "Script") -> "delayed":
        """Return (and cache) the dask.delayed node for *script*."""
        if script.name in delayed_nodes:
            return delayed_nodes[script.name]

        # build parents first
        parent_nodes: List["delayed"] = [
            build_node(scripts[parent]) for parent in getattr(script, "dependencies", [])
        ]

        # Dask enforces order because parent_nodes are arguments
        node = delayed(lambda *_: script.run(), nout=1, name=script.name)(*parent_nodes)
        delayed_nodes[script.name] = node
        return node

    # Kick off graph construction
    graph_roots = [build_node(s) for s in scripts.values()]

    # ------------------------------------------------------------------ run
    # compute returns results in the same order we passed the roots
    results: List[Any] = client.compute(graph_roots, sync=True)
    out = {name: res for name, res in zip(delayed_nodes, results)}

    if owns_client:
        client.close()

    return out


if __name__ == "__main__":
    pipe = Pipeline([...])          # your Pipeline instance
    result_map = run_pipeline(pipe, client_kwargs={"n_workers": 8})
    
    for name, res in result_map.items():
        print(f"{name:15s} ➜ {res}")

    # graph = build_node(some_root_script)
    # graph.visualize(rankdir="LR").render("pipeline_dag", format="pdf")
