# inside pipeline_framework.py  (Pipeline class)

def extract(
    self,
    include: set[str],
    *,
    rewire: bool = True,
) -> "Pipeline":
    """
    Return a new Pipeline containing only *include*.

    Parameters
    ----------
    include : nodes to keep.
    rewire  : if True (default) drop any dependency that
              points outside *include* so `depends_on`
              is self-contained.
    """
    new_nodes: dict[str, PipelineNode] = {}
    for name in include:
        node = dataclasses.replace(self.nodes[name])          # shallow copy
        if rewire:
            node.depends_on = [d for d in node.depends_on if d in include]
        new_nodes[name] = node
    return Pipeline(name=f"{self.name}_subset", nodes=new_nodes)
