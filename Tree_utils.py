# ── tree_utils.py ───────────────────────────────────────────────────────────
from collections import defaultdict
from typing import Dict, List, Set, Iterable
from pathlib import Path

def pipeline_tree(
    nodes: Dict[str, Iterable[str]],
    *,
    roots: Set[str] | None = None,
    indent: str = "   ",
) -> str:
    """
    Return a UTF-8 dependency tree.

    Parameters
    ----------
    nodes    : mapping node_name -> list/iterable of dependencies
    roots    : optional explicit set of starting nodes.  When None,
               any node with no dependencies is considered a root.
    indent   : string used per nesting level (default 3 spaces)

    The output duplicates converging nodes so each upstream path is
    shown independently—handy for eyeballing big DAGs quickly.
    """
    # build child-lookup for forward traversal
    children: Dict[str, List[str]] = defaultdict(list)
    for node, deps in nodes.items():
        for dep in deps:
            children[dep].append(node)

    for ch in children.values():
        ch.sort()                       # deterministic order

    # discover roots if not supplied
    if roots is None:
        roots = {n for n, deps in nodes.items() if not deps}
        if not roots:
            # fall back: no empty deps?  treat all nodes with missing deps as roots
            roots = set(nodes) - {d for deps in nodes.values() for d in deps}

    # pretty-printer
    lines: List[str] = []

    def _recurse(cur: str, prefix: str, is_last: bool) -> None:
        branch = "└─ " if is_last else "├─ "
        lines.append(f"{prefix}{branch}{cur}")
        next_prefix = prefix + (indent if is_last else "│" + indent[1:])
        kids = children.get(cur, [])
        for i, kid in enumerate(kids):
            _recurse(kid, next_prefix, i == len(kids) - 1)

    roots = sorted(roots)               # stable output
    for i, r in enumerate(roots):
        _recurse(r, "", i == len(roots) - 1)

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Example usage
if __name__ == "__main__":
    import yaml, textwrap
    yaml_src = textwrap.dedent("""
      nodes:
        - name: raw
          depends_on: []
        - name: fx
          depends_on: []
        - name: enriched
          depends_on: [raw, fx]
        - name: snapshot
          depends_on: [enriched]
        - name: dashboard
          depends_on: [snapshot]
    """)
    cfg = yaml.safe_load(yaml_src)
    mapping = {n["name"]: n.get("depends_on", []) for n in cfg["nodes"]}
    print(pipeline_tree(mapping))
from pipeline_framework import Pipeline
from tree_utils import pipeline_tree

pipe = load_pipeline_from_yaml("sales_pipeline.yaml")
nodes = {n: pipe.nodes[n].depends_on for n in pipe.nodes}
print(pipeline_tree(nodes))
