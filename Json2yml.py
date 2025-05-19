#!/usr/bin/env python3
"""
json2yaml.py  –  Convert pipeline JSON configs to YAML.

Usage
-----
# single file → new file with .yaml extension
python json2yaml.py path/to/pipeline.json

# batch-convert every *.json inside a directory (recursively)
python json2yaml.py path/to/config_dir
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml


def convert_file(json_path: Path, *, overwrite: bool = False) -> Path:
    """Convert a single .json file → .yaml; return output path."""
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    yaml_path = json_path.with_suffix(".yaml")
    if yaml_path.exists() and not overwrite:
        sys.stderr.write(f"✖  {yaml_path} exists (use --overwrite to replace)\n")
        return yaml_path

    with yaml_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            data,
            f,
            sort_keys=False,         # preserve insertion order (Py≥3.7)
            default_flow_style=False,
            indent=2,
            allow_unicode=True,
        )
    print(f"✓  {json_path} → {yaml_path}")
    return yaml_path


def convert_tree(root: Path, overwrite: bool) -> None:
    for json_path in root.rglob("*.json"):
        convert_file(json_path, overwrite=overwrite)


def main() -> None:
    p = argparse.ArgumentParser(description="Convert pipeline JSON configs to YAML")
    p.add_argument("path", help="JSON file or directory containing JSON files")
    p.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing .yaml files"
    )
    args = p.parse_args()

    target = Path(args.path)
    if not target.exists():
        sys.exit(f"Path not found: {target}")

    if target.is_dir():
        convert_tree(target, overwrite=args.overwrite)
    else:
        if target.suffix.lower() != ".json":
            sys.exit("Input must be a .json file")
        convert_file(target, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
