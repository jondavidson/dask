import json from pathlib import Path from datetime import datetime import argparse

def list_runs(log_root: Path, pipeline: str = None, limit: int = 10, as_json: bool = False): runs = []

for run_dir in sorted(log_root.glob(f"{pipeline or '*'}_*_*"), reverse=True):
    summary_path = run_dir / "run_summary.json"
    if not summary_path.exists():
        continue
    try:
        with summary_path.open() as f:
            summary = json.load(f)

        failed_tasks = [t for t in summary.get("tasks", []) if t.get("status") != "success"]

        runs.append({
            "run_id": summary.get("run_id"),
            "start": summary.get("start_time"),
            "end": summary.get("end_time"),
            "status": summary.get("status"),
            "log_dir": str(run_dir),
            "fail_count": len(failed_tasks),
            "failed_tasks": failed_tasks,
            "all_tasks": summary.get("tasks", [])
        })
    except Exception as e:
        print(f"Warning: Failed to read {summary_path}: {e}")

runs = sorted(runs, key=lambda r: r["start"], reverse=True)

if as_json:
    print(json.dumps(runs[:limit], indent=2))
else:
    for r in runs[:limit]:
        start = datetime.fromisoformat(r["start"]).strftime("%Y-%m-%d %H:%M:%S")
        fail_note = f" | {r['fail_count']} failed" if r['fail_count'] else ""
        print(f"[{r['status']:<16}] {start} | {r['run_id']}{fail_note} | logs: {r['log_dir']}")

def compare_runs(run_dir1: Path, run_dir2: Path): try: summary1 = json.load((run_dir1 / "run_summary.json").open()) summary2 = json.load((run_dir2 / "run_summary.json").open()) tasks1 = {t['name']: t for t in summary1.get("tasks", [])} tasks2 = {t['name']: t for t in summary2.get("tasks", [])}

all_keys = sorted(set(tasks1) | set(tasks2))
    print("Task Comparison:")
    for key in all_keys:
        s1 = tasks1.get(key, {}).get("status", "missing")
        s2 = tasks2.get(key, {}).get("status", "missing")
        print(f"{key:<20}: {s1:<12} → {s2:<12}")
except Exception as e:
    print(f"Failed to compare runs: {e}")

if name == "main": parser = argparse.ArgumentParser(description="List recent pipeline runs.") parser.add_argument("--log-dir", default="logs", help="Root logs/ directory") parser.add_argument("--pipeline", help="Filter by pipeline name") parser.add_argument("--limit", type=int, default=10) parser.add_argument("--json", action="store_true", help="Output in machine-readable JSON format") parser.add_argument("--diff", nargs=2, metavar=('RUN1', 'RUN2'), help="Compare two run folders") args = parser.parse_args()

if args.diff:
    compare_runs(Path(args.diff[0]), Path(args.diff[1]))
else:
    list_runs(Path(args.log_dir), pipeline=args.pipeline, limit=args.limit, as_json=args.json)

""" Expected directory structure:

logs/ ├── sales_pipeline_20250521T141210_zebra-mango-haze/ │   ├── get_trades.log │   ├── enrich_orders.log │   ├── sync.log │   └── run_summary.json ├── pricing_pipeline_20250520T104400_echo-raven-blitz/ │   ├── get_prices.log │   ├── generate_yields.log │   └── run_summary.json ...

Each run is isolated under logs/{run_id}/

One .log file per script (matching node name)

One run_summary.json per run """


