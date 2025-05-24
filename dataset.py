# dataset_locator.py
"""
Encapsulates dataset metadata and path resolution based on logical timestamps.
Supports rich layouts for local/shared filesystems with semantic partitioning.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, date
import polars as pl

# ──────────────────── Enums ────────────────────
class Partitioning(str, Enum):
    DATED = "dated"
    TIME = "time"
    SNAPSHOT = "snapshot"

# ──────────────────── Dataset Spec ────────────────────
@dataclass
class DatasetSpec:
    name: str
    base_path: str
    partitioning: Partitioning = Partitioning.DATED
    format: str = "parquet"
    layout: str = "{date}"
    suffix: Optional[str] = ".parquet"

    # Optional metadata
    frequency: Optional[str] = None
    source: Optional[str] = None
    market: Optional[str] = None
    product: Optional[str] = None

    def resolve_path(self, logical_dt: datetime, *, absolute: bool = True) -> str:
        values = {
            "date": logical_dt.date().isoformat(),
            "datetime": logical_dt.strftime("%Y%m%dT%H%M"),
            "year": logical_dt.year,
            "month": f"{logical_dt.month:02}",
            "day": f"{logical_dt.day:02}",
            **{k: v for k, v in self.__dict__.items() if isinstance(v, str)}
        }
        rel_path = self.layout.format(**values)
        suffix = self.suffix or ""
        full = Path(self.base_path) / f"{rel_path}{suffix}"
        return str(full.resolve()) if absolute else str(full)

# ──────────────────── Dataset Locator ────────────────────
@dataclass
class DatasetLocator:
    dataset: DatasetSpec
    logical_dt: datetime

    def path(self) -> str:
        return self.dataset.resolve_path(self.logical_dt)

    def exists(self) -> bool:
        return Path(self.path()).exists()

    def load(self, **kwargs) -> pl.DataFrame:
        if self.dataset.format == "parquet":
            return pl.read_parquet(self.path(), **kwargs)
        raise NotImplementedError(f"Unsupported format: {self.dataset.format}")

    def save(self, df: pl.DataFrame, **kwargs) -> None:
        p = Path(self.path())
        p.parent.mkdir(parents=True, exist_ok=True)
        if self.dataset.format == "parquet":
            df.write_parquet(p, **kwargs)
        else:
            raise NotImplementedError(f"Unsupported format: {self.dataset.format}")

# ──────────────────── Example Usage ────────────────────
if __name__ == "__main__":
    spec = DatasetSpec(
        name="fx_ohlcv",
        base_path="/mnt/shared/fx",
        partitioning=Partitioning.TIME,
        layout="{source}/{frequency}/{datetime}",
        source="reuters",
        frequency="1m"
    )

    locator = DatasetLocator(dataset=spec, logical_dt=datetime(2025, 5, 1, 9, 30))
    print("Resolved path:", locator.path())

    if locator.exists():
        df = locator.load()
        print(df.head())
    else:
        print("No file at:", locator.path())
