# mixins.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from enum import Enum
from pathlib import Path
from datetime import datetime
import polars as pl


# ──────────────── Enums (used by mixins) ────────────────
class Partitioning(str, Enum):
    DATED = "dated"
    TIME = "time"
    SNAPSHOT = "snapshot"


# ──────────────── Mixin: Keyed ────────────────
@dataclass
class Keyed:
    _name: str = field(init=False, repr=False)

    @property
    def name(self) -> str:
        return self._name

    @classmethod
    def from_dict(cls, key: str, cfg: Dict[str, Any]) -> Any:
        obj = cls(**cfg)
        object.__setattr__(obj, "_name", key)
        return obj

    @classmethod
    def load_mapping(cls, raw: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        return {k: cls.from_dict(k, v) for k, v in raw.items()}


# ──────────────── Mixin: Loggable ────────────────
class LoggableMixin:
    def log_id(self, logical_dt: Optional[datetime] = None) -> str:
        return f"{self.name}:{logical_dt.isoformat()}" if logical_dt else self.name


# ──────────────── Mixin: PathResolving ────────────────
class PathResolvingMixin:
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
        suffix = getattr(self, "suffix", ".parquet") or ""
        full = Path(self.base_path) / f"{rel_path}{suffix}"
        return str(full.resolve()) if absolute else str(full)


# ──────────────── Mixin: Validatable ────────────────
class ValidatableMixin:
    def validate(self) -> None:
        if not getattr(self, "script", None):
            raise ValueError(f"{self.name} missing 'script'")
        if not getattr(self, "outputs", None):
            raise ValueError(f"{self.name} has no 'outputs'")


# ──────────────── Optional: DatasetLocator helper ────────────────
@dataclass
class DatasetLocator:
    dataset: PathResolvingMixin
    logical_dt: datetime

    def path(self) -> str:
        return self.dataset.resolve_path(self.logical_dt)

    def exists(self) -> bool:
        return Path(self.path()).exists()

    def load(self, **kwargs) -> pl.DataFrame:
        return pl.read_parquet(self.path(), **kwargs)

    def save(self, df: pl.DataFrame, **kwargs) -> None:
        p = Path(self.path())
        p.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(p, **kwargs)
