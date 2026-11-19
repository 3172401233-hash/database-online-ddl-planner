"""Data model for the online DDL planner (Python 3.8.2 compatible)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None


@dataclass(frozen=True)
class Index:
    name: str
    columns: Tuple[str, ...]
    unique: bool = False
    size_bytes: int = 0


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: Tuple[Column, ...]
    indexes: Tuple[Index, ...] = ()
    primary_key: Tuple[str, ...] = ("id",)

    def column_map(self) -> Dict[str, Column]:
        return {c.name: c for c in self.columns}

    def index_map(self) -> Dict[str, Index]:
        return {i.name: i for i in self.indexes}


@dataclass(frozen=True)
class TableStats:
    row_count: int
    avg_row_bytes: int = 256


@dataclass(frozen=True)
class Workload:
    write_rows_per_second: float


ROLLBACK_POINTS = ("before_shadow", "before_backfill", "before_cutover", "after_cutover")


@dataclass(frozen=True)
class Budgets:
    max_lock_seconds: float
    max_replication_lag_seconds: float
    deadline_seconds: Optional[float] = None
    allowed_rollback_points: Tuple[str, ...] = ("before_cutover",)


@dataclass(frozen=True)
class UniqueConflictReport:
    """Result of probing existing data against a new unique index."""

    checked_rows: int
    violations: int
    samples: Tuple[str, ...] = ()


CONFLICT_QUARANTINE = "quarantine"
CONFLICT_FAIL_FAST = "fail_fast"


@dataclass(frozen=True)
class PlanRequest:
    existing: TableSchema
    target: TableSchema
    stats: TableStats
    workload: Workload
    budgets: Budgets
    conflict_report: Optional[UniqueConflictReport] = None
    conflict_policy: str = CONFLICT_QUARANTINE

    def fingerprint(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, default=str)
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


@dataclass
class SchemaDiff:
    added_columns: List[Column] = field(default_factory=list)
    dropped_columns: List[Column] = field(default_factory=list)
    changed_columns: List[Tuple[Column, Column]] = field(default_factory=list)
    added_indexes: List[Index] = field(default_factory=list)
    dropped_indexes: List[Index] = field(default_factory=list)
    new_unique_indexes: List[Index] = field(default_factory=list)


@dataclass
class Stage:
    stage_id: str
    name: str
    kind: str
    estimated_seconds: float
    lock_seconds: float
    replication_lag_seconds: float
    abortable: bool
    rollback_point: Optional[str]
    rollback_allowed: bool
    details: Dict[str, object] = field(default_factory=dict)
    status: str = "pending"


@dataclass
class ConflictDisposition:
    policy: str = CONFLICT_QUARANTINE
    quarantined: int = 0
    dropped: int = 0
    resolution_required: bool = False
    quarantine_table: str = ""


@dataclass
class Plan:
    plan_id: str
    table: str
    status: str  # FEASIBLE | INFEASIBLE | BLOCKED_ON_CONFLICTS
    stages: List[Stage]
    total_estimated_seconds: float
    max_lock_seconds: float
    max_replication_lag_seconds: float
    conflicts: ConflictDisposition
    diff: SchemaDiff
    problems: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def remaining_estimated_seconds(self) -> float:
        return sum(s.estimated_seconds for s in self.stages if s.status == "pending")
