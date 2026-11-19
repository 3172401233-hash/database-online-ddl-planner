"""Multi-stage online DDL planner.

Produces an *execution plan* for shadow-table based online schema changes.
It never connects to a database and never emits DDL SQL.
"""

from .models import (
    Budgets,
    Column,
    ConflictDisposition,
    Index,
    Plan,
    PlanRequest,
    Stage,
    TableSchema,
    TableStats,
    UniqueConflictReport,
    Workload,
    CONFLICT_FAIL_FAST,
    CONFLICT_QUARANTINE,
    ROLLBACK_POINTS,
)
from .planner import diff_schemas, plan_online_ddl, verify_cutover_ready
from .state import DDLStateMachine, PlanStore, RecoveryPlan, recovery_plan

__all__ = [
    "Budgets",
    "Column",
    "ConflictDisposition",
    "Index",
    "Plan",
    "PlanRequest",
    "Stage",
    "TableSchema",
    "TableStats",
    "UniqueConflictReport",
    "Workload",
    "CONFLICT_FAIL_FAST",
    "CONFLICT_QUARANTINE",
    "ROLLBACK_POINTS",
    "diff_schemas",
    "plan_online_ddl",
    "verify_cutover_ready",
    "DDLStateMachine",
    "PlanStore",
    "RecoveryPlan",
    "recovery_plan",
]
