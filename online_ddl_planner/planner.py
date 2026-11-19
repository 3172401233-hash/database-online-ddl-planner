"""Multi-stage online DDL planner.

Produces an execution plan only; it never generates or executes database DDL.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from .estimator import (
    APPLY_ROWS_PER_SECOND,
    CATCHUP_TOLERANCE_ROWS,
    METADATA_LOCK_SECONDS,
    chunk_strategy,
    cutover_lock_seconds,
    index_build_seconds,
)
from .models import (
    CONFLICT_FAIL_FAST,
    Budgets,
    ConflictDisposition,
    Plan,
    PlanRequest,
    SchemaDiff,
    Stage,
    TableSchema,
)
from .state import PlanStore


def diff_schemas(existing: TableSchema, target: TableSchema) -> SchemaDiff:
    diff = SchemaDiff()
    old_cols, new_cols = existing.column_map(), target.column_map()
    for name, col in new_cols.items():
        if name not in old_cols:
            diff.added_columns.append(col)
        elif old_cols[name] != col:
            diff.changed_columns.append((old_cols[name], col))
    for name, col in old_cols.items():
        if name not in new_cols:
            diff.dropped_columns.append(col)

    old_idx, new_idx = existing.index_map(), target.index_map()
    for name, idx in new_idx.items():
        if name not in old_idx:
            diff.added_indexes.append(idx)
        elif old_idx[name] != idx:
            diff.added_indexes.append(idx)
            diff.dropped_indexes.append(old_idx[name])
    for name, idx in old_idx.items():
        if name not in new_idx:
            diff.dropped_indexes.append(idx)

    old_unique_keys = {tuple(i.columns) for i in existing.indexes if i.unique}
    old_unique_keys.add(tuple(existing.primary_key))
    for idx in target.indexes:
        if idx.unique and tuple(idx.columns) not in old_unique_keys:
            diff.new_unique_indexes.append(idx)
    return diff


def verify_cutover_ready(
    pending_rows: int,
    replica_lag_seconds: float,
    budgets: Budgets,
) -> Tuple[bool, List[str]]:
    """Gate that must pass before the cutover stage may start."""
    reasons: List[str] = []
    if pending_rows > CATCHUP_TOLERANCE_ROWS:
        reasons.append(
            "incremental sync not caught up: %d pending rows (tolerance %d)"
            % (pending_rows, CATCHUP_TOLERANCE_ROWS)
        )
    if replica_lag_seconds > budgets.max_replication_lag_seconds:
        reasons.append(
            "replication lag %.3fs exceeds budget %.3fs"
            % (replica_lag_seconds, budgets.max_replication_lag_seconds)
        )
    return (not reasons), reasons


def plan_online_ddl(request: PlanRequest, store: Optional[PlanStore] = None) -> Plan:
    budgets = request.budgets
    problems: List[str] = []
    warnings: List[str] = []
    diff = diff_schemas(request.existing, request.target)
    plan_id = "%s-%s" % (request.target.name, request.fingerprint())
    allowed = set(budgets.allowed_rollback_points)

    if request.existing.name != request.target.name:
        problems.append("existing and target schema must describe the same table")
    if not request.target.primary_key:
        problems.append("target schema must keep a primary key for incremental sync")
    for col in diff.dropped_columns:
        warnings.append("dropped column %r is unrecoverable after cutover" % col.name)

    stages: List[Stage] = []

    def add_stage(name, kind, estimated, lock, lag, abortable, rollback_point, details):
        stages.append(
            Stage(
                stage_id="%s.%02d.%s" % (request.target.name, len(stages) + 1, name),
                name=name,
                kind=kind,
                estimated_seconds=round(estimated, 3),
                lock_seconds=round(lock, 3),
                replication_lag_seconds=round(lag, 3),
                abortable=abortable,
                rollback_point=rollback_point,
                rollback_allowed=bool(rollback_point) and rollback_point in allowed,
                details=details,
            )
        )

    add_stage(
        "validate", "check", 0.0, 0.0, 0.0, True, "before_shadow",
        {
            "added_columns": [c.name for c in diff.added_columns],
            "dropped_columns": [c.name for c in diff.dropped_columns],
            "changed_columns": [c.name for _, c in diff.changed_columns],
            "added_indexes": [i.name for i in diff.added_indexes],
            "dropped_indexes": [i.name for i in diff.dropped_indexes],
            "new_unique_indexes": [i.name for i in diff.new_unique_indexes],
        },
    )

    add_stage(
        "create_shadow_table", "schema", 1.0, METADATA_LOCK_SECONDS, 0.0, True,
        "before_backfill",
        {"shadow_table": "_%s_shadow" % request.target.name},
    )

    add_stage(
        "start_incremental_sync", "replication", 0.5, 0.0, 0.0, True,
        "before_backfill",
        {
            "checkpoint": "consistent snapshot LSN recorded before backfill",
            "single_writer": (
                "old table stays authoritative; changes are captured from the "
                "change log and applied to the shadow table - the application "
                "never dual-writes, so a failure cannot leave an ambiguous "
                "dual-write state"
            ),
        },
    )

    strategy, chunk_problems = chunk_strategy(
        request.stats.row_count,
        budgets.max_lock_seconds,
        budgets.max_replication_lag_seconds,
    )
    problems.extend(chunk_problems)

    conflicts = ConflictDisposition(policy=request.conflict_policy)
    if diff.new_unique_indexes:
        conflicts.quarantine_table = "_%s_unique_conflicts" % request.target.name

    if strategy is not None:
        backfill_details = {
            "chunk_rows": strategy.chunk_rows,
            "chunk_count": strategy.chunk_count,
            "seconds_per_chunk": strategy.seconds_per_chunk,
            "abortable_between_chunks": True,
            "conflict_policy": request.conflict_policy,
            "conflict_rule": (
                "rows violating a new unique index are written to the "
                "quarantine table and counted; they are never silently dropped"
            ),
        }
        if conflicts.quarantine_table:
            backfill_details["quarantine_table"] = conflicts.quarantine_table
        add_stage(
            "backfill", "data", strategy.total_seconds, strategy.chunk_lock_seconds,
            strategy.peak_lag_seconds, True, "before_cutover", backfill_details,
        )
    else:
        add_stage(
            "backfill", "data", 0.0, 0.0, 0.0, True, "before_cutover",
            {"error": "no viable chunk strategy within budgets"},
        )

    index_bytes = sum(
        i.size_bytes or int(request.stats.row_count * request.stats.avg_row_bytes * 0.25)
        for i in diff.added_indexes
    )
    index_seconds = index_build_seconds(index_bytes) if index_bytes else 0.0
    index_lag = index_bytes / (APPLY_ROWS_PER_SECOND * request.stats.avg_row_bytes)
    add_stage(
        "build_indexes", "schema", index_seconds, 0.0, index_lag, True,
        "before_cutover",
        {"indexes": [i.name for i in diff.added_indexes], "index_bytes": index_bytes},
    )

    violations = request.conflict_report.violations if request.conflict_report else 0
    if diff.new_unique_indexes:
        if violations and request.conflict_policy == CONFLICT_FAIL_FAST:
            problems.append(
                "%d unique-key conflict(s) detected for %s and policy is fail_fast"
                % (violations, [i.name for i in diff.new_unique_indexes])
            )
        elif violations:
            conflicts.quarantined = violations
            conflicts.dropped = 0
            conflicts.resolution_required = True
        add_stage(
            "conflict_review", "gate", 0.0, 0.0, 0.0, True, "before_cutover",
            {
                "new_unique_indexes": [i.name for i in diff.new_unique_indexes],
                "violations_detected": violations,
                "quarantine_table": conflicts.quarantine_table,
                "requirement": "every quarantined row must be resolved before cutover",
            },
        )

    add_stage(
        "catchup_verify", "gate", 1.0, 0.0, 0.0, True, "before_cutover",
        {
            "requirement": (
                "incremental sync must be fully caught up (pending rows <= %d) "
                "and replica lag must be within budget before cutover"
                % CATCHUP_TOLERANCE_ROWS
            ),
        },
    )

    cutover_lock = cutover_lock_seconds()
    if cutover_lock > budgets.max_lock_seconds:
        problems.append(
            "cutover lock %.3fs exceeds lock budget %.3fs"
            % (cutover_lock, budgets.max_lock_seconds)
        )
    cutover_requires = ["incremental_sync_caught_up", "replication_lag_within_budget"]
    if diff.new_unique_indexes:
        cutover_requires.append("unique_conflicts_resolved")
    add_stage(
        "cutover", "atomic", cutover_lock, cutover_lock, 0.0, False, "after_cutover",
        {
            "action": "single atomic rename: old -> backup, shadow -> live",
            "requires": cutover_requires,
        },
    )

    add_stage(
        "cleanup", "schema", 2.0, METADATA_LOCK_SECONDS, 0.0, True, None,
        {
            "old_table_retained_for_rollback": "after_cutover" in allowed,
            "actions": ["stop change capture", "drop or archive the old table"],
        },
    )

    total = sum(s.estimated_seconds for s in stages)
    if budgets.deadline_seconds is not None and total > budgets.deadline_seconds:
        problems.append(
            "estimated total %.0fs exceeds deadline budget %.0fs"
            % (total, budgets.deadline_seconds)
        )

    if problems:
        status = "INFEASIBLE"
    elif conflicts.resolution_required:
        status = "BLOCKED_ON_CONFLICTS"
    else:
        status = "FEASIBLE"

    if store is not None:
        done = store.completed_stages(plan_id)
        for stage in stages:
            if stage.stage_id in done:
                stage.status = "already_completed"

    return Plan(
        plan_id=plan_id,
        table=request.target.name,
        status=status,
        stages=stages,
        total_estimated_seconds=round(total, 3),
        max_lock_seconds=max(s.lock_seconds for s in stages),
        max_replication_lag_seconds=max(s.replication_lag_seconds for s in stages),
        conflicts=conflicts,
        diff=diff,
        problems=problems,
        warnings=warnings,
    )
