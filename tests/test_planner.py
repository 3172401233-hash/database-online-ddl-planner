import os
import tempfile
import unittest

from online_ddl_planner import (
    Budgets,
    Column,
    Index,
    TableSchema,
    TableStats,
    UniqueConflictReport,
    Workload,
    PlanRequest,
    plan_online_ddl,
    verify_cutover_ready,
)
from online_ddl_planner.state import DDLStateMachine, PlanStore, recovery_plan


def base_schema():
    return TableSchema(
        name="orders",
        columns=(
            Column("id", "bigint", False),
            Column("user_id", "bigint", False),
            Column("amount", "decimal(10,2)", False),
            Column("created_at", "datetime", False),
        ),
        indexes=(Index("idx_user", ("user_id",), size_bytes=100 << 20),),
        primary_key=("id",),
    )


def make_request(target, **overrides):
    budgets = overrides.pop(
        "budgets",
        Budgets(
            max_lock_seconds=5.0,
            max_replication_lag_seconds=30.0,
            deadline_seconds=86400.0,
            allowed_rollback_points=("before_shadow", "before_backfill", "before_cutover"),
        ),
    )
    return PlanRequest(
        existing=base_schema(),
        target=target,
        stats=overrides.pop("stats", TableStats(row_count=50_000_000, avg_row_bytes=256)),
        workload=overrides.pop("workload", Workload(write_rows_per_second=500.0)),
        budgets=budgets,
        **overrides,
    )


def stage(plan, name):
    return next(s for s in plan.stages if s.name == name)


class AddColumnTests(unittest.TestCase):
    def test_add_column_produces_full_stage_sequence(self):
        target = TableSchema(
            name="orders",
            columns=base_schema().columns + (Column("status", "varchar(16)"),),
            indexes=base_schema().indexes,
            primary_key=("id",),
        )
        plan = plan_online_ddl(make_request(target))
        self.assertEqual(plan.status, "FEASIBLE")
        names = [s.name for s in plan.stages]
        self.assertEqual(
            names,
            [
                "validate",
                "create_shadow_table",
                "start_incremental_sync",
                "backfill",
                "build_indexes",
                "catchup_verify",
                "cutover",
                "cleanup",
            ],
        )
        for s in plan.stages:
            self.assertGreaterEqual(s.estimated_seconds, 0.0)
            self.assertGreaterEqual(s.lock_seconds, 0.0)
            self.assertGreaterEqual(s.replication_lag_seconds, 0.0)
            self.assertIsInstance(s.abortable, bool)
        self.assertGreater(plan.total_estimated_seconds, 0.0)
        self.assertLessEqual(plan.max_lock_seconds, 5.0)
        self.assertLessEqual(plan.max_replication_lag_seconds, 30.0)
        self.assertEqual([c.name for c in plan.diff.added_columns], ["status"])

    def test_deadline_too_tight_is_infeasible(self):
        target = TableSchema(
            name="orders",
            columns=base_schema().columns + (Column("status", "varchar(16)"),),
            indexes=base_schema().indexes,
            primary_key=("id",),
        )
        request = make_request(target)
        request = PlanRequest(
            existing=request.existing,
            target=request.target,
            stats=request.stats,
            workload=request.workload,
            budgets=Budgets(
                max_lock_seconds=5.0,
                max_replication_lag_seconds=30.0,
                deadline_seconds=10.0,
            ),
        )
        plan = plan_online_ddl(request)
        self.assertEqual(plan.status, "INFEASIBLE")
        self.assertTrue(any("deadline" in p for p in plan.problems))


class IndexChangeTests(unittest.TestCase):
    def test_added_index_build_is_estimated_from_size(self):
        target = TableSchema(
            name="orders",
            columns=base_schema().columns,
            indexes=base_schema().indexes
            + (Index("idx_amount", ("amount",), size_bytes=200 << 20),),
            primary_key=("id",),
        )
        plan = plan_online_ddl(make_request(target))
        self.assertEqual(plan.status, "FEASIBLE")
        build = stage(plan, "build_indexes")
        self.assertAlmostEqual(build.estimated_seconds, 200.0 / 64.0, places=3)
        self.assertEqual(build.details["indexes"], ["idx_amount"])


class UniqueConflictTests(unittest.TestCase):
    def target_with_unique_user(self):
        return TableSchema(
            name="orders",
            columns=base_schema().columns,
            indexes=(Index("idx_user", ("user_id",), unique=True, size_bytes=100 << 20),),
            primary_key=("id",),
        )

    def test_conflicts_are_quarantined_never_dropped(self):
        report = UniqueConflictReport(checked_rows=50_000_000, violations=42)
        plan = plan_online_ddl(make_request(self.target_with_unique_user(), conflict_report=report))
        self.assertEqual(plan.status, "BLOCKED_ON_CONFLICTS")
        self.assertEqual(plan.conflicts.quarantined, 42)
        self.assertEqual(plan.conflicts.dropped, 0)
        self.assertTrue(plan.conflicts.resolution_required)
        review = stage(plan, "conflict_review")
        cutover = stage(plan, "cutover")
        self.assertLess(plan.stages.index(review), plan.stages.index(cutover))
        self.assertIn("unique_conflicts_resolved", cutover.details["requires"])

    def test_fail_fast_policy_is_infeasible_on_violations(self):
        report = UniqueConflictReport(checked_rows=50_000_000, violations=7)
        plan = plan_online_ddl(
            make_request(
                self.target_with_unique_user(),
                conflict_report=report,
                conflict_policy="fail_fast",
            )
        )
        self.assertEqual(plan.status, "INFEASIBLE")
        self.assertTrue(any("unique" in p for p in plan.problems))


class ReplicationLagTests(unittest.TestCase):
    def target(self):
        return TableSchema(
            name="orders",
            columns=base_schema().columns + (Column("status", "varchar(16)"),),
            indexes=base_schema().indexes,
            primary_key=("id",),
        )

    def test_tight_lag_budget_shrinks_chunks(self):
        request = make_request(self.target())
        tight = Budgets(
            max_lock_seconds=5.0,
            max_replication_lag_seconds=0.5,
            deadline_seconds=86400.0,
            allowed_rollback_points=("before_cutover",),
        )
        request = PlanRequest(
            existing=request.existing, target=request.target, stats=request.stats,
            workload=request.workload, budgets=tight,
        )
        plan = plan_online_ddl(request)
        self.assertEqual(plan.status, "FEASIBLE")
        backfill = stage(plan, "backfill")
        self.assertEqual(backfill.details["chunk_rows"], 10_000)
        self.assertLessEqual(backfill.replication_lag_seconds, 0.5)

    def test_impossible_lag_budget_is_infeasible(self):
        request = make_request(self.target())
        tiny = Budgets(
            max_lock_seconds=5.0,
            max_replication_lag_seconds=0.001,
            deadline_seconds=86400.0,
        )
        request = PlanRequest(
            existing=request.existing, target=request.target, stats=request.stats,
            workload=request.workload, budgets=tiny,
        )
        plan = plan_online_ddl(request)
        self.assertEqual(plan.status, "INFEASIBLE")
        self.assertTrue(any("lag" in p for p in plan.problems))

    def test_cutover_gate_blocks_when_not_caught_up(self):
        budgets = Budgets(max_lock_seconds=5.0, max_replication_lag_seconds=30.0)
        ok, reasons = verify_cutover_ready(0, 1.0, budgets)
        self.assertTrue(ok)
        ok, reasons = verify_cutover_ready(5_000, 1.0, budgets)
        self.assertFalse(ok)
        self.assertTrue(any("not caught up" in r for r in reasons))
        ok, reasons = verify_cutover_ready(0, 999.0, budgets)
        self.assertFalse(ok)
        self.assertTrue(any("replication lag" in r for r in reasons))

        plan = plan_online_ddl(make_request(self.target()))
        names = [s.name for s in plan.stages]
        self.assertLess(names.index("catchup_verify"), names.index("cutover"))
        self.assertIn("incremental_sync_caught_up", stage(plan, "cutover").details["requires"])


class LockBudgetTests(unittest.TestCase):
    def test_lock_budget_below_cutover_requirement_is_infeasible(self):
        target = TableSchema(
            name="orders",
            columns=base_schema().columns + (Column("status", "varchar(16)"),),
            indexes=base_schema().indexes,
            primary_key=("id",),
        )
        request = make_request(target)
        tight = Budgets(
            max_lock_seconds=0.5,
            max_replication_lag_seconds=30.0,
            deadline_seconds=86400.0,
        )
        request = PlanRequest(
            existing=request.existing, target=request.target, stats=request.stats,
            workload=request.workload, budgets=tight,
        )
        plan = plan_online_ddl(request)
        self.assertEqual(plan.status, "INFEASIBLE")
        self.assertTrue(any("lock" in p for p in plan.problems))


class RollbackTests(unittest.TestCase):
    def test_failure_during_backfill_rolls_back_to_old_table(self):
        machine = DDLStateMachine("BACKFILLING")
        machine.fail()
        recovery = recovery_plan(machine, ("before_cutover",))
        self.assertEqual(recovery.final_state, "ROLLED_BACK")
        self.assertEqual(recovery.authoritative_table, "old_table")
        self.assertFalse(recovery.requires_manual_intervention)
        self.assertTrue(any("drop shadow" in a for a in recovery.actions))

    def test_rollback_point_not_allowed_requires_manual_intervention(self):
        machine = DDLStateMachine("BACKFILLING")
        machine.fail()
        recovery = recovery_plan(machine, ())
        self.assertTrue(recovery.requires_manual_intervention)
        self.assertEqual(recovery.authoritative_table, "old_table")

    def test_crash_during_cutover_has_deterministic_resolution(self):
        committed = DDLStateMachine("CAUGHT_UP")
        committed.fail(rename_committed=True)
        self.assertEqual(committed.authoritative_table, "new_table")
        recovery = recovery_plan(committed, ("before_cutover",))
        self.assertEqual(recovery.final_state, "CUT_OVER")
        self.assertEqual(recovery.authoritative_table, "new_table")

        not_committed = DDLStateMachine("CAUGHT_UP")
        not_committed.fail(rename_committed=False)
        self.assertEqual(not_committed.authoritative_table, "old_table")
        recovery = recovery_plan(not_committed, ("before_cutover",))
        self.assertEqual(recovery.final_state, "ROLLED_BACK")

    def test_rollback_after_cutover_only_when_allowed(self):
        machine = DDLStateMachine("CUT_OVER")
        recovery = recovery_plan(machine, ("after_cutover",))
        self.assertEqual(recovery.final_state, "ROLLED_BACK")
        self.assertTrue(any("reverse" in a for a in recovery.actions))

        blocked = recovery_plan(machine, ("before_cutover",))
        self.assertTrue(blocked.requires_manual_intervention)
        self.assertEqual(blocked.authoritative_table, "new_table")


class IdempotencyTests(unittest.TestCase):
    def test_repeated_execution_resumes_without_duplicating(self):
        target = TableSchema(
            name="orders",
            columns=base_schema().columns + (Column("status", "varchar(16)"),),
            indexes=base_schema().indexes,
            primary_key=("id",),
        )
        request = make_request(target)
        with tempfile.TemporaryDirectory() as tmp:
            store = PlanStore(os.path.join(tmp, "state.json"))
            plan1 = plan_online_ddl(request, store=store)
            first_two = [s.stage_id for s in plan1.stages[:2]]
            self.assertTrue(store.mark_stage_completed(plan1.plan_id, first_two[0]))
            self.assertTrue(store.mark_stage_completed(plan1.plan_id, first_two[1]))
            self.assertFalse(store.mark_stage_completed(plan1.plan_id, first_two[0]))

            plan2 = plan_online_ddl(request, store=store)
            self.assertEqual(plan1.plan_id, plan2.plan_id)
            self.assertEqual([s.status for s in plan2.stages[:2]], ["already_completed"] * 2)
            self.assertTrue(all(s.status == "pending" for s in plan2.stages[2:]))
            self.assertLess(plan2.remaining_estimated_seconds, plan1.remaining_estimated_seconds)
            self.assertEqual(len(store.completed_stages(plan1.plan_id)), 2)


if __name__ == "__main__":
    unittest.main()
