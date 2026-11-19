"""Execution state tracking, idempotency store and deterministic recovery.

The state machine guarantees exactly one authoritative table at every point,
so a failure never leaves an ambiguous dual-write state.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

PRE_CUTOVER_STATES = (
    "DRAFT",
    "VALIDATED",
    "SHADOW_CREATED",
    "INCREMENTAL_SYNC",
    "BACKFILLING",
    "INDEXES_BUILT",
    "CAUGHT_UP",
)
POST_CUTOVER_STATES = ("CUT_OVER", "CLEANED_UP")
TERMINAL_STATES = ("ROLLED_BACK", "DONE")
STATES = PRE_CUTOVER_STATES + POST_CUTOVER_STATES + TERMINAL_STATES + ("FAILED",)

_STATE_ROLLBACK_POINT = {
    "DRAFT": "before_shadow",
    "VALIDATED": "before_shadow",
    "SHADOW_CREATED": "before_backfill",
    "INCREMENTAL_SYNC": "before_backfill",
    "BACKFILLING": "before_cutover",
    "INDEXES_BUILT": "before_cutover",
    "CAUGHT_UP": "before_cutover",
}


@dataclass
class RecoveryPlan:
    actions: List[str]
    final_state: str
    authoritative_table: str
    requires_manual_intervention: bool = False


class DDLStateMachine:
    """Tracks which table is authoritative at every point of the migration."""

    def __init__(self, state: str = "DRAFT", rename_committed: bool = False):
        if state not in STATES:
            raise ValueError("unknown state %r" % state)
        self.state = state
        self.rename_committed = rename_committed

    @property
    def authoritative_table(self) -> str:
        if self.state in POST_CUTOVER_STATES:
            return "new_table"
        if self.state == "FAILED" and self.rename_committed:
            return "new_table"
        return "old_table"

    def fail(self, rename_committed: Optional[bool] = None) -> None:
        if rename_committed is not None:
            self.rename_committed = rename_committed
        self.state = "FAILED"


def recovery_plan(
    machine: DDLStateMachine,
    allowed_rollback_points: Tuple[str, ...],
) -> RecoveryPlan:
    """Deterministic recovery: exactly one branch applies to any failure."""
    allowed = set(allowed_rollback_points)

    if machine.state == "FAILED" and machine.rename_committed:
        return RecoveryPlan(
            actions=[
                "confirm the atomic rename committed (shadow is now live)",
                "resume post-cutover cleanup",
            ],
            final_state="CUT_OVER",
            authoritative_table="new_table",
        )

    if machine.state in PRE_CUTOVER_STATES or machine.state == "FAILED":
        point = _STATE_ROLLBACK_POINT.get(machine.state, "before_cutover")
        if point in allowed:
            return RecoveryPlan(
                actions=[
                    "stop incremental change capture",
                    "discard quarantine/conflict tables",
                    "drop shadow table",
                    "verify the old table is the only writer",
                ],
                final_state="ROLLED_BACK",
                authoritative_table="old_table",
            )
        return RecoveryPlan(
            actions=[
                "rollback point %r is not allowed; resume the plan or "
                "involve an operator" % point
            ],
            final_state=machine.state,
            authoritative_table="old_table",
            requires_manual_intervention=True,
        )

    if machine.state in POST_CUTOVER_STATES:
        if "after_cutover" in allowed:
            return RecoveryPlan(
                actions=[
                    "start reverse incremental sync (new -> old)",
                    "wait for reverse sync to catch up",
                    "atomically rename tables back",
                    "drop the new shadow table",
                ],
                final_state="ROLLED_BACK",
                authoritative_table="old_table",
            )
        return RecoveryPlan(
            actions=[
                "rollback after cutover is not allowed; keep the new table "
                "and finish cleanup"
            ],
            final_state=machine.state,
            authoritative_table="new_table",
            requires_manual_intervention=True,
        )

    raise ValueError("no recovery needed for terminal state %s" % machine.state)


class PlanStore:
    """JSON-file store that makes repeated planning/execution idempotent."""

    def __init__(self, path: str):
        self.path = path

    def _load(self) -> Dict:
        if not os.path.exists(self.path):
            return {"plans": {}}
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _save(self, data: Dict) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    def completed_stages(self, plan_id: str) -> Set[str]:
        return set(self._load()["plans"].get(plan_id, {}).get("completed_stages", []))

    def mark_stage_completed(self, plan_id: str, stage_id: str) -> bool:
        """Record a completed stage. Returns False if it was already recorded."""
        data = self._load()
        plan = data["plans"].setdefault(plan_id, {"completed_stages": []})
        if stage_id in plan["completed_stages"]:
            return False
        plan["completed_stages"].append(stage_id)
        self._save(data)
        return True
