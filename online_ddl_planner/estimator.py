"""Deterministic cost model.

All constants are documented planning assumptions, not measurements; callers
plan against them and re-plan if reality diverges.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

COPY_ROWS_PER_SECOND = 50_000.0
APPLY_ROWS_PER_SECOND = 20_000.0
INDEX_BUILD_BYTES_PER_SECOND = 64 * 1024 * 1024
METADATA_LOCK_SECONDS = 0.5
MIN_CHUNK_ROWS = 100
CATCHUP_TOLERANCE_ROWS = 100


@dataclass(frozen=True)
class ChunkStrategy:
    chunk_rows: int
    chunk_count: int
    seconds_per_chunk: float
    total_seconds: float
    chunk_lock_seconds: float
    peak_lag_seconds: float


def chunk_strategy(
    row_count: int,
    max_lock_seconds: float,
    max_replication_lag_seconds: float,
) -> Tuple[Optional[ChunkStrategy], List[str]]:
    """Size backfill chunks so each chunk respects the lock and lag budgets.

    A chunk holds row locks for ``chunk_rows / COPY_ROWS_PER_SECOND`` seconds
    and creates up to ``chunk_rows / APPLY_ROWS_PER_SECOND`` seconds of replica
    apply backlog. Between chunks the copier throttles so the replica drains,
    keeping steady-state lag bounded by the budget.
    """
    problems: List[str] = []
    lock_cap = int(COPY_ROWS_PER_SECOND * max_lock_seconds)
    lag_cap = int(APPLY_ROWS_PER_SECOND * max_replication_lag_seconds)
    if lock_cap < MIN_CHUNK_ROWS:
        problems.append(
            "lock budget %.3fs allows chunks of at most %d rows; "
            "minimum viable chunk is %d rows" % (max_lock_seconds, lock_cap, MIN_CHUNK_ROWS)
        )
    if lag_cap < MIN_CHUNK_ROWS:
        problems.append(
            "replication lag budget %.3fs allows chunks of at most %d rows; "
            "minimum viable chunk is %d rows"
            % (max_replication_lag_seconds, lag_cap, MIN_CHUNK_ROWS)
        )
    if problems:
        return None, problems

    chunk_rows = max(MIN_CHUNK_ROWS, min(lock_cap, lag_cap, max(row_count, 1)))
    chunk_count = (row_count + chunk_rows - 1) // chunk_rows if row_count else 0
    copy_seconds = chunk_rows / COPY_ROWS_PER_SECOND
    drain_seconds = chunk_rows / APPLY_ROWS_PER_SECOND
    per_chunk = max(copy_seconds, drain_seconds)
    return ChunkStrategy(
        chunk_rows=chunk_rows,
        chunk_count=chunk_count,
        seconds_per_chunk=per_chunk,
        total_seconds=per_chunk * chunk_count,
        chunk_lock_seconds=copy_seconds,
        peak_lag_seconds=drain_seconds,
    ), []


def index_build_seconds(size_bytes: int) -> float:
    return size_bytes / INDEX_BUILD_BYTES_PER_SECOND


def cutover_lock_seconds() -> float:
    """Two atomic renames (old->backup, shadow->live) under a metadata lock."""
    return 2 * METADATA_LOCK_SECONDS
