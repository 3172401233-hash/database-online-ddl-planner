# database-online-ddl-planner

多阶段在线 DDL 规划器（Python 3.8.2，纯标准库，不连接数据库）。

**输出是执行计划，不负责生成或执行数据库 DDL。** 规划器只根据输入的结构、
数据量、写入速率和预算，产出一份带估算和门禁条件的阶段序列，由外部执行器
（或 DBA）按计划操作数据库。

## 输入

`PlanRequest` 包含：

- `existing` / `target`：现有与目标表结构（列、索引、主键）
- `stats`：行数、平均行宽
- `workload`：写入速率（行/秒）
- `budgets`：锁预算（秒）、主从延迟预算（秒）、任务截止时间预算、允许的回滚点
- `conflict_report` / `conflict_policy`：新增唯一键的冲突预检结果与处理策略

## 计划阶段

| 阶段 | 说明 | 锁/延迟估算 |
| --- | --- | --- |
| `validate` | 结构 diff、预算与主键检查 | 无 |
| `create_shadow_table` | 按目标结构建影子表 | 短暂元数据锁 |
| `start_incremental_sync` | 记录一致性位点，开始增量捕获 | 无 |
| `backfill` | 分块回填，块大小同时受锁预算和延迟预算约束，块间可中止 | 每块锁时间、峰值延迟 |
| `build_indexes` | 影子表上构建新索引 | 按索引大小估算 |
| `conflict_review` | 新增唯一键时的冲突门禁（仅在有新唯一索引时出现） | 无 |
| `catchup_verify` | 切换门禁：增量同步必须追平、复制延迟必须在预算内 | 无 |
| `cutover` | 原子 rename 切换，不可中止 | 切换锁必须 ≤ 锁预算 |
| `cleanup` | 停止捕获、清理旧表 | 短暂元数据锁 |

每个阶段都给出 `estimated_seconds`、`lock_seconds`、`replication_lag_seconds`、
`abortable`（可中止点）和 `rollback_point` / `rollback_allowed`。

## 保证

- **唯一键冲突不静默丢弃**：回填期间违反新唯一索引的行写入隔离表
  （`_<table>_unique_conflicts`）并计数，`dropped` 恒为 0；存在未解决冲突时
  计划状态为 `BLOCKED_ON_CONFLICTS`，切换被门禁挡住。
- **切换前必须追平**：`verify_cutover_ready` 要求增量同步无积压且复制延迟在
  预算内，否则 `cutover` 不得开始。
- **失败不留双写模糊状态**：应用从不双写——旧表在切换前始终是唯一权威写入点，
  变更经日志捕获应用到影子表；`DDLStateMachine` 在每个状态下都有唯一权威表，
  `recovery_plan` 对任何失败点给出确定性的恢复/回滚动作（包括切换中途崩溃时
  按 `rename_committed` 唯一确定归属）。
- **重复执行幂等**：`PlanStore` 记录已完成阶段，相同输入产生相同 `plan_id`，
  重跑时已完成的阶段标记为 `already_completed`，不会重复执行。

## 用法

```python
from online_ddl_planner import (
    Budgets, Column, Index, PlanRequest, TableSchema, TableStats,
    Workload, plan_online_ddl,
)

plan = plan_online_ddl(PlanRequest(
    existing=..., target=...,
    stats=TableStats(row_count=50_000_000, avg_row_bytes=256),
    workload=Workload(write_rows_per_second=500),
    budgets=Budgets(
        max_lock_seconds=5.0,
        max_replication_lag_seconds=30.0,
        deadline_seconds=86400.0,
        allowed_rollback_points=("before_cutover",),
    ),
))
print(plan.status)            # FEASIBLE / INFEASIBLE / BLOCKED_ON_CONFLICTS
for stage in plan.stages:
    print(stage.name, stage.estimated_seconds, stage.lock_seconds,
          stage.replication_lag_seconds, stage.abortable)
```

## 测试

```bash
python -m unittest
```

覆盖：加列、改索引、唯一键冲突（隔离不丢弃 / fail-fast）、复制延迟预算
（收紧分块 / 不可行）、锁预算不足、切换门禁、回滚（切换前后、切换中崩溃）
和重复执行幂等。
