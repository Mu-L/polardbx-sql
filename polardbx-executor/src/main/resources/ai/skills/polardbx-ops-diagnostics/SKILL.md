---
name: ops-diagnostics
description: |
  PolarDB-X 运维诊断视图完整参考。覆盖事务/锁、慢SQL、DDL、执行计划、分区拓扑、存储资源、列存、内存等八大场景的 information_schema 视图用法与示例 SQL。
  Use when diagnosing performance issues, checking cluster health, analyzing slow queries, monitoring DDL progress, investigating locks/transactions, or inspecting data distribution on PolarDB-X.
  Triggers: "诊断", "运维", "巡检", "慢SQL", "长事务", "锁等待", "DDL进度", "数据倾斜", "存储节点", "DN状态", "连接数", "内存", "计划缓存", "SPM", "死锁", "information_schema", "STATEMENTS_SUMMARY", "POLARDBX_TRX", "TABLE_DETAIL", "STORAGE", "DDL_PROGRESS", "METADATA_LOCK", "DEADLOCKS", "PLAN_CACHE", "OPTIMIZER_ALERT", "diagnostics", "health check"
metadata:
  version: 0.1.0
---

# PolarDB-X 运维诊断视图参考

本技能提供 PolarDB-X 独有 information_schema 视图的完整使用指南。当用户提出运维诊断类问题时，优先使用这些视图获取信息。

## Scope

**适用于：**
- 慢 SQL 排查、QPS/RT 分析
- 分布式事务排查（长事务、锁等待、死锁）
- DDL 执行监控
- 集群/存储节点健康检查
- 数据分布与倾斜分析
- 执行计划缓存与 SPM 管理
- 内存/连接池/线程池监控
- 列存/冷存状态检查

**不适用于：**
- MySQL 标准 information_schema 视图的基本用法（如 TABLES, COLUMNS, SCHEMATA 等）
- DN 内部 InnoDB 状态（使用 SHOW ENGINE INNODB STATUS）

---

## Core Workflow

1. 识别用户问题所属场景（事务/慢SQL/DDL/拓扑/存储/内存等）
2. 选择对应的诊断视图并构造查询
3. 解读结果，给出诊断结论和建议

---

## 诊断视图速查表

### 1. 事务与锁

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `POLARDBX_TRX` | 查看当前所有分布式事务（含 duration、状态、SQL） | `SELECT * FROM information_schema.polardbx_trx ORDER BY DURATION_TIME DESC LIMIT 20` |
| `DEADLOCKS` | 查看最近发生的死锁记录 | `SELECT * FROM information_schema.deadlocks ORDER BY GMT_CREATED DESC LIMIT 10` |
| `METADATA_LOCK` | 查看 MDL 锁持有者和等待者 | `SELECT * FROM information_schema.metadata_lock` |
| `INNODB_TRX` | DN 级别 InnoDB 事务列表 | `SELECT * FROM information_schema.innodb_trx` |
| `INNODB_LOCKS` | DN 级别行锁信息 | `SELECT * FROM information_schema.innodb_locks` |
| `INNODB_LOCK_WAITS` | DN 级别锁等待关系 | `SELECT * FROM information_schema.innodb_lock_waits` |
| `PREPARED_TRX_BRANCH` | 悬挂的 XA 预备事务分支 | `SELECT * FROM information_schema.prepared_trx_branch` |

**典型诊断流程 — 长事务排查：**
```sql
-- 1. 找到超过 60 秒的事务
SELECT TRX_ID, DURATION_TIME, SQL, PROCESS_ID FROM information_schema.polardbx_trx WHERE DURATION_TIME > 60;
-- 2. 如果需要 kill
KILL <PROCESS_ID>;
```

### 2. 慢 SQL 分析

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `STATEMENTS_SUMMARY` | SQL 摘要统计（累计执行次数、总耗时、平均RT等） | `SELECT SCHEMA, SQL_TEMPLATE, COUNT, AVG_RESPONSE_TIME_MS, SUM_RESPONSE_TIME_MS FROM information_schema.statements_summary ORDER BY SUM_RESPONSE_TIME_MS DESC LIMIT 20` |
| `STATEMENTS_SUMMARY_HISTORY` | 历史 SQL 摘要（与上类似，保留更长时间窗口） | `SELECT * FROM information_schema.statements_summary_history WHERE SCHEMA='xxx' ORDER BY SUM_RESPONSE_TIME_MS DESC LIMIT 10` |
| `PHYSICAL_PROCESSLIST` | DN 上正在执行的物理 SQL | `SELECT * FROM information_schema.physical_processlist WHERE TIME > 5` |
| `PROCESSLIST` | CN 逻辑连接列表 | `SELECT * FROM information_schema.processlist WHERE COMMAND != 'Sleep'` |
| `WORKLOAD` | 实时负载指标（QPS、连接数等） | `SELECT * FROM information_schema.workload` |

**典型诊断流程 — TOP 慢 SQL：**
```sql
-- 找出平均 RT 最高的 SQL 模板
SELECT SQL_TEMPLATE, COUNT, AVG_RESPONSE_TIME_MS,
       SUM_AFFECTED_ROWS, SUM_RESPONSE_TIME_MS
FROM information_schema.statements_summary
WHERE SCHEMA = 'your_db'
ORDER BY AVG_RESPONSE_TIME_MS DESC LIMIT 10;
```

### 3. DDL 监控

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `DDL_PROGRESS` | DDL 执行进度（百分比） | `SELECT * FROM information_schema.ddl_progress` |
| `DDL_PLAN` | DDL 计划详情（步骤列表） | `SELECT * FROM information_schema.ddl_plan WHERE JOB_ID=xxx` |
| `DDL_INFO` | DDL 任务概览 | `SELECT * FROM information_schema.ddl_info` |
| `DDL_SCHEDULER` | DDL 调度器状态 | `SELECT * FROM information_schema.ddl_scheduler` |
| `DDL_ENGINE_RESOURCE` | DDL 引擎资源占用 | `SELECT * FROM information_schema.ddl_engine_resource` |
| `DDL_PHYSICAL_LOCK_STAT` | DDL 物理锁持有统计 | `SELECT * FROM information_schema.ddl_physical_lock_stat` |
| `OMC_PROGRESS` | Online Modify Column 进度 | `SELECT * FROM information_schema.omc_progress` |
| `REBALANCE_PROGRESS` | Rebalance 进度 | `SELECT * FROM information_schema.rebalance_progress` |

### 4. 执行计划与优化器

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `PLAN_CACHE` | 执行计划缓存内容 | `SELECT * FROM information_schema.plan_cache WHERE SCHEMA_NAME='xxx' LIMIT 20` |
| `PLAN_CACHE_CAPACITY` | 计划缓存容量 | `SELECT * FROM information_schema.plan_cache_capacity` |
| `SPM` | SQL Plan Management 绑定的基线 | `SELECT * FROM information_schema.spm WHERE SCHEMA_NAME='xxx'` |
| `OPTIMIZER_ALERT` | 优化器告警（全表扫描、走错计划等） | `SELECT * FROM information_schema.optimizer_alert WHERE ALERT_COUNT > 0 ORDER BY ALERT_COUNT DESC` |
| `STATISTIC_TASK` | 统计信息采集任务 | `SELECT * FROM information_schema.statistic_task` |
| `STATISTICS_DATA` | 统计信息详情 | `SELECT * FROM information_schema.statistics_data WHERE SCHEMA_NAME='xxx' AND TABLE_NAME='yyy'` |

### 5. 分区与拓扑

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `TABLE_DETAIL` | 每个分区的行数、大小分布 | `SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, TABLE_ROWS, DATA_LENGTH FROM information_schema.table_detail WHERE TABLE_SCHEMA='xxx' ORDER BY TABLE_ROWS DESC` |
| `TABLE_GROUP` | 表组信息（共置关系） | `SELECT * FROM information_schema.table_group WHERE TABLE_SCHEMA='xxx'` |
| `FULL_TABLE_GROUP` | 表组详情含成员表 | `SELECT * FROM information_schema.full_table_group WHERE TABLE_SCHEMA='xxx'` |
| `GLOBAL_INDEXES` | 全局二级索引（GSI）列表 | `SELECT * FROM information_schema.global_indexes WHERE SCHEMA='xxx'` |
| `PARTITIONS_META` | 分区元数据 | `SELECT * FROM information_schema.partitions_meta WHERE TABLE_SCHEMA='xxx' AND TABLE_NAME='yyy'` |
| `LOCALITY_INFO` | 数据局部性配置 | `SELECT * FROM information_schema.locality_info` |
| `MOVE_DATABASE` | 搬库任务进度 | `SELECT * FROM information_schema.move_database` |

**典型诊断流程 — 数据倾斜：**
```sql
-- 查看各分区行数分布
SELECT PARTITION_NAME, TABLE_ROWS, DATA_LENGTH
FROM information_schema.table_detail
WHERE TABLE_SCHEMA='your_db' AND TABLE_NAME='your_table'
ORDER BY TABLE_ROWS DESC;
-- 对比最大/最小分区行数判断倾斜程度
```

### 6. 存储节点

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `STORAGE` | DN 节点概览（角色、IP、端口、状态） | `SELECT * FROM information_schema.storage` |
| `STORAGE_STATUS` | DN 连接池、线程状态 | `SELECT * FROM information_schema.storage_status` |
| `STORAGE_REPLICAS` | DN 副本信息 | `SELECT * FROM information_schema.storage_replicas` |
| `FULL_STORAGE` | 完整 DN 信息（含只读节点） | `SELECT * FROM information_schema.full_storage` |
| `STORAGE_PROPERTIES` | DN 配置属性 | `SELECT * FROM information_schema.storage_properties` |
| `DN_PERF` | DN 性能指标 | `SELECT * FROM information_schema.dn_perf` |
| `NODE_STATS` | CN 节点统计 | `SELECT * FROM information_schema.node_stats` |

### 7. 内存与连接池

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `CN_MEMORYPOOL` | CN 内存池使用情况 | `SELECT * FROM information_schema.cn_memorypool` |
| `QUERY_MEMORY` | 当前查询的内存使用 | `SELECT * FROM information_schema.query_memory ORDER BY QUERY_USED DESC LIMIT 10` |
| `TOTAL_MEMORY` | CN 总内存统计 | `SELECT * FROM information_schema.total_memory` |
| `EXECUTOR_MEMORY` | 执行器内存 | `SELECT * FROM information_schema.executor_memory` |
| `CN_THREADPOOL` | CN 线程池状态 | `SELECT * FROM information_schema.cn_threadpool` |
| `CN_DBSTATS` | CN 各库统计 | `SELECT * FROM information_schema.cn_dbstats` |
| `CN_STATUS` | CN 节点状态 | `SELECT * FROM information_schema.cn_status` |

### 8. 列存与冷存

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `COLUMNAR_INDEX_STATUS` | 列存索引物理空间占用 | `SELECT * FROM information_schema.columnar_index_status` |
| `COLUMNAR_STATUS` | 列存索引有效数据统计 | `SELECT * FROM information_schema.columnar_status` |
| `FILE_STORAGE` | 冷存文件存储概览 | `SELECT * FROM information_schema.file_storage` |
| `FILE_STORAGE_FILES_META` | 冷存文件明细 | `SELECT * FROM information_schema.file_storage_files_meta` |

### 9. 其他

| 视图 | 用途 | 示例 SQL |
|------|------|----------|
| `CCL_RULE` | 并发控制规则 | `SELECT * FROM information_schema.ccl_rule` |
| `SEQUENCES` | 序列信息 | `SELECT * FROM information_schema.sequences WHERE SCHEMA_NAME='xxx'` |
| `REACTOR_PERF` | NIO Reactor 性能 | `SELECT * FROM information_schema.reactor_perf` |
| `SESSION_PERF` | 会话级性能 | `SELECT * FROM information_schema.session_perf` |
| `REPLICA_STAT` | 复制统计 | `SELECT * FROM information_schema.replica_stat` |
| `SCHEDULE_JOBS` | 定时任务列表 | `SELECT * FROM information_schema.schedule_jobs WHERE TABLE_SCHEMA='xxx'` |
| `TTL_INFO` | TTL 配置信息 | `SELECT * FROM information_schema.ttl_info WHERE TABLE_SCHEMA='xxx'` |
| `MODULE` | CN 各模块运行状态 | `SELECT * FROM information_schema.module` |

---

## Best Practices

1. **先窄后宽**：先用 WHERE 条件限定 schema/table，避免全量扫描
2. **关注关键列**：STATEMENTS_SUMMARY 重点看 SUM_RESPONSE_TIME_MS, COUNT, AVG_RESPONSE_TIME_MS, SUM_AFFECTED_ROWS
3. **对比基线**：诊断时对比历史数据（STATEMENTS_SUMMARY_HISTORY）判断是否异常
4. **组合使用**：长事务 + MDL 锁 + 物理连接 三个视图联合排查阻塞链
5. **避免高频查询**：部分视图（如 PHYSICAL_PROCESSLIST）涉及跨 DN 收集，在高负载时谨慎使用
