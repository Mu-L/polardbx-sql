# polardbx-transaction

分布式事务管理器，支持 XA、TSO、AUTO_COMMIT、ASYNC_COMMIT 等多种事务类型，提供全局事务日志和恢复机制。

## Key Classes

| Class | Role |
|-------|------|
| TransactionManager | 事务管理器单例，管理活跃事务，调度 XA Recover/死锁检测/事务统计 |
| AbstractTransaction | 分布式事务基类，管理跨库连接、Savepoint、事务状态 |
| XATransaction | XA 两阶段提交实现 |
| TsoTransaction | 基于 Timestamp Oracle 的分布式事务实现 |
| GlobalTxLogManager | 全局事务日志管理（v1 单表 / v2 A/B 表轮转） |

## Transaction Types

| Type | Description |
|------|-------------|
| XA | 标准 XA 两阶段提交 |
| TSO | 基于 TSO 的分布式事务 |
| AUTO_COMMIT | 自动提交单 shard 事务 |
| ASYNC_COMMIT | 异步提交优化 |
| BEST_EFFORT | 尽力而为事务（允许部分失败） |
| READ_ONLY_TSO | 只读 TSO 事务 |

## Transaction Lifecycle

```
begin()
  → TransactionManager.createTransaction(type, executionContext)
  → register(transaction)

execute()
  → AbstractTransaction.getConnection(groupName)  — 获取跨库连接
  → 执行 SQL

commit()
  → beforePrimaryCommit()
  → duringPrimaryCommit()
  → afterPrimaryCommit()
  → GlobalTxLogManager.appendTrxLog()  — 写事务日志
  → cleanupAllConnections()
  → TransactionManager.unregister(txid)
```

## Conventions

- 错误消息必须通过 ErrorCode 封装，禁止裸抛 RuntimeException
- 资源获取后若后续失败，必须在 finally 中释放
- trx log v2 使用 A/B 表轮转避免单表热点，通过 DynamicConfig.getTrxLogMethod() 控制
- TSO 心跳和 Purge 任务仅在 Master 实例运行
- AsyncCommit 任务有数量上限，通过 TransactionManager.isExceedAsyncCommitTaskLimit() 检查
