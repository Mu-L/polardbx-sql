# polardbx-executor

分布式查询执行引擎，将优化器生成的执行计划转换为实际分布式执行，支持 Cursor、TP_LOCAL、MPP 三种执行模式。同时包含 DDL Job Framework。

## Key Classes

| Class | Role |
|-------|------|
| PlanExecutor | DQL/DML 主入口，静态方法 execute() 处理 Explain、SPM 缓存等 |
| ExecutorHelper | 根据 ExecutorMode 路由到 Cursor/TP_LOCAL/MPP 执行路径 |
| HandlerCommon | Handler 基类，handlePlan() 是 DDL/DML 统一入口 |
| Cursor | 游标接口，定义 next()/close()，执行结果的迭代器抽象 |
| *JobFactory | DDL Job 工厂类（CreateTableJobFactory 等），创建异步 DDL 任务 |

## Execution Flow

```
PlanExecutor.execute(executionPlan, executionContext)
  → ExecutorHelper.execute()
    → CURSOR:    executeByCursor()    — 传统游标执行
    → TP_LOCAL:  executeLocal()       — 本地向量化执行
    → MPP:       executeCluster()     — 分布式集群执行
  → ResultCursor
```

DDL Flow:
```
DDL SQL → HandlerCommon.handlePlan()
  → LogicalXxxHandler (如 LogicalCreateTableHandler)
  → XxxJobFactory.create() → DDL Job
  → DDL Engine 异步执行
```

## Do NOT

- **执行阶段禁止访问 RelMetadataQuery**：会 NPE，需要的元数据应在优化阶段计算并存入 ExecutionContext
- **执行阶段禁止修改 SqlNode/RelNode**：执行计划为 SPM 缓存的静态对象
- **ExecutionContext 禁止长生命周期持有**：请求结束后必须清理
- **动态属性（如 now() 值）放 ExecutionContext**：不要存入 RelNode
