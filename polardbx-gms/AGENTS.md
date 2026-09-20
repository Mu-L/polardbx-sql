# polardbx-gms

全局元数据服务（Global Meta Service），通过 MetaDB（MySQL 兼容）持久化管理拓扑信息、配置参数、权限、序列号等全局元数据。

## Key Classes

| Class | Role |
|-------|------|
| MetaDbDataSource | MetaDB 数据源单例，管理连接池，支持 HA 主从切换 |
| InstConfUtil | 实例配置工具类，从 DynamicConfig 或 MetaDB 读取非请求级配置 |
| DynamicConfig | 动态配置管理中心，200+ 运行时可调参数，支持热更新 |
| DbTopologyManager | 数据库拓扑管理，管理 DN/CN 节点注册和 db_info/storage_info |
| GmsSystemTables | 系统表名常量定义（100+ 张表） |

## MetaDB Access Pattern

所有 MetaDB 表操作通过 **Accessor 模式**完成：

```java
try (Connection conn = MetaDbUtil.getConnection()) {
    XxxAccessor accessor = new XxxAccessor();
    accessor.setConnection(conn);
    List<XxxRecord> records = accessor.queryByXxx(params);
}
```

Accessor 类继承 AbstractAccessor，统一管理 Connection 获取和 SQL 执行。

## Configuration Layering

| Layer | API | Use Case |
|-------|-----|----------|
| 请求级 | `ExecutionContext.getParamManager()` | SQL Hint、Session 变量 |
| 非请求级 | `InstConfUtil.getLong/getInt/getBool()` | 实例级配置 |
| 动态更新 | `DynamicConfig.getInstance().getXxx()` | 内存缓存，启动时从 MetaDB 加载 |

## Conventions

- 禁止从 TDataSource properties 获取参数，使用 InstConfUtil
- MetaDB 表结构定义集中在 GmsSystemTables.java
- 组件基于实例的单例模型，避免挂靠 TDataSource
- MetaDbDataSource 支持 HA 切换，物理连接包装在 MetaDbDataSourceHaWrapper 中
- MetaDB 写操作仅 Leader 节点有权限，非 Leader 节点仅读
