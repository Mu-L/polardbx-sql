# polardbx-server

MySQL 协议层和连接管理入口，负责接收客户端连接、协议解析、SQL 路由分发和会话生命周期管理。

## Key Classes

| Class | Role |
|-------|------|
| TddlLauncher | 程序入口，启动 CobarServer 单例 |
| CobarServer | 服务器核心单例，管理 NIOAcceptor、NIOProcessor、线程池 |
| ServerConnection | 前端连接实现，封装 MySQL 会话状态、事务管理、SQL 执行入口 |
| ServerQueryHandler | SQL 类型解析与路由（SELECT/SET/SHOW/USE 等） |
| CobarConfig | 配置管理器，协调 ServerLoader 和 ClusterLoader |

## Connection Hierarchy

```
AbstractConnection (polardbx-net, NIO 抽象)
  └── FrontendConnection (MySQL 协议握手、认证、字符集)
        └── ServerConnection (业务逻辑，桥接协议层与执行引擎)
```

## Request Flow

```
Client → NIOAcceptor → FrontendAuthenticator → ServerConnection
  → ServerQueryHandler.queryRaw()
    → ServerParse (识别 SQL 类型)
    → 路由到具体 Handler (SelectHandler/SetHandler/...)
    → ServerConnection.execute()
      → TConnection.executeSQL()
        → Optimizer → Executor → DN
  → 返回 MySQL 协议响应 (ResultSet/OK/Error Packet)
```

## Conventions

- ServerConnection 中的 TConnection 是历史遗留，后续逻辑应迁移合并到 ServerConnection
- 配置加载通过 ServerLoader 读取 server.properties，支持 `-D` JVM 参数覆盖
- 部分配置可通过 MetaDB 动态更新（InstConfUtil），无需重启
- SystemConfig 中标注 `@Immutable` 的字段不允许动态修改
