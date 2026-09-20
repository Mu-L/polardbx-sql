# PolarDB-X CN 分布式表向量索引 Sysbench 测试方案

## 一、概述

本目录包含针对 PolarDB-X 计算节点 (CN) 分布式表向量索引的 sysbench 压测脚本。
脚本基于 DN 层 `vidx_oltp_common.lua` 系列改造，核心差异为建表时支持分区子句。

**前置条件**：
- 数据库必须使用 AUTO 模式创建：`CREATE DATABASE <db_name> MODE='AUTO'`
- 含 DELETE+INSERT 的场景（write_only / read_write）需添加 `--mysql-ignore-errors=1062,1213`
  避免 `skip_trx=on` 下多线程并发的主键冲突导致 FATAL 退出

## 二、核心差异（CN vs DN）

| 维度 | DN 直连 | CN 分布式表 |
|------|---------|------------|
| 连接目标 | 存储节点 (port 3101) | 计算节点 (port 3306/8527) |
| 表类型 | 单机 InnoDB 表 | KEY/HASH 分区逻辑表 |
| ANN 查询 | 单分区 HNSW 检索 | 多分区扇出 + 合并排序 |
| 向量索引 DDL | `ALTER TABLE ... ADD VECTOR INDEX` | 语法完全一致 |
| 事务 | 本地事务 | 分布式事务（TSO/XA） |
| AUTO_INCREMENT | MySQL 原生自增 | Group Sequence（全局唯一） |
| 会话变量 | `SET SESSION vidx_hnsw_ef_search` | 完全一致 |

## 三、表结构设计

```sql
-- 分布式表（KEY 分区，分区键为主键 id）
CREATE TABLE sbtest1 (
  id INTEGER NOT NULL AUTO_INCREMENT,
  k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL,
  embedding VECTOR(768),
  PRIMARY KEY (id),
  KEY k_1 (k)
) ENGINE=InnoDB
PARTITION BY KEY(id) PARTITIONS 16;

-- 数据灌入后创建向量索引（语法与 DN 一致）
ALTER TABLE sbtest1 ADD VECTOR INDEX vi_1 (embedding) M=16 DISTANCE=COSINE;
```

**分区数选择建议**：
- 4 分区：小规模验证
- 16 分区：标准测试（匹配常见 DN 节点数）
- 64 分区：大规模压力测试

## 四、测试场景

| 场景 | 脚本 | 说明 |
|------|------|------|
| 纯 ANN 查询 | `vidx_ann_only.lua` | 评估分布式 ANN 扇出合并性能 |
| 只读混合 | `vidx_oltp_read_only.lua` | point_select + range_select + ANN |
| 只写 | `vidx_oltp_write_only.lua` | index_update + vector_update + delete_insert |
| 读写混合 | `vidx_oltp_read_write.lua` | 全场景混合，最接近真实负载 |
| 纯插入 | `vidx_insert.lua` | 评估持续写入吞吐（含向量索引维护开销） |
| 纯删除 | `vidx_delete.lua` | 评估删除对向量索引的影响 |
| 点查 | `vidx_point_select_only.lua` | 向量表点查基线 |
| 非索引列更新 | `vidx_update_non_index.lua` | 非向量列更新对向量索引的影响 |

## 五、新增参数（相比 DN 脚本）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--partition_type` | KEY | 分区方式: KEY, HASH, SINGLE |
| `--partitions` | 16 | 分区数 |

`SINGLE` 表示不分区（退化为单表），可与分布式表做对比。

## 六、关键参数推荐

```bash
# 基础参数
--threads=64            # 并发线程数
--time=600              # 运行时间(秒), 短跑 600s, 长稳 3600s
--report-interval=1     # 每秒输出一次 QPS/latency
--tables=1              # 表数量（分布式场景 1 表多分区即可）
--table-size=100000     # 初始数据量（10w-100w）

# 连接参数（连 CN）
--mysql-host=<CN_IP>
--mysql-port=3306
--mysql-user=<user>
--mysql-password=<pass>
--mysql-db=test

# 向量参数
--vector_dim=768        # 向量维度（768/1024/1536 常见）
--vector_m=16           # HNSW M 参数
--vector_distance=COSINE  # 距离度量 (EUCLIDEAN 或 COSINE)
--vector_ef_search=64   # 搜索精度（越大越准，越慢）
--vector_ann_selects=1  # 每事务 ANN 查询数
--vector_ann_limit=10   # Top-K

# CN 专用
--skip_trx=on           # 跳过显式事务，避免分布式事务开销
--db-ps-mode=disable    # 禁用 server-side PS，使用 text protocol

# 分布式表参数
--partition_type=KEY    # 分区方式
--partitions=16         # 分区数
```

## 七、测试流程

### Step 1: Prepare（建表 + 灌数据 + 建向量索引）

```bash
sysbench --threads=1 \
  --mysql-host=<CN_IP> --mysql-port=3306 \
  --mysql-user=admin --mysql-password=123456 \
  --mysql-db=test \
  --tables=1 --table-size=100000 \
  --vector_dim=768 --vector_m=16 --vector_distance=COSINE \
  --partition_type=KEY --partitions=16 \
  --skip_trx=on --db-ps-mode=disable \
  /path/to/vidx_oltp_read_write.lua prepare
```

### Step 2: Run（执行压测）

```bash
sysbench --threads=64 \
  --mysql-host=<CN_IP> --mysql-port=3306 \
  --mysql-user=admin --mysql-password=123456 \
  --mysql-db=test \
  --tables=1 --table-size=100000 \
  --time=600 --report-interval=1 \
  --vector_dim=768 --vector_ann_selects=1 --vector_ann_limit=10 \
  --vector_ef_search=64 \
  --skip_trx=on --db-ps-mode=disable \
  --mysql-ignore-errors=1062,1213 \
  --partition_type=KEY --partitions=16 \
  /path/to/vidx_oltp_read_write.lua run
```

### Step 3: Cleanup

```bash
sysbench --threads=1 \
  --mysql-host=<CN_IP> --mysql-port=3306 \
  --mysql-user=admin --mysql-password=123456 \
  --mysql-db=test \
  --tables=1 \
  --skip_trx=on --db-ps-mode=disable \
  /path/to/vidx_oltp_read_write.lua cleanup
```

## 八、对比测试矩阵

| 变量 | 取值 | 目的 |
|------|------|------|
| 分区数 | 1 / 4 / 16 / 64 | ANN 扇出开销 vs 写入并行度 |
| 并发数 | 16 / 64 / 128 / 256 | 吞吐和延迟拐点 |
| 数据量 | 10K / 100K / 1M | 向量索引规模效应 |
| 向量维度 | 128 / 768 / 1536 | 维度对 ANN 性能的影响 |
| ef_search | 20 / 64 / 200 | 精度-延迟 tradeoff |
| 读写比 | read_only / read_write / write_only | 不同负载模型 |
| 事务模式 | skip_trx=on / off | 分布式事务开销 |
| 表类型 | SINGLE / KEY(16) | 单表 vs 分布式表对比基线 |

## 九、关注指标

1. **QPS / TPS** - 吞吐量
2. **P95 / P99 Latency** - 尾延迟
3. **ANN Recall@K** - 向量检索精度（需额外验证脚本）
4. **DDL 耗时** - `ADD VECTOR INDEX` 在不同数据量/分区数下的建索引时间
5. **写入吞吐退化** - 对比有/无向量索引时的写入性能差异
6. **分布式事务开销** - skip_trx=on vs off 的 QPS 差异
