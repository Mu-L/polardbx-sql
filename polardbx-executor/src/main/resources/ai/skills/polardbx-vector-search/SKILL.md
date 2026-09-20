---
name: vector-search
description: |
  PolarDB-X 向量搜索使用指南。覆盖 VECTOR 数据类型、向量函数（VEC_FROMTEXT/VEC_TOTEXT/VEC_DISTANCE/VEC_DISTANCE_COSINE/VEC_DISTANCE_EUCLIDEAN/VECTOR_DIM）、VECTOR INDEX 创建与管理、ANN 近似最近邻搜索的完整用法与最佳实践。
  Use when the user asks how to use vector search features, VECTOR data type, vector index, ANN queries, or wants examples of similarity search in SQL.
  Triggers: "向量", "VECTOR", "VEC_DISTANCE", "VEC_FROMTEXT", "VEC_TOTEXT", "VECTOR_DIM", "向量索引", "VECTOR INDEX", "ANN", "近似最近邻", "相似度搜索", "embedding存储", "向量检索", "VEC_DISTANCE_COSINE", "VEC_DISTANCE_EUCLIDEAN", "cosine distance", "euclidean distance"
metadata:
  version: 0.1.0
---

# PolarDB-X 向量搜索使用指南

PolarDB-X 原生支持向量数据类型和向量索引，允许你直接在 SQL 中进行 ANN（近似最近邻）搜索，无需外部向量数据库。

## VECTOR 数据类型

VECTOR 类型存储定长浮点向量，底层为 float32 little-endian 二进制格式（每维 4 字节）。

```sql
-- 建表时指定维度（推荐）
CREATE TABLE documents (
  id BIGINT PRIMARY KEY,
  content TEXT,
  embedding VECTOR(768)    -- 768 维向量
) PARTITION BY HASH(id) PARTITIONS 4;

-- 不指定维度（接受任意长度）
CREATE TABLE flexible_vec (
  id BIGINT PRIMARY KEY,
  vec VECTOR
) PARTITION BY HASH(id) PARTITIONS 4;
```

## 向量函数

### VEC_FROMTEXT — 文本转向量二进制

将 JSON 数组格式的文本转为 VECTOR 二进制格式，用于 INSERT 数据。

```sql
-- 插入向量数据
INSERT INTO documents(id, embedding)
VALUES (1, VEC_FROMTEXT('[0.1, 0.2, 0.3, 0.4]'));

-- 批量插入
INSERT INTO documents(id, content, embedding)
VALUES
  (1, '文档1', VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')),
  (2, '文档2', VEC_FROMTEXT('[0.5,0.6,0.7,0.8]')),
  (3, '文档3', VEC_FROMTEXT('[0.9,0.8,0.7,0.6]'));
```

### VEC_TOTEXT — 向量二进制转文本

将 VECTOR 二进制格式转为可读的 JSON 数组文本。

```sql
-- 查看存储的向量值
SELECT id, VEC_TOTEXT(embedding) FROM documents WHERE id = 1;
-- 返回: [0.1,0.2,0.3,0.4]
```

### VECTOR_DIM — 获取向量维度

返回向量的维度数。

```sql
SELECT VECTOR_DIM(embedding) FROM documents WHERE id = 1;
-- 返回: 4
```

### VEC_DISTANCE — 向量距离计算（使用索引定义的距离度量）

计算两个向量之间的距离。**距离度量类型由 VECTOR INDEX 的 DISTANCE 参数决定**，不需要在函数中额外指定。

```sql
-- 需要先有 VECTOR INDEX（定义距离度量）
CREATE VECTOR INDEX vec_idx ON documents(embedding) M=6 DISTANCE=COSINE;

-- 计算距离
SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist
FROM documents
ORDER BY dist
LIMIT 10;
```

### VEC_DISTANCE_COSINE — 余弦距离（显式指定）

不依赖索引，直接使用余弦距离计算。

```sql
SELECT id, VEC_DISTANCE_COSINE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist
FROM documents
ORDER BY dist
LIMIT 10;
```

### VEC_DISTANCE_EUCLIDEAN — 欧氏距离（显式指定）

不依赖索引，直接使用欧氏距离计算。

```sql
SELECT id, VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist
FROM documents
ORDER BY dist
LIMIT 10;
```

## VECTOR INDEX（向量索引）

向量索引是 ANN 搜索的加速结构。支持以下距离度量：
- `COSINE` — 余弦距离（推荐用于文本 embedding）
- `EUCLIDEAN` — 欧氏距离

### 创建向量索引

```sql
-- 独立创建（推荐）
CREATE VECTOR INDEX vec_idx ON table_name(column_name) M=6 DISTANCE=COSINE;

-- 建表时内联创建
CREATE TABLE documents (
  id BIGINT PRIMARY KEY,
  embedding VECTOR(768),
  VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=COSINE
) PARTITION BY HASH(id) PARTITIONS 4;

-- ALTER TABLE 添加
ALTER TABLE documents ADD VECTOR INDEX vec_idx(embedding) M=6 DISTANCE=COSINE;
```

**参数说明：**
| 参数 | 说明 |
|------|------|
| `M` | HNSW 图的连接数，值越大精度越高但构建越慢（推荐 6~64） |
| `DISTANCE` | 距离度量类型：`COSINE` 或 `EUCLIDEAN` |

### 删除向量索引

```sql
DROP INDEX vec_idx ON documents;
```

## ANN 搜索模式

典型的 ANN（近似最近邻）搜索使用 `ORDER BY VEC_DISTANCE(...) LIMIT N` 模式：

```sql
-- 基本 ANN 搜索：找最相似的 Top-K
SELECT id, content,
  VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist
FROM documents
ORDER BY dist
LIMIT 10;

-- 结合 WHERE 条件的混合查询
SELECT id, content,
  VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]')) AS dist
FROM documents
WHERE category = 'tech'
ORDER BY dist
LIMIT 5;
```

## 典型场景

### 场景1: 语义搜索

```sql
-- 1. 建表（含向量列和向量索引）
CREATE TABLE knowledge_base (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  title VARCHAR(200),
  content TEXT,
  embedding VECTOR(768),
  VECTOR INDEX emb_idx(embedding) M=16 DISTANCE=COSINE
) PARTITION BY HASH(id) PARTITIONS 8;

-- 2. 存入文档 embedding（配合 AI_EMBEDDING 函数生成）
INSERT INTO knowledge_base(title, content, embedding)
VALUES ('PolarDB-X简介', '...内容...', VEC_FROMTEXT(AI_EMBEDDING('PolarDB-X简介...', 'my-embedding')));

-- 3. 语义检索
SELECT id, title, content,
  VEC_DISTANCE(embedding, VEC_FROMTEXT(AI_EMBEDDING('什么是分布式数据库', 'my-embedding'))) AS score
FROM knowledge_base
ORDER BY score
LIMIT 5;
```

注意：AI_EMBEDDING、AI_VL_EMBEDDING 等 AI 函数的完整用法请参考 ai-functions 技能。

### 场景2: 图片相似搜索

```sql
-- 存储图片 embedding
CREATE TABLE image_gallery (
  id BIGINT PRIMARY KEY,
  image_url VARCHAR(500),
  embedding VECTOR(1024),
  VECTOR INDEX img_idx(embedding) M=16 DISTANCE=COSINE
) PARTITION BY HASH(id) PARTITIONS 4;

-- 以图搜图
SELECT id, image_url,
  VEC_DISTANCE(embedding, VEC_FROMTEXT('[...]')) AS dist
FROM image_gallery
ORDER BY dist
LIMIT 20;
```

### 场景3: 推荐系统

```sql
-- 用户向量与物品向量的相似度匹配
SELECT item_id, item_name,
  VEC_DISTANCE_COSINE(item_embedding, VEC_FROMTEXT('[用户向量]')) AS similarity
FROM items
ORDER BY similarity
LIMIT 50;
```

## 注意事项

1. `VEC_DISTANCE` 依赖 VECTOR INDEX 来确定距离度量类型，使用前需确保目标列已创建向量索引
2. `VEC_DISTANCE_COSINE` 和 `VEC_DISTANCE_EUCLIDEAN` 可以不依赖索引独立使用
3. VECTOR 列的维度在建表时指定后，INSERT 的向量维度必须匹配
4. 向量数据通过 `VEC_FROMTEXT('[0.1,0.2,...]')` 格式写入，格式为 JSON 浮点数组
5. ANN 搜索利用 `ORDER BY VEC_DISTANCE(...) LIMIT N` 模式触发近似搜索优化
6. M 参数影响索引质量：值越大召回率越高，但索引构建和查询开销也越大

## Reference Links

| Reference | Description |
|-----------|-------------|
| [references/vector-functions-reference.md](references/vector-functions-reference.md) | 所有向量函数的完整参数说明与语法格式 |
