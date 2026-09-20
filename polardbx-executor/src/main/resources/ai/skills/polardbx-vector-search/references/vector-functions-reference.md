# 向量函数完整参考

## 数据类型

### VECTOR[(dimension)]

向量数据类型，存储定长 float32 向量。

| 属性 | 说明 |
|------|------|
| 存储格式 | float32 little-endian binary（每维 4 字节） |
| 总大小 | dimension × 4 字节 |
| dimension | 可选，建表时指定维度约束（0 表示不限） |
| SQL 映射 | VARBINARY |

**建表语法：**
```sql
column_name VECTOR(dimension)
-- 或不指定维度
column_name VECTOR
```

---

## 向量转换函数

### VEC_FROMTEXT(text)

将 JSON 数组格式的文本转为 VECTOR 二进制格式。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| text | VARCHAR | 是 | JSON 浮点数组，如 `'[0.1, 0.2, 0.3]'` |

**返回**: VECTOR（binary）— 可直接存入 VECTOR 列

**示例：**
```sql
INSERT INTO t VALUES (1, VEC_FROMTEXT('[0.1,0.2,0.3,0.4]'));
```

---

### VEC_TOTEXT(vector)

将 VECTOR 二进制格式转为 JSON 数组文本。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| vector | VECTOR | 是 | VECTOR 列或 VEC_FROMTEXT 结果 |

**返回**: VARCHAR — JSON 浮点数组文本，如 `[0.1,0.2,0.3,0.4]`

**输出格式说明：**
- 6 位有效数字（匹配 MySQL FLT_DIG）
- 整数不带 ".0" 后缀
- 极端值使用科学计数法（不带 '+' 号，如 `1e15`）

---

### VECTOR_DIM(vector)

返回向量的维度数。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| vector | VECTOR | 是 | VECTOR 列或 VEC_FROMTEXT 结果 |

**返回**: BIGINT — 维度数（= 二进制长度 / 4）

---

## 距离计算函数

### VEC_DISTANCE(column, vector_literal)

计算两个向量之间的距离。距离度量类型由该列上的 VECTOR INDEX 的 `DISTANCE` 参数决定。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| column | VECTOR 列 | 是 | 表中的 VECTOR 列引用 |
| vector_literal | VECTOR | 是 | 查询向量（通常用 `VEC_FROMTEXT(...)` 包裹） |

**返回**: DOUBLE — 距离值（越小越相似）

**前置条件**: 目标列必须已创建 VECTOR INDEX（用于确定距离度量类型）

**典型用法：**
```sql
SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('[0.1,0.2,0.3]')) AS dist
FROM documents ORDER BY dist LIMIT 10;
```

---

### VEC_DISTANCE_COSINE(vector1, vector2)

显式使用余弦距离计算两个向量的距离，不依赖 VECTOR INDEX。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| vector1 | VECTOR | 是 | 第一个向量（列引用或 VEC_FROMTEXT） |
| vector2 | VECTOR | 是 | 第二个向量 |

**返回**: DOUBLE — 余弦距离（0 = 完全相同方向，2 = 完全相反）

---

### VEC_DISTANCE_EUCLIDEAN(vector1, vector2)

显式使用欧氏距离计算两个向量的距离，不依赖 VECTOR INDEX。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| vector1 | VECTOR | 是 | 第一个向量（列引用或 VEC_FROMTEXT） |
| vector2 | VECTOR | 是 | 第二个向量 |

**返回**: DOUBLE — 欧氏距离（L2 距离，≥ 0，0 = 完全相同）

---

## 向量索引 DDL

### CREATE VECTOR INDEX

```sql
CREATE VECTOR INDEX index_name ON table_name(column_name) M=N DISTANCE=metric;
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| index_name | 标识符 | 是 | 索引名称 |
| table_name | 标识符 | 是 | 目标表 |
| column_name | 标识符 | 是 | VECTOR 类型的列名 |
| M | INT | 是 | HNSW 连接数（推荐 6~64，越大越精确但越慢） |
| DISTANCE | ENUM | 是 | 距离度量：`COSINE` / `EUCLIDEAN` |

**内联建表语法：**
```sql
CREATE TABLE t (
  id BIGINT PRIMARY KEY,
  embedding VECTOR(768),
  VECTOR INDEX idx_name(embedding) M=16 DISTANCE=COSINE
) PARTITION BY HASH(id) PARTITIONS 4;
```

**ALTER TABLE 语法：**
```sql
ALTER TABLE t ADD VECTOR INDEX idx_name(embedding) M=16 DISTANCE=COSINE;
```

### DROP INDEX（删除向量索引）

```sql
DROP INDEX index_name ON table_name;
```

---

## 函数对比速查

| 函数 | 需要索引 | 距离类型 | 参数数 | 用途 |
|------|----------|----------|--------|------|
| `VEC_DISTANCE` | 是 | 由索引决定 | 2 | ANN 搜索（性能最佳） |
| `VEC_DISTANCE_COSINE` | 否 | 余弦 | 2 | 精确余弦距离计算 |
| `VEC_DISTANCE_EUCLIDEAN` | 否 | 欧氏 | 2 | 精确欧氏距离计算 |
| `VEC_FROMTEXT` | — | — | 1 | 文本→VECTOR 二进制 |
| `VEC_TOTEXT` | — | — | 1 | VECTOR 二进制→文本 |
| `VECTOR_DIM` | — | — | 1 | 获取维度数 |
