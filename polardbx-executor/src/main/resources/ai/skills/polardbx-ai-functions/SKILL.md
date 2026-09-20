---
name: ai-functions
description: |
  PolarDB-X AI 函数使用指南。覆盖模型注册管理、AI 推理函数（AI_PROMPT/AI_EMBEDDING/AI_SIMILARITY/AI_CLASSIFY/AI_EXTRACT/AI_SUMMARIZE/AI_RANK/AI_TEXT2SQL/AI_PARSE_DOCUMENT/AI_VL_EMBEDDING）的完整用法与最佳实践。
  Use when the user asks how to use AI functions in SQL, how to register/manage AI models, how to call LLM/embedding/rerank from SQL, or wants examples of AI-powered SQL queries.
  Triggers: "AI函数", "AI_PROMPT", "AI_EMBEDDING", "AI_SIMILARITY", "AI_CLASSIFY", "AI_EXTRACT", "AI_SUMMARIZE", "AI_RANK", "AI_TEXT2SQL", "AI_PARSE_DOCUMENT", "AI_VL_EMBEDDING", "AI_REGISTER_MODEL", "注册模型", "调用大模型", "embedding", "向量化", "文本分类", "信息提取", "文本摘要", "相似度", "rerank", "text2sql", "文档解析", "多模态embedding"
metadata:
  version: 0.1.0
---

# PolarDB-X AI 函数使用指南

PolarDB-X 内置了一套 AI 函数，允许你直接在 SQL 中调用大语言模型、Embedding 模型、Rerank 模型等 AI 能力，无需外部服务集成。

## 前置条件

使用 AI 函数前，需要先注册模型配置（指定 provider、endpoint、model 名和 API Key）。

## 模型管理函数

| 函数 | 用途 | 语法 |
|------|------|------|
| `AI_REGISTER_MODEL` | 注册新模型配置 | `SELECT AI_REGISTER_MODEL(name, provider, endpoint, model [, options_json])` |
| `AI_UPDATE_MODEL` | 更新模型配置 | `SELECT AI_UPDATE_MODEL(name, options_json)` |
| `AI_DROP_MODEL` | 删除模型配置 | `SELECT AI_DROP_MODEL(name)` |
| `AI_LIST_MODELS` | 列出所有已注册模型 | `SELECT AI_LIST_MODELS()` |
| `AI_DESCRIBE_MODEL` | 查看模型详情 | `SELECT AI_DESCRIBE_MODEL(name)` |

### 模型注册示例

```sql
-- 注册 LLM 模型（通义千问）
SELECT AI_REGISTER_MODEL(
  'my-qwen-plus',           -- 模型配置名（自定义）
  'dashscope',              -- provider: dashscope / openai
  'https://dashscope.aliyuncs.com',  -- API endpoint
  'qwen-plus',              -- 模型名
  '{"api_key": "sk-xxx", "description": "通义千问Plus"}'  -- 选项（含 API Key）
);

-- 注册 Embedding 模型
SELECT AI_REGISTER_MODEL(
  'my-embedding',
  'dashscope',
  'https://dashscope.aliyuncs.com',
  'text-embedding-v3',
  '{"api_key": "sk-xxx", "type": "EMBEDDING"}'
);

-- 注册 Rerank 模型
SELECT AI_REGISTER_MODEL(
  'my-rerank',
  'dashscope',
  'https://dashscope.aliyuncs.com',
  'gte-rerank',
  '{"api_key": "sk-xxx", "type": "RERANK"}'
);

-- 注册 VL Embedding 模型（多模态）
SELECT AI_REGISTER_MODEL(
  'my-vl-embed',
  'dashscope',
  'https://dashscope.aliyuncs.com',
  'multimodal-embedding-one-peace-v1',
  '{"api_key": "sk-xxx", "type": "VL_EMBEDDING"}'
);
```

## AI 推理函数

### AI_PROMPT — 调用 LLM 生成文本

```sql
-- 基本用法
SELECT AI_PROMPT('What is PolarDB-X?');

-- 指定模型
SELECT AI_PROMPT('解释一下分布式事务', 'my-qwen-plus');

-- 带选项
SELECT AI_PROMPT('写一首诗', 'my-qwen-plus', '{"temperature": 0.9, "max_tokens": 500}');

-- 结合表数据：为每条评论生成回复建议
SELECT id, content, AI_PROMPT(CONCAT('请为以下用户评论生成一条友好的客服回复: ', content), 'my-qwen-plus')
FROM reviews WHERE status = 'pending' LIMIT 10;
```

### AI_EMBEDDING — 生成文本向量

```sql
-- 基本用法
SELECT AI_EMBEDDING('PolarDB-X is a distributed database');

-- 指定模型和维度
SELECT AI_EMBEDDING('Hello', 'my-embedding', '{"dimension": 512}');

-- 将文本转为向量存入表中
INSERT INTO documents(id, content, embedding)
SELECT id, content, VEC_FROMTEXT(AI_EMBEDDING(content, 'my-embedding'))
FROM raw_docs;
```

### AI_SIMILARITY — 计算相似度

```sql
-- 文本相似度（自动调用 embedding）
SELECT AI_SIMILARITY('cloud database', 'distributed SQL');

-- 向量相似度
SELECT AI_SIMILARITY('[0.1, 0.2, 0.3]', '[0.4, 0.5, 0.6]');

-- 指定距离类型: cosine(默认), euclidean, dot
SELECT AI_SIMILARITY('[1,2,3]', '[4,5,6]', 'euclidean');

-- 语义搜索: 找最相似的文档
SELECT id, title, AI_SIMILARITY(embedding, AI_EMBEDDING('query text')) AS score
FROM documents ORDER BY score DESC LIMIT 10;
```

### AI_CLASSIFY — 文本分类

```sql
-- 情感分类
SELECT AI_CLASSIFY('This product is amazing!', '["positive","negative","neutral"]');
-- 返回: 'positive'

-- 批量分类
SELECT id, content, AI_CLASSIFY(content, '["tech","sports","politics"]') AS category
FROM articles;
```

### AI_EXTRACT — 结构化信息提取

```sql
-- 从文本中提取字段
SELECT AI_EXTRACT(
  '请联系张三，电话：13800138000，邮箱：zhangsan@example.com',
  '{"name": "姓名", "phone": "电话号码", "email": "电子邮箱"}'
);
-- 返回: {"name": "张三", "phone": "13800138000", "email": "zhangsan@example.com"}

-- 批量提取产品信息
SELECT id, AI_EXTRACT(description, '{"brand":"品牌","price":"价格","color":"颜色"}')
FROM products WHERE category = 'electronics';
```

### AI_SUMMARIZE — 文本摘要

```sql
-- 基本摘要（默认200字以内）
SELECT AI_SUMMARIZE('A very long article content...');

-- 指定长度
SELECT AI_SUMMARIZE(content, 150) FROM articles WHERE id = 1;

-- 带选项：要点列表格式
SELECT AI_SUMMARIZE(content, 300, 'my-qwen-plus', '{"style": "bullet_points", "language": "Chinese"}')
FROM reports;
```

### AI_RANK — 相关性打分（Rerank）

```sql
-- 计算查询与文档的相关性分数（0~1）
SELECT AI_RANK('database optimization', 'Top 10 database performance tuning tips');

-- 对候选文档重排序
SELECT id, title, AI_RANK('分布式事务', content, 'my-rerank') AS relevance
FROM documents ORDER BY relevance DESC LIMIT 5;
```

### AI_TEXT2SQL — 自然语言转 SQL

```sql
-- 根据当前 schema 自动生成 SQL
SELECT AI_TEXT2SQL('查询所有年龄大于30的用户');
-- 返回: SELECT * FROM users WHERE age > 30

SELECT AI_TEXT2SQL('统计每个品类的总销售额');
-- 返回: SELECT category, SUM(amount) FROM orders GROUP BY category
```

### AI_PARSE_DOCUMENT — 文档解析

```sql
-- 解析 PDF 文档为文本
SELECT AI_PARSE_DOCUMENT('https://example.com/doc.pdf');

-- 解析图片（OCR）
SELECT AI_PARSE_DOCUMENT('https://example.com/image.jpg', 'text_and_images');
```

### AI_VL_EMBEDDING — 多模态向量化

```sql
-- 文本向量化
SELECT AI_VL_EMBEDDING('PolarDB-X is a distributed database');

-- 图片向量化（URL）
SELECT AI_VL_EMBEDDING('https://example.com/image.jpg');

-- 指定维度
SELECT AI_VL_EMBEDDING('Hello world', 'my-vl-embed', '{"dimension": 1024}');
```

## 典型场景

### 场景1: RAG（检索增强生成）

```sql
-- 1. 将文档内容向量化后存储
INSERT INTO knowledge_base(id, content, embedding)
SELECT id, content, VEC_FROMTEXT(AI_EMBEDDING(content, 'my-embedding'))
FROM raw_docs;

-- 2. 检索最相关的文档
SELECT content FROM knowledge_base
ORDER BY VEC_DISTANCE(embedding, VEC_FROMTEXT(AI_EMBEDDING('用户的问题', 'my-embedding')))
LIMIT 5;

-- 3. 结合检索结果调用 LLM 回答
SELECT AI_PROMPT(CONCAT('基于以下参考资料回答问题...\n参考资料:\n', group_concat(content), '\n\n问题: 用户的问题'));
```

注意：VECTOR 数据类型、VEC_FROMTEXT/VEC_DISTANCE 等向量函数以及 VECTOR INDEX 的完整用法请参考 vector-search 技能。

### 场景2: 数据清洗与标注

```sql
-- 批量分类 + 提取关键信息
SELECT id,
  AI_CLASSIFY(content, '["bug","feature","question"]') AS issue_type,
  AI_EXTRACT(content, '{"component":"涉及模块","priority":"紧急程度"}') AS metadata
FROM issues WHERE label IS NULL;
```

## 注意事项

1. AI 函数执行会调用外部 API，有网络延迟，不建议在大批量查询中使用
2. 需要先通过 `AI_REGISTER_MODEL` 注册模型（含 API Key）才能使用推理函数
3. 默认内置模型需要在实例级别配置 `AI_GATEWAY_KEY` 参数
4. 不同 provider 支持的模型列表不同，请参考对应平台文档

## Reference Links

| Reference | Description |
|-----------|-------------|
| [references/ai-functions-reference.md](references/ai-functions-reference.md) | 所有 AI 函数的完整参数说明与返回值格式 |
