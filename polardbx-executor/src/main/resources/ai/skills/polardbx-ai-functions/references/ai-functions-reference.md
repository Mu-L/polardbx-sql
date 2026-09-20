# AI 函数完整参考

## 模型管理函数

### AI_REGISTER_MODEL(name, provider, endpoint, model [, options_json])

注册一个 AI 模型配置。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | VARCHAR | 是 | 模型配置名（唯一标识） |
| provider | VARCHAR | 是 | API 提供商：`dashscope` / `openai` |
| endpoint | VARCHAR | 是 | API 端点 URL |
| model | VARCHAR | 是 | 模型名称（如 `qwen-plus`、`text-embedding-v3`） |
| options_json | JSON | 否 | 额外配置，支持的字段见下 |

**options_json 支持的字段：**
- `api_key`: API 密钥（必填）
- `description`: 模型描述
- `type`: 模型类型 — `LLM`（默认）/ `EMBEDDING` / `RERANK` / `VL_EMBEDDING` / `DOCUMENT_PARSE`
- `status`: `ACTIVE`（默认）/ `INACTIVE`

### AI_UPDATE_MODEL(name, options_json)

更新已注册模型的配置。options_json 中的字段会覆盖原有值。

### AI_DROP_MODEL(name)

删除模型配置。内置模型（以 `__POLARDBX_` 开头）不可删除。

### AI_LIST_MODELS()

列出所有已注册的模型配置，返回 JSON 数组。

### AI_DESCRIBE_MODEL(name)

返回指定模型的详细配置信息（JSON 格式）。

---

## AI 推理函数

### AI_PROMPT(prompt [, model_name [, options_json]])

调用 LLM 生成文本。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| prompt | TEXT | 是 | 输入提示词 |
| model_name | VARCHAR | 否 | 模型配置名，默认使用内置 LLM |
| options_json | JSON | 否 | `temperature`, `max_tokens`, `top_p`, `stop`, `system_prompt` |

**返回**: TEXT — LLM 生成的文本

---

### AI_EMBEDDING(text [, model_name [, options_json]])

生成文本的 Embedding 向量。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| text | TEXT | 是 | 输入文本 |
| model_name | VARCHAR | 否 | 模型配置名，默认使用内置 EMBEDDING 模型 |
| options_json | JSON | 否 | `dimension`（指定输出维度） |

**返回**: TEXT — JSON 数组格式的浮点数向量，如 `[0.123, -0.456, 0.789, ...]`

---

### AI_SIMILARITY(input1, input2 [, similarity_type [, model_name]])

计算两个输入的相似度。输入可以是文本（自动 embedding）或 JSON 向量数组。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| input1 | TEXT/JSON | 是 | 文本或向量 |
| input2 | TEXT/JSON | 是 | 文本或向量 |
| similarity_type | VARCHAR | 否 | `cosine`（默认）/ `euclidean` / `dot` |
| model_name | VARCHAR | 否 | 当输入为文本时使用的 embedding 模型 |

**返回**: DOUBLE — 相似度分数

---

### AI_CLASSIFY(text, categories [, model_name [, options_json]])

将文本分类到候选类别之一。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| text | TEXT | 是 | 待分类文本 |
| categories | JSON | 是 | 候选类别 JSON 数组，如 `["positive","negative","neutral"]` |
| model_name | VARCHAR | 否 | LLM 模型名 |
| options_json | JSON | 否 | `temperature`, `multi_label`, `threshold` |

**返回**: VARCHAR — 匹配的类别标签

---

### AI_EXTRACT(text, schema [, model_name [, options_json]])

从文本中提取结构化信息。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| text | TEXT | 是 | 输入文本 |
| schema | JSON | 是 | 字段名到描述的映射，如 `{"name":"姓名","phone":"电话"}` |
| model_name | VARCHAR | 否 | LLM 模型名 |
| options_json | JSON | 否 | `temperature`, `max_tokens` |

**返回**: JSON — 提取的字段值对象

---

### AI_SUMMARIZE(text [, max_length [, model_name [, options_json]]])

生成文本摘要。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| text | TEXT | 是 | 输入文本 |
| max_length | INT | 否 | 最大字符数（默认200） |
| model_name | VARCHAR | 否 | LLM 模型名 |
| options_json | JSON | 否 | `language`（目标语言）, `style`（`paragraph`/`bullet_points`） |

**返回**: TEXT — 摘要文本

---

### AI_RANK(query, candidate [, model_name [, options_json]])

计算查询与候选文本的相关性分数。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| query | TEXT | 是 | 查询文本 |
| candidate | TEXT | 是 | 候选文本 |
| model_name | VARCHAR | 否 | Rerank 模型名 |
| options_json | JSON | 否 | 可选参数 |

**返回**: DOUBLE — 相关性分数（0~1）

---

### AI_TEXT2SQL(prompt [, model_name [, options_json]])

将自然语言转换为 SQL，基于当前 schema 的表结构。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| prompt | TEXT | 是 | 自然语言描述 |
| model_name | VARCHAR | 否 | LLM 模型名 |
| options_json | JSON | 否 | `temperature`, `max_tokens` |

**返回**: TEXT — 生成的 SQL 语句

---

### AI_PARSE_DOCUMENT(file_url [, input_format [, model_name [, options_json]]])

解析文档（PDF/Word/PPT/图片等）为文本。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| file_url | TEXT | 是 | 文件 URL（HTTP/HTTPS，需公开可访问） |
| input_format | TEXT | 否 | `auto`（默认）/ `text_only` / `text_and_images` |
| model_name | VARCHAR | 否 | Document Parse 模型名 |
| options_json | JSON | 否 | 可选参数 |

**支持的文件类型**: PDF, Word (doc/docx), PPT (ppt/pptx), TXT, Markdown, HTML, 图片 (jpg/png/bmp/tiff)

**返回**: TEXT — 解析后的文本内容

---

### AI_VL_EMBEDDING(content [, model_name [, options_json]])

多模态 Embedding — 将文本、图片或视频统一向量化到同一语义空间。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| content | TEXT | 是 | 文本内容 / 图片 URL / 视频 URL（自动检测类型） |
| model_name | VARCHAR | 否 | VL Embedding 模型名 |
| options_json | JSON | 否 | `dimension`（输出维度）, `content_type`（强制指定: text/image/video）, `fps`（视频帧率） |

**内容类型自动检测规则：**
- URL 含图片后缀(.jpg/.png/.webp 等) → image
- URL 含视频后缀(.mp4/.avi/.mov) → video  
- 其他 → text

**返回**: TEXT — JSON 数组格式的浮点数向量
