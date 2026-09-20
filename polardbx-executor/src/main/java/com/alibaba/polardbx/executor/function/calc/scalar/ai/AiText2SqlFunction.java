/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.function.calc.scalar.ai;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.executor.ai.AiApiProviderFactory;
import com.alibaba.polardbx.executor.ai.ChatCompletionProvider;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AI_TEXT2SQL(prompt [, model_name [, options_json]])
 *
 * <p>Converts a natural language query into a SQL statement based on the current schema's table structures.
 *
 * <p>The function:
 * <ol>
 *   <li>Retrieves all table metadata from the current schema</li>
 *   <li>When the schema has more than {@code MAX_RELEVANT_TABLES} tables, asks the LLM to pick
 *       the relevant ones; otherwise uses all tables directly</li>
 *   <li>Generates CREATE TABLE definitions for the selected tables</li>
 *   <li>Sends the prompt along with table definitions to the LLM for SQL generation</li>
 *   <li>Returns the generated SQL statement</li>
 * </ol>
 *
 * <p>Parameters:
 * <ul>
 *   <li>prompt (TEXT, required) - The natural language description of the desired query</li>
 *   <li>model_name (VARCHAR, optional) - Model config name; uses default LLM model if omitted</li>
 *   <li>options_json (JSON, optional) - Override parameters: temperature, max_tokens, etc.</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 * SELECT AI_TEXT2SQL('Find all users older than 30');
 * SELECT AI_TEXT2SQL('Show total revenue by product category', 'my_model');
 * </pre>
 */
public class AiText2SqlFunction extends AbstractScalarFunction implements AiFunctionMetadata {
    public AiText2SqlFunction() {
    }

    public AiText2SqlFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    private static final int MAX_RELEVANT_TABLES = 10;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1 || args[0] == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_TEXT2SQL requires at least 1 argument: prompt");
        }

        String prompt = DataTypes.StringType.convertFrom(args[0]);
        if (prompt == null || prompt.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_TEXT2SQL: prompt cannot be empty");
        }

        // Model name: use default if not specified
        String modelName = ModelManager.getInstance().getDefaultModelForFunction("AI_TEXT2SQL");
        if (args.length > 1 && args[1] != null) {
            String specified = DataTypes.StringType.convertFrom(args[1]);
            if (specified != null && !specified.trim().isEmpty()) {
                modelName = specified.trim();
            }
        }

        if (modelName == null || modelName.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_TEXT2SQL: no model specified and no default model configured. "
                    + "Use AI_UPDATE_FUNCTION to set a default model or pass model name explicitly.");
        }

        JSONObject options = null;
        if (args.length > 2 && args[2] != null) {
            String optionsStr = DataTypes.StringType.convertFrom(args[2]);
            if (optionsStr != null && !optionsStr.trim().isEmpty()) {
                try {
                    options = JSON.parseObject(optionsStr);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "AI_TEXT2SQL: invalid options JSON: " + e.getMessage());
                }
            }
        }

        // Get schema name from execution context
        String schemaName = ec.getSchemaName();

        // Resolve model config first (needed for LLM-based table filtering)
        ModelManager modelManager = ModelManager.getInstance();
        ModelConfigRecord modelConfig = modelManager.getModelConfig(modelName.trim());
        if (modelConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_TEXT2SQL: model '" + modelName + "' not found. "
                    + "Use AI_REGISTER_MODEL to register it first.");
        }

        if (modelConfig.apiKey == null || modelConfig.apiKey.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "AI_TEXT2SQL: model '" + modelConfig.name + "' has no API key configured.");
        }

        ChatCompletionProvider provider = AiApiProviderFactory.getChatProvider(modelConfig.provider);

        // Get table definitions: LLM filters relevant tables when schema is large
        String tableDefinitions = getRelevantTableDefinitions(schemaName, prompt, provider, modelConfig, ec);

        // Build the system prompt for text2sql
        String systemPrompt = buildSystemPrompt(tableDefinitions);

        // Merge system_prompt into options
        if (options == null) {
            options = new JSONObject();
        }
        options.put("system_prompt", systemPrompt);

        // Call the LLM for the final SQL generation
        String result = (String) provider.chatCompletion(modelConfig, prompt, options);

        // Clean up the result: strip markdown code fences if present
        return cleanSqlResult(result);
    }

    /**
     * Get relevant table definitions from the schema.
     * Uses LLM to select relevant tables when there are many; otherwise uses all.
     */
    private String getRelevantTableDefinitions(String schemaName, String prompt,
                                               ChatCompletionProvider provider, ModelConfigRecord modelConfig,
                                               ExecutionContext ec) {
        OptimizerContext context = OptimizerContext.getContext(schemaName);
        if (context == null) {
            return "";
        }

        SchemaManager schemaManager = context.getLatestSchemaManager();
        if (schemaManager == null) {
            return "";
        }

        Collection<TableMeta> allTables = schemaManager.getAllUserTables();
        if (allTables == null || allTables.isEmpty()) {
            return "";
        }

        List<TableMeta> tableList = new ArrayList<>(allTables);

        // If there are too many tables, use LLM to filter relevant ones first
        List<TableMeta> relevantTables;
        if (tableList.size() > MAX_RELEVANT_TABLES) {
            relevantTables = filterRelevantTablesByLlm(prompt, tableList, provider, modelConfig);
            if (relevantTables.isEmpty()) {
                relevantTables = tableList.stream().limit(MAX_RELEVANT_TABLES).collect(Collectors.toList());
            }
        } else {
            relevantTables = tableList;
        }

        // Generate CREATE TABLE statements for the selected tables
        StringBuilder sb = new StringBuilder();
        for (TableMeta table : relevantTables) {
            sb.append(generateCreateTableDDL(schemaName, table, ec));
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Ask the LLM to select which tables from the full list are relevant to the user's prompt.
     * The LLM is given only table names (and column names) to keep the context small,
     * and is asked to respond with a comma-separated list of table names.
     */
    private List<TableMeta> filterRelevantTablesByLlm(String prompt, List<TableMeta> allTables,
                                                      ChatCompletionProvider provider, ModelConfigRecord modelConfig) {
        // Build a compact schema summary: table_name(col1, col2, ...)
        StringBuilder schemaSummary = new StringBuilder();
        for (TableMeta table : allTables) {
            schemaSummary.append(table.getTableName()).append("(");
            List<ColumnMeta> cols = table.getAllColumns();
            if (cols != null) {
                schemaSummary.append(
                    cols.stream().map(ColumnMeta::getName).collect(Collectors.joining(", ")));
            }
            schemaSummary.append(")\n");
        }

        String filterPrompt = "Given the following database tables and their columns:\n\n"
            + schemaSummary
            + "\nUser question: " + prompt
            + "\n\nWhich tables are needed to answer this question? "
            + "Reply with ONLY a comma-separated list of table names, nothing else. "
            + "Return at most " + MAX_RELEVANT_TABLES + " tables. "
            + "Example response: orders,customers,products";

        JSONObject filterOptions = new JSONObject();
        filterOptions.put("max_tokens", 200);
        filterOptions.put("temperature", 0);

        String response;
        try {
            response = provider.chatCompletion(modelConfig, filterPrompt, filterOptions);
        } catch (Exception e) {
            // If the LLM call fails, fall back to returning empty (caller will use default)
            return Collections.emptyList();
        }

        if (response == null || response.trim().isEmpty()) {
            return Collections.emptyList();
        }

        // Parse the comma-separated table names returned by LLM
        Set<String> selectedNames = Arrays.stream(response.split("[,\\s]+"))
            .map(String::trim)
            .map(String::toLowerCase)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toSet());

        return allTables.stream()
            .filter(t -> selectedNames.contains(t.getTableName().toLowerCase()))
            .collect(Collectors.toList());
    }

    /**
     * Generate a CREATE TABLE DDL statement from TableMeta.
     * Uses the same method as SHOW CREATE TABLE to ensure accurate DDL.
     */
    private String generateCreateTableDDL(String schemaName, TableMeta table, ExecutionContext ec) {
        // Use the same approach as LogicalShowCreateTableHandler.fetchShowCreateTableFromMetaDb
        MySqlCreateTableStatement createTableStmt = LogicalShowCreateTableHandler
            .fetchShowCreateTableFromMetaDb(schemaName, table.getTableName(), ec, table);

        if (createTableStmt == null) {
            // Fallback to simple DDL if MetaDB fetch fails
            return generateSimpleCreateTableDDL(table);
        }

        return createTableStmt.toString() + ";\n";
    }

    /**
     * Fallback method to generate a simple CREATE TABLE DDL when MetaDB fetch fails.
     */
    private String generateSimpleCreateTableDDL(TableMeta table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `").append(table.getTableName()).append("` (\n");

        List<ColumnMeta> columns = table.getAllColumns();
        List<String> primaryKeys = new ArrayList<>();

        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta col = columns.get(i);
            sb.append("  `").append(col.getName()).append("` ");

            // Get data type string
            DataType dataType = col.getDataType();
            if (dataType != null) {
                sb.append(dataType.getStringSqlType());
            } else {
                sb.append("VARCHAR(255)");
            }

            if (!col.isNullable()) {
                sb.append(" NOT NULL");
            }

            if (col.isAutoIncrement()) {
                sb.append(" AUTO_INCREMENT");
            }

            if (i < columns.size() - 1 || !primaryKeys.isEmpty()) {
                sb.append(",");
            }
            sb.append("\n");

            if (table.getPrimaryIndex() != null
                && table.getPrimaryIndex().getKeyColumns() != null
                && table.getPrimaryIndex().getKeyColumns().stream()
                .anyMatch(k -> k.getName().equalsIgnoreCase(col.getName()))) {
                primaryKeys.add(col.getName());
            }
        }

        if (!primaryKeys.isEmpty()) {
            sb.append("  PRIMARY KEY (");
            sb.append(primaryKeys.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", ")));
            sb.append(")\n");
        }

        sb.append(");\n");
        return sb.toString();
    }

    /**
     * Build system prompt for the LLM with table schema context.
     */
    private String buildSystemPrompt(String tableDefinitions) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a SQL expert. Convert natural language queries into valid SQL statements.\n\n");
        sb.append("Rules:\n");
        sb.append("1. Only output the SQL statement, no explanation or markdown formatting.\n");
        sb.append("2. Use only the tables and columns provided below.\n");
        sb.append("3. Use standard SQL syntax compatible with MySQL.\n");
        sb.append("4. If the query is ambiguous, make reasonable assumptions.\n\n");

        if (tableDefinitions != null && !tableDefinitions.isEmpty()) {
            sb.append("Available table schemas:\n\n");
            sb.append(tableDefinitions);
        }

        return sb.toString();
    }

    /**
     * Clean up the LLM result by removing markdown code fences.
     */
    private String cleanSqlResult(String result) {
        if (result == null) {
            return null;
        }
        result = result.trim();

        // Remove ```sql ... ``` wrapper
        if (result.startsWith("```sql")) {
            result = result.substring(6);
        } else if (result.startsWith("```SQL")) {
            result = result.substring(6);
        } else if (result.startsWith("```")) {
            result = result.substring(3);
        }
        if (result.endsWith("```")) {
            result = result.substring(0, result.length() - 3);
        }
        return result.trim();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AI_TEXT2SQL"};
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.StringType;
    }

    @Override
    public String getDescription() {
        return "Convert natural language to SQL using current schema context";
    }

    @Override
    public String getDefaultModelName() {
        return ModelManager.getInstance().getDefaultModelForFunction("AI_TEXT2SQL");
    }

}
