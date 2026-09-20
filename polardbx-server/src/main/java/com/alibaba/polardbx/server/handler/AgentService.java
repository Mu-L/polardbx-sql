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

package com.alibaba.polardbx.server.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ai.AgentSession;
import com.alibaba.polardbx.executor.ai.AiApiProviderFactory;
import com.alibaba.polardbx.executor.ai.AiParamUtils;
import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.executor.ai.OpenAiApiProvider;
import com.alibaba.polardbx.executor.ai.SkillManager;
import com.alibaba.polardbx.executor.ai.StreamEventHandler;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.gms.metadb.model.SkillConfigRecord;
import com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.parser.ServerParse;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Agent loop core for NL2SQL.
 * Manages the iterative LLM + tool call cycle using DashScope OpenAI-compatible endpoint.
 */
public class AgentService {

    private static final Logger logger = LoggerFactory.getLogger(AgentService.class);
    private static final Logger nl2sqlLog = LoggerFactory.getLogger("NL2SQL");

    private static final String TOOL_NAME = "execute_sql";
    private static final String TOOL_NAME_QUERY = "query_sql";
    private static final String TOOL_NAME_PLAN = "set_plan";
    private static final int MAX_RESULT_ROWS = 50;
    private static final int EXPLAIN_TIMEOUT_MS = 5000;
    private static final Pattern ROWCOUNT_PATTERN =
        Pattern.compile("rowcount\\s*=\\s*([0-9]+(?:\\.[0-9]+)?(?:E[-+]?[0-9]+)?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern PHYSICAL_ROWS_PATTERN =
        Pattern.compile("rows:([0-9]+(?:\\.[0-9]+)?(?:E[-+]?[0-9]+)?)", Pattern.CASE_INSENSITIVE);

    /**
     * SQL types allowed in READ_ONLY mode.
     */
    private static final Set<Integer> READ_ONLY_ALLOWED;
    /**
     * SQL types allowed in READ_WRITE mode (superset of READ_ONLY).
     */
    private static final Set<Integer> READ_WRITE_ALLOWED;
    /**
     * Transaction control statements — always allowed regardless of policy.
     */
    private static final Set<Integer> TRANSACTION_ALLOWED;

    static {
        Set<Integer> ro = new HashSet<>();
        ro.add(ServerParse.SELECT);
        ro.add(ServerParse.SHOW);
        ro.add(ServerParse.USE);
        READ_ONLY_ALLOWED = Collections.unmodifiableSet(ro);

        Set<Integer> rw = new HashSet<>(ro);
        rw.add(ServerParse.INSERT);
        rw.add(ServerParse.UPDATE);
        rw.add(ServerParse.DELETE);
        rw.add(ServerParse.REPLACE);
        READ_WRITE_ALLOWED = Collections.unmodifiableSet(rw);

        Set<Integer> tx = new HashSet<>();
        tx.add(ServerParse.BEGIN);
        tx.add(ServerParse.COMMIT);
        tx.add(ServerParse.ROLLBACK);
        tx.add(ServerParse.SAVEPOINT);
        tx.add(ServerParse.START);
        TRANSACTION_ALLOWED = Collections.unmodifiableSet(tx);
    }

    // --- Inner data classes ---

    /**
     * Result of a single SQL execution (internal).
     */
    static class SqlExecResult {
        final String text;
        final int rowCount;
        final boolean success;
        // Raw result set data for direct client display (null for DML/errors)
        final List<String> columns;
        final List<List<String>> rows;

        SqlExecResult(String text, int rowCount, boolean success) {
            this(text, rowCount, success, null, null);
        }

        SqlExecResult(String text, int rowCount, boolean success,
                      List<String> columns, List<List<String>> rows) {
            this.text = text;
            this.rowCount = rowCount;
            this.success = success;
            this.columns = columns;
            this.rows = rows;
        }
    }

    /**
     * A streaming step emitted during the agent loop.
     */
    public static class AgentStep {
        public enum StepType {SQL_ACTION, SQL_RESULT, THOUGHT, ANSWER, PLAN, CONTEXT_COMPRESS}

        public final StepType type;
        public final int sqlIndex;       // for SQL_ACTION and SQL_RESULT
        public final String sql;         // for SQL_ACTION
        public final String reason;      // for SQL_ACTION (intent/reason)
        public final String resultText;  // for SQL_RESULT and THOUGHT
        public final int rowCount;       // for SQL_RESULT
        public final long timeMs;        // for SQL_RESULT
        public final boolean success;    // for SQL_RESULT
        // Raw result set for direct client display (null if DML/error)
        public final List<String> columns;
        public final List<List<String>> rows;
        // Plan steps for PLAN type
        public final List<String> planSteps;

        private AgentStep(StepType type, int sqlIndex, String sql, String reason,
                          String resultText, int rowCount, long timeMs, boolean success,
                          List<String> columns, List<List<String>> rows,
                          List<String> planSteps) {
            this.type = type;
            this.sqlIndex = sqlIndex;
            this.sql = sql;
            this.reason = reason;
            this.resultText = resultText;
            this.rowCount = rowCount;
            this.timeMs = timeMs;
            this.success = success;
            this.columns = columns;
            this.rows = rows;
            this.planSteps = planSteps;
        }

        public static AgentStep sqlAction(int index, String sql, String reason) {
            return new AgentStep(StepType.SQL_ACTION, index, sql, reason, null, 0, 0, true, null, null, null);
        }

        public static AgentStep sqlResult(int index, String resultText, int rowCount,
                                          long timeMs, boolean success,
                                          List<String> columns, List<List<String>> rows) {
            return new AgentStep(StepType.SQL_RESULT, index, null, null, resultText, rowCount, timeMs, success,
                columns, rows, null);
        }

        public static AgentStep plan(List<String> steps) {
            return new AgentStep(StepType.PLAN, 0, null, null, null, 0, 0, true, null, null, steps);
        }

    }

    // ==================== Streaming API ====================

    /**
     * Listener for streaming agent output. Receives token-level chunks for THOUGHT/ANSWER,
     * and complete steps for SQL_ACTION/SQL_RESULT.
     */
    public interface StreamingStepListener {
        void onStreamBegin(AgentStep.StepType type);

        void onStreamChunk(String chunk);

        void onStreamEnd(AgentStep.StepType type);

        void onStep(AgentStep step);
    }

    /**
     * Streaming version of the Agent loop with an optional schema-switch callback.
     * The callback is invoked when the LLM successfully executes a USE statement.
     */
    public static void chatStreaming(AgentSession session, String userInput,
                                     String schemaName, String modelName,
                                     String userName, String userHost,
                                     AgentConnectionContext connectionContext,
                                     StreamingStepListener listener,
                                     BooleanSupplier isAlive,
                                     Consumer<String> schemaSwitcher) {
        long startTime = System.currentTimeMillis();
        nl2sqlLog.info("========== NL2SQL Request (Streaming) ==========");
        nl2sqlLog.info("User input: " + userInput);
        nl2sqlLog.info("Schema: " + schemaName + ", Model: " + modelName);
        EventLogger.log(EventType.NL2SQL_USER_INPUT,
            buildNl2sqlEventJson(session.getSessionId(), schemaName, userName, userHost, userInput));

        // 1. Resolve model config
        ModelConfigRecord modelConfig = resolveModelConfig(modelName);

        // 2. Get OpenAiApiProvider
        OpenAiApiProvider provider = (OpenAiApiProvider) AiApiProviderFactory.getProvider("openai");

        // 3. Build system prompt
        String systemPrompt = buildSystemPrompt(schemaName, userName, userHost, connectionContext);
        nl2sqlLog.debug("System prompt:\n" + systemPrompt);

        // 4. Build messages from session history + new user input
        JSONArray messages = buildMessages(session, systemPrompt, userInput);

        // 4.5 Compress context if token estimate exceeds threshold
        long[] usageTotals = {0, 0, 0}; // prompt, completion, total
        messages = compressContextIfNeeded(session, messages, modelConfig,
            listener, usageTotals);

        // 5. Build tools definition
        JSONArray tools = buildToolsDefinition();

        // 6. Agent loop with streaming
        int maxIterations = InstConfUtil.getInt(ConnectionParams.NL2SQL_MAX_ITERATIONS);
        List<JSONObject> newMessages = new ArrayList<>();
        JSONObject userMsg = new JSONObject();
        userMsg.put("role", "user");
        userMsg.put("content", userInput);
        newMessages.add(userMsg);

        int llmCalls = 0;
        int[] sqlCount = {0};
        int[] sqlIndexHolder = {0};
        String finalAnswer = null;

        for (int i = 0; i < maxIterations; i++) {
            if (isAlive != null && !isAlive.getAsBoolean()) {
                nl2sqlLog.info("NL2SQL: client disconnected, aborting agent loop at iteration " + i);
                finalAnswer = null;
                break;
            }

            int queryTimeoutMs = InstConfUtil.getInt(ConnectionParams.NL2SQL_QUERY_TIMEOUT_MS);
            long totalTimeoutMs = (long) queryTimeoutMs * maxIterations;
            if (System.currentTimeMillis() - startTime > totalTimeoutMs) {
                nl2sqlLog.info("NL2SQL: overall timeout exceeded (" + totalTimeoutMs
                    + "ms), aborting at iteration " + i);
                finalAnswer = "Agent 总执行时间超限（" + (totalTimeoutMs / 1000) + "秒），已中止。"
                    + "请尝试简化问题，或使用 CLEAR CONTEXT 重置对话后重试。";
                break;
            }

            // We don't know if this iteration produces THOUGHT (with tool_calls)
            // or ANSWER (final content) until streaming completes. Use THOUGHT for
            // onStreamBegin; the correct type is determined at onStreamEnd.
            final boolean[] streamBegan = {false};

            // TextChunker accumulates tokens and emits sentence-level chunks
            TextChunker chunker = new TextChunker(chunk -> {
                if (!streamBegan[0]) {
                    listener.onStreamBegin(AgentStep.StepType.THOUGHT);
                    streamBegan[0] = true;
                }
                listener.onStreamChunk(chunk);
            });

            StreamEventHandler handler = new StreamEventHandler() {
                @Override
                public void onContentDelta(String text) {
                    chunker.feed(text);
                }

                @Override
                public void onToolCallDelta(int index, String id, String functionName, String argsDelta) {
                    // Accumulated by provider, no action needed here
                }

                @Override
                public void onComplete(String finishReason) {
                    // noop - post-processing done below
                }

                @Override
                public void onError(Exception e) {
                    nl2sqlLog.warn("Streaming error in LLM call", e);
                }
            };

            long llmStart = System.currentTimeMillis();
            OpenAiApiProvider.ChatCompletionResult result;
            try {
                result = provider.chatCompletionWithToolsStreaming(
                    modelConfig, messages, tools, null, handler, queryTimeoutMs);
            } catch (Exception e) {
                nl2sqlLog.warn("LLM streaming call #" + (llmCalls + 1) + " failed: " + e.getMessage(), e);
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "NL2SQL LLM call failed: " + e.getMessage());
            }
            llmCalls++;
            long llmElapsed = System.currentTimeMillis() - llmStart;
            JSONObject assistantMsg = result.message;
            accumulateUsage(result.usage, usageTotals);
            nl2sqlLog.info("LLM streaming call #" + llmCalls + " took " + llmElapsed + "ms");

            if (isAlive != null && !isAlive.getAsBoolean()) {
                nl2sqlLog.info("NL2SQL: client disconnected after LLM call #" + llmCalls + ", aborting");
                finalAnswer = null;
                break;
            }

            messages.add(assistantMsg);
            newMessages.add(assistantMsg);

            // Check for tool_calls
            JSONArray toolCalls = assistantMsg.getJSONArray("tool_calls");
            if (toolCalls == null || toolCalls.isEmpty()) {
                // Final answer - content already streamed via chunker
                String remaining = chunker.flush();
                // onStreamChunk was already called by flush() callback if content remained
                if (streamBegan[0]) {
                    listener.onStreamEnd(AgentStep.StepType.ANSWER);
                }
                finalAnswer = assistantMsg.getString("content");
                nl2sqlLog.info("Final answer (streamed): " +
                    (finalAnswer != null ? finalAnswer.length() + " chars" : "null"));
                break;
            }

            // Intermediate response with tool_calls - flush any thought content
            String remaining = chunker.flush();
            if (streamBegan[0]) {
                listener.onStreamEnd(AgentStep.StepType.THOUGHT);
            }

            // Process each tool_call (same as non-streaming chat())
            for (int j = 0; j < toolCalls.size(); j++) {
                if (isAlive != null && !isAlive.getAsBoolean()) {
                    nl2sqlLog.info("NL2SQL: client disconnected before tool call #" + j + ", aborting");
                    finalAnswer = null;
                    break;
                }

                JSONObject toolCall = toolCalls.getJSONObject(j);
                String callId = toolCall.getString("id");
                JSONObject function = toolCall.getJSONObject("function");
                String funcName = function.getString("name");
                String argsJson = function.getString("arguments");

                nl2sqlLog.info("Tool call: " + funcName + ", args: " + argsJson);

                String toolResult;
                try {
                    toolResult = dispatchToolCall(funcName, argsJson, session, schemaName,
                        userName, userHost, connectionContext, sqlIndexHolder, sqlCount, listener::onStep,
                        schemaSwitcher);
                } catch (Exception e) {
                    toolResult = "Error: failed to parse tool arguments - " + e.getMessage();
                    nl2sqlLog.warn("Tool call parse error: " + funcName + ", args: " + argsJson, e);
                }

                // Build tool response message
                JSONObject toolMsg = new JSONObject();
                toolMsg.put("role", "tool");
                toolMsg.put("tool_call_id", callId);
                toolMsg.put("content", toolResult);
                messages.add(toolMsg);
                newMessages.add(toolMsg);
            }
            if (isAlive != null && !isAlive.getAsBoolean()) {
                break;
            }
        }

        // 7. Update session history
        session.addMessages(newMessages);

        long totalElapsed = System.currentTimeMillis() - startTime;
        nl2sqlLog.info("Total (streaming): " + totalElapsed + "ms, LLM calls: " + llmCalls);
        nl2sqlLog.info("====================================");
        logger.info("NL2SQL streaming total: " + totalElapsed + "ms, llmCalls=" + llmCalls);

        // 8. Build final result
        boolean fallbackAnswer = false;
        boolean cancelled = (isAlive != null && !isAlive.getAsBoolean());
        if (finalAnswer == null || finalAnswer.isEmpty()) {
            if (cancelled) {
                finalAnswer = "Query cancelled.";
            } else {
                finalAnswer = "Agent 达到最大执行轮次（" + maxIterations + "），未能给出最终答案。"
                    + "请尝试简化问题，或使用 CLEAR CONTEXT 重置对话后重试。";
            }
            fallbackAnswer = true;
            try {
                listener.onStreamBegin(AgentStep.StepType.ANSWER);
                listener.onStreamChunk(finalAnswer);
                listener.onStreamEnd(AgentStep.StepType.ANSWER);
            } catch (Exception e) {
                nl2sqlLog.info("NL2SQL: failed to send fallback answer: " + e.getMessage());
            }
        }
        EventLogger.log(EventType.NL2SQL_ANSWER,
            buildNl2sqlEventJson(session.getSessionId(), schemaName, userName, userHost,
                modelConfig.name, llmCalls, totalElapsed, usageTotals[0], usageTotals[1], usageTotals[2],
                sqlCount[0], fallbackAnswer, finalAnswer));
    }

    // ==================== Shared Tool Dispatch ====================

    /**
     * Dispatch a single tool call and return the result text to feed back to LLM.
     * Handles execute_sql, set_plan, and unknown tools.
     *
     * @param funcName tool function name
     * @param argsJson raw JSON arguments string from LLM
     * @param session agent session (holds transactional connection and effective schema)
     * @param defaultSchema default database from ServerConnection
     * @param userName connection user
     * @param userHost connection host
     * @param sqlIndexHolder mutable int[1] — incremented for each execute_sql call
     * @param sqlCount mutable int[1] — incremented for each execute_sql/query_sql call
     * @param stepEmitter callback for emitting AgentStep events (may be null)
     * @return tool result text
     */
    private static String dispatchToolCall(String funcName, String argsJson,
                                           AgentSession session,
                                           String defaultSchema, String userName, String userHost,
                                           AgentConnectionContext connectionContext,
                                           int[] sqlIndexHolder,
                                           int[] sqlCount,
                                           Consumer<AgentStep> stepEmitter,
                                           Consumer<String> schemaSwitcher) {
        // Resolve effective schema: session override > default from connection
        String schemaName = session.getEffectiveSchema() != null
            ? session.getEffectiveSchema() : defaultSchema;
        if (TOOL_NAME_QUERY.equals(funcName)) {
            // --- query_sql: read operations + transaction control + USE ---
            JSONObject args = JSON.parseObject(argsJson);
            String sql = args.getString("sql");
            String reason = args.getString("reason");

            // Handle USE statement: schema switch (no actual SQL execution)
            if (isUseStatement(sql)) {
                String targetSchema = extractSchemaFromUse(sql);
                if (targetSchema == null || targetSchema.isEmpty()) {
                    return "Error: USE 语句格式错误，正确格式: USE <database_name>";
                }
                if (hasTrailingStatementAfterUse(sql, targetSchema)) {
                    return "Error: USE 语句不能与其他语句合并发送。请先单独调用 USE "
                        + targetSchema + "，下一次工具调用再执行后续 SQL。";
                }
                session.setEffectiveSchema(targetSchema);
                // Sync the connection-level schema so subsequent native SQL
                // commands (entered directly by the user, not through the agent)
                // also see the new database. Without this, the user would hit
                // "No database selected" right after asking the agent to USE.
                if (schemaSwitcher != null) {
                    try {
                        schemaSwitcher.accept(targetSchema);
                    } catch (Exception e) {
                        nl2sqlLog.warn("Failed to sync connection schema to " + targetSchema, e);
                    }
                }
                nl2sqlLog.info("Schema switched to: " + targetSchema);
                return "OK, switched to database: " + targetSchema;
            }

            // Validate SQL type: only read queries and transaction control allowed
            if (!isQueryStatement(sql) && !isTransactionStatement(sql)) {
                return "Error: query_sql 只支持只读查询（SELECT/SHOW/DESCRIBE/EXPLAIN）、事务控制语句（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）和 USE 切库语句，数据修改请使用 execute_sql 工具。";
            }

            int idx = ++sqlIndexHolder[0];
            if (stepEmitter != null) {
                stepEmitter.accept(AgentStep.sqlAction(idx, sql, reason));
            }
            long sqlStart = System.currentTimeMillis();

            String intercepted = tryGetReferenceDirectly(sql);
            if (intercepted != null) {
                long refElapsed = System.currentTimeMillis() - sqlStart;
                String toolResult = sql.trim().toUpperCase().contains("AI_GET_SKILL_PROMPT")
                    ? intercepted
                    : "=== 参考文档内容（请严格按照以下文档中的工作流和建议回答）===\n" + intercepted
                    + "\n=== 参考文档结束 ===";
                if (stepEmitter != null) {
                    stepEmitter.accept(AgentStep.sqlResult(idx,
                        "(缓存直读, " + intercepted.length() + " 字符)",
                        1, refElapsed, true, null, null));
                }
                sqlCount[0]++;
                return toolResult;
            }

            // Transaction statement handling: manage connection lifecycle
            if (isTransactionStatement(sql)) {
                SqlExecResult txResult = handleTransactionStatement(
                    session, sql, schemaName, userName, userHost, connectionContext);
                long sqlElapsed = System.currentTimeMillis() - sqlStart;
                nl2sqlLog.info("TX [" + sqlElapsed + "ms]: " + sql
                    + (txResult.success ? "" : " (FAILED)"));
                if (stepEmitter != null) {
                    stepEmitter.accept(AgentStep.sqlResult(idx,
                        txResult.text, txResult.rowCount, sqlElapsed, txResult.success,
                        null, null));
                }
                sqlCount[0]++;
                return txResult.text;
            }

            // Regular query: if in active transaction, reuse shared connection
            SqlExecResult execResult;
            if (session.isInTransaction()) {
                execResult = executeOnSharedConnection(
                    session.getTransactionalConnection(), sql, true);
            } else {
                MasterSlave readRoute = InstConfUtil.getBool(ConnectionParams.NL2SQL_READ_USE_MASTER)
                    ? MasterSlave.MASTER_ONLY : MasterSlave.SLAVE_ONLY;
                execResult = executeSqlInternal(
                    sql, schemaName, userName, userHost, readRoute, connectionContext);
            }
            long sqlElapsed = System.currentTimeMillis() - sqlStart;
            nl2sqlLog.info("SQL [" + sqlElapsed + "ms]: " + sql
                + (execResult.success ? "" : " (FAILED)"));
            nl2sqlLog.info("SQL result (" + execResult.text.length() + " chars)");
            if (stepEmitter != null) {
                stepEmitter.accept(AgentStep.sqlResult(idx,
                    execResult.text, execResult.rowCount, sqlElapsed, execResult.success,
                    execResult.columns, execResult.rows));
            }
            sqlCount[0]++;
            return execResult.text;
        } else if (TOOL_NAME.equals(funcName)) {
            // --- execute_sql: write operations, route to master ---
            JSONObject args = JSON.parseObject(argsJson);
            String sql = args.getString("sql");
            String reason = args.getString("reason");

            if (connectionContext.isReadOnly()) {
                return "Error: current session is read-only; execute_sql is not allowed.";
            }

            // Validate SQL type: only write operations allowed
            if (isQueryStatement(sql)) {
                return "Error: execute_sql 仅用于写操作（INSERT/UPDATE/DELETE/REPLACE），读取数据请使用 query_sql 工具以避免影响主库。";
            }

            int idx = ++sqlIndexHolder[0];
            if (stepEmitter != null) {
                stepEmitter.accept(AgentStep.sqlAction(idx, sql, reason));
            }
            long sqlStart = System.currentTimeMillis();
            // If in active transaction, reuse shared connection
            SqlExecResult execResult;
            if (session.isInTransaction()) {
                execResult = executeOnSharedConnection(
                    session.getTransactionalConnection(), sql, false);
            } else {
                execResult = executeSqlInternal(sql, schemaName, userName, userHost,
                    MasterSlave.MASTER_ONLY, connectionContext);
            }
            long sqlElapsed = System.currentTimeMillis() - sqlStart;
            nl2sqlLog.info("SQL [" + sqlElapsed + "ms]: " + sql + " (master)");
            nl2sqlLog.info("SQL result (" + execResult.text.length() + " chars)");
            if (stepEmitter != null) {
                stepEmitter.accept(AgentStep.sqlResult(idx,
                    execResult.text, execResult.rowCount, sqlElapsed, execResult.success,
                    execResult.columns, execResult.rows));
            }
            sqlCount[0]++;
            return execResult.text;
        } else if (TOOL_NAME_PLAN.equals(funcName)) {
            List<String> stepList = new ArrayList<>();
            JSONArray steps = JSON.parseObject(argsJson).getJSONArray("steps");
            if (steps != null) {
                for (int k = 0; k < steps.size(); k++) {
                    stepList.add(steps.getString(k));
                }
            }
            if (stepEmitter != null) {
                stepEmitter.accept(AgentStep.plan(stepList));
            }
            StringBuilder planResult = new StringBuilder("执行计划已确认（" + stepList.size() + " 步）:\n");
            for (int k = 0; k < stepList.size(); k++) {
                planResult.append(k + 1).append(". ").append(stepList.get(k)).append("\n");
            }
            return planResult.append("\n请按此框架逐步执行。每步完成后对照计划推进，可根据实际结果灵活调整步骤。")
                .toString();
        } else {
            nl2sqlLog.warn("Unknown tool: " + funcName);
            return "Error: unknown tool '" + funcName + "'";
        }
    }

    private static ModelConfigRecord resolveModelConfig(String modelName) {
        ModelManager manager = ModelManager.getInstance();
        if (modelName == null || modelName.isEmpty()) {
            modelName = manager.getDefaultModelForFunction("AI_PROMPT");
        }
        if (modelName == null || modelName.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "No model configured for NL2SQL. Please set a default model via AI_UPDATE_FUNCTION('AI_PROMPT', '<model_name>'), or set NL2SQL_MODEL_NAME parameter.");
        }
        ModelConfigRecord config = manager.getModelConfig(modelName);
        if (config == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "NL2SQL model not found: " + modelName
                + ". Please register a model, set NL2SQL_MODEL_NAME, or use AI_UPDATE_FUNCTION('AI_PROMPT', '<model_name>').");
        }
        return config;
    }

    private static String buildNl2sqlEventJson(String sessionId, String schemaName, String userName,
                                               String userHost, String input) {
        JSONObject json = new JSONObject();
        json.put("sessionId", sessionId);
        json.put("schema", schemaName);
        json.put("user", userName);
        json.put("host", userHost);
        json.put("input", input);
        return json.toJSONString();
    }

    private static String buildNl2sqlEventJson(String sessionId, String schemaName, String userName,
                                               String userHost, String modelName, int llmCalls,
                                               long totalElapsedMs, long promptTokens,
                                               long completionTokens, long totalTokens,
                                               int sqlCount, boolean fallbackAnswer, String answer) {
        JSONObject json = new JSONObject();
        json.put("sessionId", sessionId);
        json.put("schema", schemaName);
        json.put("user", userName);
        json.put("host", userHost);
        json.put("model", modelName);
        json.put("llmCalls", llmCalls);
        json.put("elapsedMs", totalElapsedMs);
        json.put("promptTokens", promptTokens);
        json.put("completionTokens", completionTokens);
        json.put("totalTokens", totalTokens);
        json.put("sqlCount", sqlCount);
        json.put("fallbackAnswer", fallbackAnswer);
        json.put("answer", answer);
        return json.toJSONString();
    }

    private static void accumulateUsage(JSONObject usage, long[] usageTotals) {
        if (usage == null) {
            return;
        }
        usageTotals[0] += usage.getLongValue("prompt_tokens");
        usageTotals[1] += usage.getLongValue("completion_tokens");
        usageTotals[2] += usage.getLongValue("total_tokens");
    }

    // ==================== Context Compression ====================

    /**
     * Estimate the number of tokens in a text string.
     * Uses a heuristic: CJK characters ~= 2 tokens each (conservative),
     * ASCII characters ~= 1 token per 4 chars.
     */
    static int estimateTokens(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        int tokens = 0;
        int asciiRun = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c <= 0x7F) {
                asciiRun++;
            } else {
                if (asciiRun > 0) {
                    tokens += Math.max(1, (asciiRun + 3) / 4);
                    asciiRun = 0;
                }
                tokens += 2;
            }
        }
        if (asciiRun > 0) {
            tokens += Math.max(1, (asciiRun + 3) / 4);
        }
        return tokens;
    }

    /**
     * Estimate total tokens for a messages array (OpenAI format).
     * Each message has ~4 tokens overhead for role/separators.
     */
    static int estimateMessagesTokens(JSONArray messages) {
        int total = 0;
        for (int i = 0; i < messages.size(); i++) {
            JSONObject msg = messages.getJSONObject(i);
            total += 4; // per-message overhead
            String content = msg.getString("content");
            total += estimateTokens(content);
            JSONArray toolCalls = msg.getJSONArray("tool_calls");
            if (toolCalls != null) {
                for (int j = 0; j < toolCalls.size(); j++) {
                    JSONObject tc = toolCalls.getJSONObject(j);
                    JSONObject func = tc.getJSONObject("function");
                    if (func != null) {
                        total += estimateTokens(func.getString("name"));
                        total += estimateTokens(func.getString("arguments"));
                    }
                }
            }
        }
        total += 2; // priming tokens
        return total;
    }

    /**
     * Resolve the effective max context tokens.
     * Priority: model_params.context_length > NL2SQL_MAX_CONTEXT_TOKENS global param.
     */
    private static int resolveMaxContextTokens(ModelConfigRecord modelConfig) {
        JSONObject modelParams = AiParamUtils.parseModelParams(modelConfig);
        Integer contextLength = AiParamUtils.getIntegerParam("context_length", null, modelParams);
        if (contextLength != null && contextLength > 0) {
            return contextLength;
        }
        return InstConfUtil.getInt(ConnectionParams.NL2SQL_MAX_CONTEXT_TOKENS);
    }

    /**
     * Build the compression prompt that instructs the LLM to summarize conversation history.
     */
    private static String buildCompressionPrompt(JSONArray messagesToCompress) {
        StringBuilder sb = new StringBuilder();
        sb.append("请将以下多轮对话历史压缩为一段简洁的摘要。要求：\n");
        sb.append("1. 保留所有关键信息：用户问了什么问题、执行了哪些 SQL、得到了什么结果、最终结论\n");
        sb.append("2. 保留数据库名、表名、列名等具体标识符\n");
        sb.append("3. 保留执行过的 SQL 语句摘要（不需要完整结果集，但需要记录查询意图和关键发现）\n");
        sb.append("4. 保留错误信息和解决方案\n");
        sb.append("5. 不要包含对话格式（如 \"用户说:\" \"助手回复:\"），直接用叙述体写摘要\n");
        sb.append("6. 摘要长度控制在 500 字以内\n\n");
        sb.append("=== 对话历史 ===\n");

        for (int i = 0; i < messagesToCompress.size(); i++) {
            JSONObject msg = messagesToCompress.getJSONObject(i);
            String role = msg.getString("role");
            String content = msg.getString("content");
            if (content != null && !content.isEmpty()) {
                if (content.length() > 2000) {
                    content = content.substring(0, 2000) + "...(已截断)";
                }
                sb.append("[").append(role).append("] ").append(content).append("\n\n");
            }
        }
        sb.append("=== 对话历史结束 ===\n\n请输出摘要：");
        return sb.toString();
    }

    /**
     * Compress context if estimated tokens exceed the threshold.
     * Calls LLM to summarize old history, replaces session messages with summary,
     * and returns updated messages array for the current LLM call.
     *
     * @return the (potentially compressed) messages array to use for the LLM call
     */
    private static JSONArray compressContextIfNeeded(AgentSession session,
                                                     JSONArray messages,
                                                     ModelConfigRecord modelConfig,
                                                     StreamingStepListener streamListener,
                                                     long[] usageTotals) {
        int maxTokens = resolveMaxContextTokens(modelConfig);
        int threshold = (int) (maxTokens * 0.8);
        int estimatedTokens = estimateMessagesTokens(messages);

        nl2sqlLog.info("Context token estimate: " + estimatedTokens
            + ", threshold: " + threshold + ", max: " + maxTokens);

        if (estimatedTokens <= threshold) {
            return messages;
        }

        nl2sqlLog.info("Context compression triggered: " + estimatedTokens
            + " tokens exceeds threshold " + threshold);

        // Separate history into conversation messages vs tool messages.
        // Conversation messages: role=user or (role=assistant without tool_calls)
        // Tool messages: role=assistant with tool_calls, or role=tool
        // Strategy: compress ALL tool messages + old conversation turns;
        //           preserve only the last N conversation turns (user Q + final A).
        int preserveTurns = 2; // keep last 2 Q&A pairs
        int startIdx = 1; // skip system message
        int endIdx = messages.size() - 1; // skip current user input (last element)

        // Collect indices of conversation messages (non-tool messages in history)
        List<Integer> conversationIndices = new ArrayList<>();
        for (int i = startIdx; i < endIdx; i++) {
            if (isConversationMessage(messages.getJSONObject(i))) {
                conversationIndices.add(i);
            }
        }

        // Determine which conversation messages to preserve (last N*2: N users + N answers)
        int preserveCount = preserveTurns * 2;
        int preserveStart = Math.max(0, conversationIndices.size() - preserveCount);
        Set<Integer> preserveSet = new HashSet<>();
        for (int i = preserveStart; i < conversationIndices.size(); i++) {
            preserveSet.add(conversationIndices.get(i));
        }

        // Everything not in preserveSet is compressed
        JSONArray toCompress = new JSONArray();
        for (int i = startIdx; i < endIdx; i++) {
            if (!preserveSet.contains(i)) {
                toCompress.add(messages.getJSONObject(i));
            }
        }

        if (toCompress.isEmpty()) {
            nl2sqlLog.info("Context compression skipped: nothing to compress");
            return messages;
        }

        // Notify client
        if (streamListener != null) {
            streamListener.onStreamBegin(AgentStep.StepType.CONTEXT_COMPRESS);
            streamListener.onStreamChunk("正在压缩对话上下文...");
            streamListener.onStreamEnd(AgentStep.StepType.CONTEXT_COMPRESS);
        }

        // Call LLM to generate summary
        String compressionPrompt = buildCompressionPrompt(toCompress);
        String summary;
        try {
            OpenAiApiProvider provider = (OpenAiApiProvider) AiApiProviderFactory.getProvider("openai");

            JSONArray compressMessages = new JSONArray();
            JSONObject compressSystemMsg = new JSONObject();
            compressSystemMsg.put("role", "system");
            compressSystemMsg.put("content", "你是一个对话摘要助手。请严格按照用户要求压缩对话历史。");
            compressMessages.add(compressSystemMsg);

            JSONObject compressUserMsg = new JSONObject();
            compressUserMsg.put("role", "user");
            compressUserMsg.put("content", compressionPrompt);
            compressMessages.add(compressUserMsg);

            OpenAiApiProvider.ChatCompletionResult result = provider.chatCompletionWithTools(
                modelConfig, compressMessages, null, null);
            summary = result.message.getString("content");
            accumulateUsage(result.usage, usageTotals);

            if (summary == null || summary.isEmpty()) {
                return messages;
            }
        } catch (Exception e) {
            return messages;
        }

        // Collect preserved conversation messages
        List<JSONObject> preservedMsgs = new ArrayList<>();
        for (int idx : preserveSet) {
            preservedMsgs.add(messages.getJSONObject(idx));
        }

        // Update session: replace history with summary + preserved conversation messages
        session.replaceHistoryWithSummaryAndConversation(summary, preservedMsgs);

        // Rebuild messages from updated session + current user input
        JSONObject newUserMsg = messages.getJSONObject(messages.size() - 1);
        JSONArray compressedMessages = session.getMessages();
        compressedMessages.add(newUserMsg);

        int compressedTokens = estimateMessagesTokens(compressedMessages);
        nl2sqlLog.info("Context compressed: " + estimatedTokens + " -> " + compressedTokens + " tokens");

        EventLogger.log(EventType.NL2SQL_CONTEXT_COMPRESS,
            buildCompressionEventJson(session.getSessionId(), estimatedTokens, compressedTokens,
                toCompress.size(), summary.length()));

        return compressedMessages;
    }

    /**
     * Check if a message is a "conversation message" (user question or final assistant answer).
     * Tool-related messages (assistant with tool_calls, or role=tool) return false.
     */
    static boolean isConversationMessage(JSONObject msg) {
        String role = msg.getString("role");
        if ("user".equals(role)) {
            return true;
        }
        if ("assistant".equals(role)) {
            // Assistant message with tool_calls is NOT a conversation message
            return msg.getJSONArray("tool_calls") == null || msg.getJSONArray("tool_calls").isEmpty();
        }
        // role=tool or anything else is not a conversation message
        return false;
    }

    private static String buildCompressionEventJson(String sessionId, int originalTokens,
                                                    int compressedTokens, int messagesCompressed,
                                                    int summaryLength) {
        JSONObject json = new JSONObject();
        json.put("sessionId", sessionId);
        json.put("originalTokens", originalTokens);
        json.put("compressedTokens", compressedTokens);
        json.put("messagesCompressed", messagesCompressed);
        json.put("summaryLength", summaryLength);
        json.put("compressionRatio",
            String.format("%.1f%%", (1.0 - (double) compressedTokens / originalTokens) * 100));
        return json.toJSONString();
    }

    private static JSONArray buildMessages(AgentSession session, String systemPrompt, String userInput) {
        // Ensure system message is stored and up-to-date in session
        JSONObject systemMsg = new JSONObject();
        systemMsg.put("role", "system");
        systemMsg.put("content", systemPrompt);
        session.ensureSystemMessage(systemMsg);

        // Get a copy of session history (includes system message)
        JSONArray messages = session.getMessages();

        // Append user message to the copy (for LLM call)
        JSONObject userMsg = new JSONObject();
        userMsg.put("role", "user");
        userMsg.put("content", userInput);
        messages.add(userMsg);

        return messages;
    }

    private static String buildSystemPrompt(String schemaName, String userName, String userHost,
                                            AgentConnectionContext connectionContext) {
        StringBuilder sb = new StringBuilder();
        sb.append("你是 PolarDB-X 数据库助手。你可以通过 query_sql 和 execute_sql 工具执行 SQL 来回答用户的问题。\n\n");
        sb.append("当前数据库: ").append(schemaName).append("\n");
        sb.append("当前用户: ").append(userName).append("@").append(userHost).append("\n\n");

        // Inject user's GRANT privileges as soft constraint
        try {
            PolarAccountInfo polarUserInfo = PolarPrivManager.getInstance().getExactUser(userName, userHost);
            if (polarUserInfo != null) {
                List<String> grants = PolarPrivManager.getInstance().showGrants(
                    polarUserInfo, connectionContext.getActiveRoles(),
                    polarUserInfo.getAccount(), Collections.emptyList());
                if (grants != null && !grants.isEmpty()) {
                    sb.append("当前用户权限:\n");
                    grants.forEach(grant -> sb.append("  ").append(grant).append("\n"));
                    sb.append("\n");
                }
            }
        } catch (Exception e) {
            nl2sqlLog.warn("Failed to get user grants for system prompt: " + userName + "@" + userHost, e);
        }

        // === Capability boundary: tell the agent what it can and cannot do ===
        sb.append("=== 能力边界 ===\n");
        sb.append("你当前可执行的操作类型：\n");
        sb.append("- 只读查询: SELECT, SHOW, DESCRIBE, EXPLAIN\n");
        sb.append(
            "- 向量相似度查询: 使用 VEC_DISTANCE/VEC_DISTANCE_COSINE/VEC_DISTANCE_EUCLIDEAN 对 VECTOR 列进行 ANN 搜索\n");
        sb.append("- 事务控制: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, START TRANSACTION\n");
        sb.append("- 切换数据库: USE <database_name>\n");
        sb.append("- 诊断命令: SHOW SLOW, SHOW TRACE, EXPLAIN COST, SHOW TOPOLOGY, SHOW RULE 等\n");

        String policy = InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES);
        boolean isAll = policy != null && "ALL".equalsIgnoreCase(policy.trim());
        boolean isReadWrite = policy != null && "READ_WRITE".equalsIgnoreCase(policy.trim());
        if (isAll || isReadWrite) {
            sb.append("- 数据修改: INSERT, UPDATE, DELETE, REPLACE（当前已开启）\n");
        } else {
            sb.append("- 数据修改: INSERT/UPDATE/DELETE 当前不可用（安全策略为只读模式）\n");
        }
        if (isAll) {
            sb.append("- DDL: CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX 等（当前已开启）\n");
        }

        sb.append("\n以下操作不可执行（安全约束）：\n");
        if (!isAll) {
            sb.append("- DDL: CREATE TABLE, ALTER TABLE, DROP TABLE 等\n");
        }
        sb.append("- 权限管理: GRANT, REVOKE, CREATE USER 等\n");
        sb.append("- 运维命令: RELOAD, FLUSH, KILL, RESIZE 等\n");
        sb.append("\n用户可通过 HELP DRDS 命令查看 CN 支持的完整命令列表。\n");
        sb.append("=== 能力边界结束 ===\n");
        sb.append("\n=== PolarDB-X 诊断视图速查 ===\n");
        sb.append("以下 information_schema 视图为 PolarDB-X 独有，是运维诊断核心入口：\n");
        sb.append("- SHOW FULL DATABASES: 查看所有数据库及其模式类型（AUTO 或 DRDS）\n");
        sb.append("- SHOW CREATE DATABASE <db_name>: 查看指定数据库的创建语句及模式类型（AUTO/DRDS）\n");
        sb.append("- STATEMENTS_SUMMARY: SQL 摘要统计（按 SUM_RESPONSE_TIME_MS 排序）\n");
        sb.append("- POLARDBX_TRX: 分布式事务列表（按 DURATION_TIME 排序）\n");
        sb.append("- DEADLOCKS: 最近发生的死锁记录（按 GMT_CREATED 排序）\n");
        sb.append("- DDL_PROGRESS: DDL 执行进度百分比\n");
        sb.append("- STORAGE: DN 存储节点状态（主从角色、连接数、健康检查）\n");
        sb.append("- TABLE_DETAIL: 表分区数据分布（按 TABLE_ROWS 排序）\n");
        sb.append("更完整的视图列表、示例 SQL 和典型诊断流程，请先调用 AI_GET_SKILL_PROMPT('ops-diagnostics') 获取。\n");
        sb.append("=== 诊断视图速查结束 ===\n");
        sb.append("\n=== 事务支持说明 ===\n");
        sb.append("事务跨多次工具调用共享同一连接。工作流程：\n");
        sb.append("1. 调用 query_sql 执行 BEGIN\n");
        sb.append("2. 后续的 query_sql / execute_sql 调用自动在同一事务内执行\n");
        sb.append("3. 调用 query_sql 执行 COMMIT 或 ROLLBACK 结束事务\n");
        sb.append("注意：事务连接在当前会话内持续有效，直到 COMMIT/ROLLBACK 或 CLEAR CONTEXT。\n");
        sb.append("=== 事务支持说明结束 ===\n\n");

        sb.append("规则:\n");
        sb.append(
            "1. 读取数据（查表、查看表结构、SHOW/DESCRIBE/EXPLAIN 等）、事务控制（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）和切库（USE）使用 query_sql 工具；修改数据（INSERT/UPDATE/DELETE/REPLACE）使用 execute_sql 工具。"
                + "如果当前安全策略为 ALL，DDL（CREATE TABLE/ALTER TABLE/DROP TABLE/CREATE INDEX 等）也使用 execute_sql 工具。"
                + "不要用 execute_sql 执行查询操作，也不要用 query_sql 执行写操作或 DDL。\n");
        sb.append(
            "2. 【库类型识别】PolarDB-X 数据库分为 AUTO 模式库和 DRDS 模式库，建表语法、分区策略、索引行为差异很大。"
                + "在回答建表、分区设计、GSI、CCI 等问题前，必须先通过 `SHOW CREATE DATABASE <db_name>` 或 `SHOW FULL DATABASES` 确认当前数据库类型。"
                + "如果用户要求创建新库，默认使用 `CREATE DATABASE <name> mode = auto`，禁止创建无 mode 参数的 DRDS 库。\n");
        sb.append("3. 如果 SQL 执行报错，尝试修正并重试\n");
        sb.append("4. 将查询结果直接整理成文本回答给用户，不要输出 SQL 代码块\n");
        sb.append("5. 回答时使用中文，尽量简洁\n");
        sb.append(
                "6. 【格式要求 - 严格遵守】输出纯文本，绝对禁止任何 markdown 语法，也禁止使用 ||| 作为列分隔符。具体规则：\n")
            .append("   - 禁止使用 **加粗**、*斜体*、__下划线__\n")
            .append("   - 禁止使用 # 标题标记\n")
            .append("   - 禁止使用 ``` 代码围栏，SQL 直接写出即可\n")
            .append("   - 禁止使用 `反引号` 包裹代码（SQL 中的标识符反引号除外）\n")
            .append("   - 禁止使用 - 或 * 开头的无序列表，改用 1. 2. 3. 有序编号\n")
            .append("   - 编号标题后必须紧跟内容，不要空行；整个编号块结束后再加一个空行与下一个块分隔\n")
            .append("   - 用空行分隔段落，用缩进表示层级\n")
            .append("   - 允许使用 utf8mb4 支持的表情符号（如 ✅ ❌ ⚠️ 💡）增强可读性，但不要过度使用\n")
            .append("   结果在 MySQL 终端以单列 Answer 表格显示，不支持任何富文本渲染。\n");
        sb.append("7. 【重要】回答 PolarDB-X 相关问题时，如果问题与某个专业技能相关，"
            + "必须先通过 AI_GET_SKILL_PROMPT 获取该技能的完整指令，再按指令中要求获取参考文档（AI_GET_REFERENCE）。"
            + "你的通用数据库知识可能与 PolarDB-X 的实际行为不同，技能指令和参考文档包含 PolarDB-X 特有的功能和最佳实践。"
            + "不要跳过这一步直接基于通用知识回答。\n");
        sb.append("8. 获取技能指令和参考文档后，必须严格按照其中描述的工作流、步骤和建议来回答。")
            .append(
                "如果文档推荐了特定的命令或流程（如 EXPLAIN ONLINE_DDL、ALGORITHM=OMC 等），你必须在回答中包含这些内容。")
            .append("不要用通用知识覆盖或忽略技能指令和参考文档的具体指导。\n");
        sb.append("9. 你执行的 SQL 受当前用户权限约束。如果用户没有某个库/表的操作权限，SQL 会被拒绝执行。")
            .append("请根据上面列出的用户权限，避免执行超出权限的操作，并在必要时告知用户权限不足。\n");
        sb.append("10. 对于需要多步操作才能回答的复杂问题（如分析表设计、排查慢SQL、对比方案等），"
            + "先调用 set_plan 工具声明你的思路框架（每步用一句话描述意图，如\"查看表结构\"、\"分析分区信息\"）。"
            + "后续按计划推进，但可根据中间结果灵活调整（跳过不需要的步骤、增加新步骤、改变查询方向）。"
            + "简单问题（1-2步即可完成）无需调用 set_plan。\n");
        sb.append("11. 向量搜索决策：当用户问题涉及语义相似、模糊匹配、推荐、找相近内容等意图，"
            + "且相关表存在 VECTOR 列时，应优先使用向量相似度查询（VEC_DISTANCE/VEC_DISTANCE_COSINE/VEC_DISTANCE_EUCLIDEAN），"
            + "而不是传统 LIKE 或等值查询。复杂场景可混合使用：先用向量搜索召回 Top-K，再用传统 SQL 过滤。"
            + "如果当前数据库的向量列元数据已列出，请直接参考；如果未列出或不确定，先用 DESCRIBE <table> 确认列类型。\n");

        // Inject current Agent runtime parameters so the LLM is aware of operational limits
        sb.append("\n=== Agent 运行参数 ===\n");
        sb.append("- 最大迭代轮次: ").append(InstConfUtil.getInt(ConnectionParams.NL2SQL_MAX_ITERATIONS)).append("\n");
        sb.append("- 单 SQL 超时(ms): ").append(InstConfUtil.getInt(ConnectionParams.NL2SQL_QUERY_TIMEOUT_MS))
            .append("\n");
        sb.append("- 最大扫描行数: ").append(InstConfUtil.getInt(ConnectionParams.NL2SQL_MAX_SCAN_ROWS)).append("\n");
        sb.append("- 中间步骤展示: ").append(InstConfUtil.getBool(ConnectionParams.NL2SQL_SHOW_STEPS) ? "开启" : "关闭")
            .append("\n");
        sb.append("=== Agent 运行参数结束 ===\n");

        // Inject vector column metadata for the current schema
        try {
            List<String> vectorColumns = fetchVectorColumns(schemaName, userName, userHost, connectionContext);
            if (!vectorColumns.isEmpty()) {
                sb.append("\n=== 向量列元数据 ===\n当前数据库包含以下 VECTOR 列，可用于语义相似度搜索（ANN）：\n");
                vectorColumns.forEach(col -> sb.append("- ").append(col).append("\n"));
                sb.append(
                    "\n对上述列使用 VEC_DISTANCE/VEC_DISTANCE_COSINE/VEC_DISTANCE_EUCLIDEAN 进行相似度查询时，查询向量通常用 VEC_FROMTEXT('[...]') 包裹。\n=== 向量列元数据结束 ===\n");
            }
        } catch (Exception e) {
            nl2sqlLog.warn("Failed to load vector column metadata for schema: " + schemaName, e);
        }

        // Inject active skills — progressive loading: only name + description in system prompt.
        // Full prompt is loaded on demand via AI_GET_SKILL_PROMPT when the agent needs it.
        try {
            List<SkillConfigRecord> skills = SkillManager.getInstance().getActiveSkills();
            if (!skills.isEmpty()) {
                sb.append("\n=== 专业技能 ===\n");
                sb.append("以下是可用的专业技能。当用户要求列出、查看当前可用技能列表时，"
                    + "必须通过 SELECT AI_LIST_SKILLS() 获取完整列表，禁止查询 information_schema.AI_FUNCTIONS "
                    + "等不存在的系统表。当用户的问题与某个技能相关时，"
                    + "必须先通过 SELECT AI_GET_SKILL_PROMPT('技能名') 获取该技能的完整指令，"
                    + "然后严格按照指令执行。不要跳过这一步。\n");
                for (SkillConfigRecord skill : skills) {
                    sb.append("- ").append(skill.name);
                    if (skill.description != null && !skill.description.isEmpty()) {
                        sb.append(": ").append(skill.description);
                    }
                    sb.append("\n");
                }
            }
        } catch (Exception e) {
            nl2sqlLog.warn("Failed to load skills for system prompt", e);
        }

        return sb.toString();
    }

    /**
     * Fetch VECTOR column metadata for the given schema from information_schema.columns.
     * Returns a list of "table_name.column_name (column_type)" strings.
     */
    private static List<String> fetchVectorColumns(String schemaName, String userName, String userHost,
                                                   AgentConnectionContext connectionContext) {
        List<String> result = new ArrayList<>();
        if (schemaName == null || schemaName.isEmpty()) {
            return result;
        }

        String sql = "SELECT table_name, column_name, column_type FROM information_schema.columns "
            + "WHERE table_schema = ? AND data_type = 'VECTOR' "
            + "ORDER BY table_name, column_name";

        InnerConnection conn = null;
        try {
            conn = createAgentConnection(
                schemaName, userName, userHost, MasterSlave.MASTER_ONLY, connectionContext);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(rs.getString("table_name") + "." + rs.getString("column_name")
                            + " (" + rs.getString("column_type") + ")");
                    }
                }
            }
        } catch (Exception e) {
            nl2sqlLog.warn("Failed to fetch vector columns for schema: " + schemaName, e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return result;
    }

    private static JSONArray buildToolsDefinition() {
        JSONArray tools = new JSONArray();

        // --- query_sql tool (always available) ---
        JSONObject queryTool = new JSONObject();
        queryTool.put("type", "function");

        JSONObject queryFunction = new JSONObject();
        queryFunction.put("name", TOOL_NAME_QUERY);
        queryFunction.put("description",
            "在当前数据库执行只读查询（SELECT、SHOW、DESCRIBE、EXPLAIN）"
                + "、事务控制语句（BEGIN、COMMIT、ROLLBACK、SAVEPOINT、START TRANSACTION）"
                + "或切换数据库（USE）。"
                + "同时支持向量相似度查询，例如使用 VEC_DISTANCE/VEC_DISTANCE_COSINE/VEC_DISTANCE_EUCLIDEAN 对 VECTOR 列进行 ANN 搜索。"
                + "查询会路由到低延迟备库执行，事务语句在主库执行。"
                + "BEGIN 后的后续 SQL 将在同一事务连接上执行，直到 COMMIT/ROLLBACK。"
                + "所有数据检索、查看操作、向量搜索、事务控制和切库必须使用此工具。"
                + "返回查询结果文本。");

        JSONObject queryParams = new JSONObject();
        queryParams.put("type", "object");
        JSONObject queryProperties = new JSONObject();
        JSONObject querySqlProp = new JSONObject();
        querySqlProp.put("type", "string");
        querySqlProp.put("description",
            "The read-only SQL, transaction control, or USE statement to execute (SELECT/SHOW/DESCRIBE/EXPLAIN/BEGIN/COMMIT/ROLLBACK/SAVEPOINT/USE)");
        queryProperties.put("sql", querySqlProp);
        JSONObject queryReasonProp = new JSONObject();
        queryReasonProp.put("type", "string");
        queryReasonProp.put("description", "Brief reason/intent for executing this SQL (shown to user)");
        queryProperties.put("reason", queryReasonProp);
        queryParams.put("properties", queryProperties);
        JSONArray queryRequired = new JSONArray();
        queryRequired.add("sql");
        queryRequired.add("reason");
        queryParams.put("required", queryRequired);
        queryFunction.put("parameters", queryParams);

        queryTool.put("function", queryFunction);
        tools.add(queryTool);

        // --- execute_sql tool (only in READ_WRITE or ALL mode) ---
        String policy = InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES);
        if (policy == null || policy.isEmpty()) {
            policy = "READ_ONLY";
        }
        policy = policy.trim().toUpperCase();

        boolean policyIsAll = "ALL".equals(policy);
        if ("READ_WRITE".equals(policy) || policyIsAll) {
            JSONObject execTool = new JSONObject();
            execTool.put("type", "function");

            JSONObject execFunction = new JSONObject();
            execFunction.put("name", TOOL_NAME);
            if (policyIsAll) {
                execFunction.put("description",
                    "在当前数据库执行写入操作（INSERT、UPDATE、DELETE、REPLACE）"
                        + "或 DDL（CREATE TABLE、ALTER TABLE、DROP TABLE、CREATE INDEX 等），"
                        + "语句在主库执行。用于数据修改和 schema 变更操作。"
                        + "读取数据请使用 query_sql 工具。"
                        + "返回执行结果。");
            } else {
                execFunction.put("description",
                    "在当前数据库执行写入操作（INSERT、UPDATE、DELETE、REPLACE），"
                        + "语句在主库执行。仅用于数据修改操作。"
                        + "读取数据请使用 query_sql 工具。"
                        + "返回执行结果。");
            }

            JSONObject execParams = new JSONObject();
            execParams.put("type", "object");
            JSONObject execProperties = new JSONObject();
            JSONObject execSqlProp = new JSONObject();
            execSqlProp.put("type", "string");
            if (policyIsAll) {
                execSqlProp.put("description",
                    "The write SQL statement to execute (INSERT/UPDATE/DELETE/REPLACE) or DDL (CREATE/ALTER/DROP/CREATE INDEX)");
            } else {
                execSqlProp.put("description", "The write SQL statement to execute (INSERT/UPDATE/DELETE/REPLACE)");
            }
            execProperties.put("sql", execSqlProp);
            JSONObject execReasonProp = new JSONObject();
            execReasonProp.put("type", "string");
            execReasonProp.put("description", "Brief reason/intent for executing this SQL (shown to user)");
            execProperties.put("reason", execReasonProp);
            execParams.put("properties", execProperties);
            JSONArray execRequired = new JSONArray();
            execRequired.add("sql");
            execRequired.add("reason");
            execParams.put("required", execRequired);
            execFunction.put("parameters", execParams);

            execTool.put("function", execFunction);
            tools.add(execTool);
        }

        // --- set_plan tool (always available) ---
        JSONObject planTool = new JSONObject();
        planTool.put("type", "function");

        JSONObject planFunction = new JSONObject();
        planFunction.put("name", TOOL_NAME_PLAN);
        planFunction.put("description",
            "Declare the execution plan before starting a multi-step task. "
                + "Call this first to show the user your planned approach. "
                + "Only use for complex questions requiring 2+ SQL executions.");

        JSONObject planParams = new JSONObject();
        planParams.put("type", "object");
        JSONObject planProperties = new JSONObject();
        JSONObject stepsProp = new JSONObject();
        stepsProp.put("type", "array");
        JSONObject stepsItems = new JSONObject();
        stepsItems.put("type", "string");
        stepsProp.put("items", stepsItems);
        stepsProp.put("description", "List of step descriptions in execution order");
        planProperties.put("steps", stepsProp);
        planParams.put("properties", planProperties);
        JSONArray planRequired = new JSONArray();
        planRequired.add("steps");
        planParams.put("required", planRequired);
        planFunction.put("parameters", planParams);

        planTool.put("function", planFunction);
        tools.add(planTool);

        return tools;
    }

    /**
     * Pattern to detect SHOW DATABASES / SHOW FULL DATABASES.
     * These commands must be intercepted because InnerConnection routes them
     * through the optimizer as PhyShow, which pushes them down to the physical DN
     * and returns physical database names instead of logical ones.
     */
    private static final Pattern SHOW_DATABASES_PATTERN =
        Pattern.compile("^\\s*SHOW\\s+(FULL\\s+)?DATABASES\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * Intercept SHOW DATABASES / SHOW FULL DATABASES and return the correct
     * logical database list from CobarServer config, bypassing InnerConnection
     * which would push the command to a physical DN via PhyShow.
     *
     * @return SqlExecResult with logical databases, or null if sql is not SHOW DATABASES
     */
    private static SqlExecResult tryHandleShowDatabases(String sql) {
        if (!SHOW_DATABASES_PATTERN.matcher(sql).matches()) {
            return null;
        }

        try {
            Map<String, SchemaConfig> schemaConfigMap =
                CobarServer.getInstance().getConfig().getSchemas();
            TreeSet<String> schemas = new TreeSet<>();
            if (schemaConfigMap != null) {
                schemas.addAll(schemaConfigMap.keySet());
            }
            // Remove internal system databases (same as ShowDatabases.java)
            schemas.remove(SystemDbHelper.DEFAULT_DB_NAME);
            schemas.remove(SystemDbHelper.CDC_DB_NAME);

            // Format as tabular result: "Database\n<db1>\n<db2>\n..."
            StringBuilder sb = new StringBuilder();
            sb.append("Database\n");
            for (String name : schemas) {
                sb.append(name).append("\n");
            }
            return new SqlExecResult(sb.toString().trim(), schemas.size(), true);
        } catch (Exception e) {
            return null; // fall through to normal execution
        }
    }

    private static InnerConnection createAgentConnection(String schemaName, String userName, String userHost,
                                                         MasterSlave masterSlave,
                                                         AgentConnectionContext connectionContext)
        throws SQLException {
        return new InnerConnection(schemaName, userName, userHost, masterSlave,
            connectionContext.getSessionVariables(), connectionContext.getActiveRoles(),
            connectionContext.getTransactionIsolation(), connectionContext.isReadOnly());
    }

    /**
     * Execute SQL internally using InnerConnection with caller's privileges.
     * Includes EXPLAIN pre-check (for SELECT) and query timeout watchdog.
     *
     * @param masterSlave routing preference: MASTER_ONLY for writes, LOW_DELAY_SLAVE_ONLY for reads
     */
    private static SqlExecResult executeSqlInternal(String sql, String schemaName,
                                                    String userName, String userHost,
                                                    MasterSlave masterSlave,
                                                    AgentConnectionContext connectionContext) {
        // Intercept SHOW DATABASES / SHOW FULL DATABASES: InnerConnection would push
        // this to the physical DN via PhyShow, returning physical database names.
        // We handle it here to return the correct logical database list.
        SqlExecResult showDbResult = tryHandleShowDatabases(sql);
        if (showDbResult != null) {
            return showDbResult;
        }

        // SQL type check
        String violation = checkSqlTypeAllowed(sql);
        if (violation != null) {
            return new SqlExecResult("Blocked: " + violation, -1, false);
        }

        if (isSelectStatement(sql)) {
            String rejection = checkExplainCost(
                sql, schemaName, userName, userHost, masterSlave, connectionContext);
            if (rejection != null) {
                return new SqlExecResult(rejection, -1, false);
            }
        }

        if (isDmlStatement(sql)) {
            String rejection = checkExplainCost(
                sql, schemaName, userName, userHost, masterSlave, connectionContext);
            if (rejection != null) {
                return new SqlExecResult(rejection, -1, false);
            }
        }

        int timeoutMs = InstConfUtil.getInt(ConnectionParams.NL2SQL_QUERY_TIMEOUT_MS);
        final AtomicBoolean timedOut = new AtomicBoolean(false);
        InnerConnection conn = null;
        ScheduledFuture<?> timeoutFuture = null;

        try {
            conn = createAgentConnection(schemaName, userName, userHost, masterSlave, connectionContext);
            final InnerConnection connRef = conn;

            timeoutFuture = CobarServer.getInstance().getTimerTaskExecutor().schedule(() -> {
                timedOut.set(true);
                try {
                    connRef.close();
                } catch (Exception e) {
                    nl2sqlLog.debug("Error closing connection on timeout", e);
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);

            try (Statement stmt = conn.createStatement()) {
                if (isQueryStatement(sql)) {
                    ResultSet rs = stmt.executeQuery(sql);
                    if (rs == null) {
                        return new SqlExecResult("(empty result set, 0 rows)", 0, true);
                    }
                    return resultSetToResult(rs, MAX_RESULT_ROWS);
                } else {
                    int affected = stmt.executeUpdate(sql);
                    return new SqlExecResult("OK, " + affected + " rows affected.", affected, true);
                }
            }
        } catch (NullPointerException npe) {
            if (timedOut.get()) {
                nl2sqlLog.info("NL2SQL timeout (NPE): sql=" + sql + ", timeoutMs=" + timeoutMs);
                return new SqlExecResult(
                    "Query timeout: execution exceeded " + timeoutMs
                        + "ms. The query is too expensive. "
                        + "Please add more specific WHERE conditions or LIMIT to narrow the scope.",
                    -1, false);
            }
            nl2sqlLog.warn("NPE executing SQL (treating as empty result): " + sql, npe);
            return new SqlExecResult("(empty result set, 0 rows)", 0, true);
        } catch (Exception e) {
            if (timedOut.get()) {
                nl2sqlLog.info("NL2SQL timeout: sql=" + sql + ", timeoutMs=" + timeoutMs);
                return new SqlExecResult(
                    "Query timeout: execution exceeded " + timeoutMs
                        + "ms. The query is too expensive. "
                        + "Please add more specific WHERE conditions or LIMIT to narrow the scope.",
                    -1, false);
            }
            String msg = e.getMessage();
            if (msg == null) {
                msg = e.getClass().getSimpleName();
            }
            if (msg.contains("all slave is delay") || msg.contains("can't continue use slave")) {
                return new SqlExecResult(
                    "当前没有可用的低延迟备库（所有备库复制延迟较大）。"
                        + "请稍后重试，或建议用户使用 /*+TDDL:master()*/ 提示直接在主库查询。",
                    -1, false);
            }
            return new SqlExecResult("SQL Error: " + msg, -1, false);
        } finally {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
            if (conn != null && !timedOut.get()) {
                try {
                    conn.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Check if a SELECT query will scan too many rows by running EXPLAIN COST.
     * Returns rejection message if scan rows exceed threshold, null otherwise.
     * Fail-open: returns null on any EXPLAIN failure.
     */
    private static String checkExplainCost(String sql, String schemaName,
                                           String userName, String userHost,
                                           MasterSlave masterSlave,
                                           AgentConnectionContext connectionContext) {
        int maxScanRows = InstConfUtil.getInt(ConnectionParams.NL2SQL_MAX_SCAN_ROWS);
        if (maxScanRows <= 0) {
            return null;
        }

        final AtomicBoolean explainTimedOut = new AtomicBoolean(false);
        InnerConnection explainConn = null;
        ScheduledFuture<?> explainTimeout = null;

        try {
            explainConn = createAgentConnection(
                schemaName, userName, userHost, masterSlave, connectionContext);
            final InnerConnection connRef = explainConn;

            explainTimeout = CobarServer.getInstance().getTimerTaskExecutor().schedule(() -> {
                explainTimedOut.set(true);
                try {
                    connRef.close();
                } catch (Exception e) {
                    // ignore
                }
            }, EXPLAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            try (Statement stmt = explainConn.createStatement()) {
                ResultSet rs = stmt.executeQuery("EXPLAIN COST " + sql);
                double maxRowCount = parseMaxRowCount(rs);
                rs.close();

                nl2sqlLog.info("NL2SQL EXPLAIN pre-check: sql=" + sql
                    + ", maxRowCount=" + (long) maxRowCount + ", threshold=" + maxScanRows);

                if (maxRowCount > maxScanRows) {
                    return "Query rejected by safety check: estimated scan of " + (long) maxRowCount
                        + " rows exceeds the " + maxScanRows + " row limit. "
                        + "Please add WHERE conditions or LIMIT clause to reduce the scan scope.";
                }
            }
            return null;
        } catch (Exception e) {
            if (explainTimedOut.get()) {
                nl2sqlLog.info("NL2SQL EXPLAIN pre-check timed out (proceeding): sql=" + sql);
            } else {
                nl2sqlLog.info("NL2SQL EXPLAIN pre-check failed (proceeding): sql=" + sql
                    + ", error=" + e.getMessage());
            }
            return null;
        } finally {
            if (explainTimeout != null) {
                explainTimeout.cancel(false);
            }
            if (explainConn != null && !explainTimedOut.get()) {
                try {
                    explainConn.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Parse EXPLAIN COST output to find maximum scan row estimate.
     * Checks both logical rowcount (rowcount = N) and physical scan rows (rows:N in physicalPlan).
     */
    private static double parseMaxRowCount(ResultSet rs) throws Exception {
        double maxRowCount = 0;
        while (rs.next()) {
            String line = rs.getString(1);
            if (line == null) {
                continue;
            }
            Matcher matcher = ROWCOUNT_PATTERN.matcher(line);
            while (matcher.find()) {
                try {
                    double rowCount = Double.parseDouble(matcher.group(1));
                    if (rowCount > maxRowCount) {
                        maxRowCount = rowCount;
                    }
                } catch (NumberFormatException e) {
                    // skip
                }
            }
            Matcher physMatcher = PHYSICAL_ROWS_PATTERN.matcher(line);
            while (physMatcher.find()) {
                try {
                    double rowCount = Double.parseDouble(physMatcher.group(1));
                    if (rowCount > maxRowCount) {
                        maxRowCount = rowCount;
                    }
                } catch (NumberFormatException e) {
                    // skip
                }
            }
        }
        return maxRowCount;
    }

    /**
     * Check if a SQL is a pure SELECT (not SHOW/DESC/EXPLAIN).
     * Used to gate EXPLAIN pre-check.
     */
    private static boolean isSelectStatement(String sql) {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("SELECT")
            || trimmed.startsWith("WITH");
    }

    /**
     * Check if a SQL is DML (UPDATE/DELETE) that needs affected-row estimate.
     */
    private static boolean isDmlStatement(String sql) {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("UPDATE")
            || trimmed.startsWith("DELETE");
    }

    /**
     * Determine if a SQL statement is a query (returns ResultSet).
     */
    private static boolean isQueryStatement(String sql) {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("SELECT")
            || trimmed.startsWith("SHOW")
            || trimmed.startsWith("DESC")
            || trimmed.startsWith("EXPLAIN");
    }

    /**
     * Determine if a SQL statement is a transaction control statement.
     * These are always allowed regardless of security policy.
     */
    private static boolean isTransactionStatement(String sql) {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("BEGIN")
            || trimmed.startsWith("COMMIT")
            || trimmed.startsWith("ROLLBACK")
            || trimmed.startsWith("SAVEPOINT")
            || trimmed.startsWith("START");
    }

    /**
     * Determine if a SQL statement is a USE statement (schema switch).
     */
    private static boolean isUseStatement(String sql) {
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("USE ") || trimmed.equals("USE");
    }

    /**
     * Extract the target schema name from a USE statement.
     * Handles: USE db, USE `db`, USE "db", USE db; (trailing semicolon),
     * USE db ; SELECT ... (extra statements — only the first identifier is taken).
     */
    static String extractSchemaFromUse(String sql) {
        String trimmed = sql.trim();
        // Remove "USE" prefix (case-insensitive)
        String rest = trimmed.substring(3).trim();
        if (rest.isEmpty()) {
            return null;
        }
        // Take only the first token: stop at any whitespace or semicolon.
        // Backtick/double-quote-quoted identifiers are taken as-is up to the closing quote.
        String name;
        char first = rest.charAt(0);
        if (first == '`' || first == '"') {
            int end = rest.indexOf(first, 1);
            if (end < 0) {
                return null;
            }
            name = rest.substring(1, end);
        } else {
            int end = rest.length();
            for (int i = 0; i < rest.length(); i++) {
                char c = rest.charAt(i);
                if (Character.isWhitespace(c) || c == ';') {
                    end = i;
                    break;
                }
            }
            name = rest.substring(0, end);
        }
        return name.isEmpty() ? null : name;
    }

    /**
     * After locating the database identifier in a USE statement, check if there is
     * any non-whitespace content remaining (e.g. "; SELECT ..." indicating the LLM
     * passed multiple statements in one tool call).
     */
    static boolean hasTrailingStatementAfterUse(String sql, String schema) {
        String trimmed = sql.trim();
        // Skip the "USE" keyword.
        String rest = trimmed.substring(3).trim();
        // Skip the schema identifier (handles backtick/double-quote forms).
        if (!rest.isEmpty() && (rest.charAt(0) == '`' || rest.charAt(0) == '"')) {
            int end = rest.indexOf(rest.charAt(0), 1);
            rest = end < 0 ? "" : rest.substring(end + 1);
        } else {
            rest = rest.substring(schema.length());
        }
        // Strip trailing whitespace and a single optional terminating semicolon.
        rest = rest.trim();
        while (rest.startsWith(";")) {
            rest = rest.substring(1).trim();
        }
        return !rest.isEmpty();
    }

    /**
     * Handle transaction control statements with connection lifecycle management.
     * - BEGIN/START TRANSACTION: create a new shared connection, set autoCommit=false
     * - COMMIT/ROLLBACK: execute on shared connection, then close it
     * - SAVEPOINT: execute on shared connection, keep it open
     */
    private static SqlExecResult handleTransactionStatement(
        AgentSession session, String sql, String schemaName,
        String userName, String userHost, AgentConnectionContext connectionContext) {
        String trimmedUpper = sql.trim().toUpperCase();

        if (trimmedUpper.startsWith("BEGIN") || trimmedUpper.startsWith("START")) {
            // --- BEGIN TRANSACTION ---
            // If already in transaction, close old connection first (implicit rollback)
            if (session.isInTransaction()) {
                session.closeTransactionalConnection();
            }
            try {
                InnerConnection conn = createAgentConnection(
                    schemaName, userName, userHost, MasterSlave.MASTER_ONLY, connectionContext);
                conn.setAutoCommit(false);
                session.setTransactionalConnection(conn);
                return new SqlExecResult(
                    "OK, transaction started. Subsequent SQL will execute on the same connection.",
                    0, true);
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                nl2sqlLog.warn("Failed to begin transaction", e);
                return new SqlExecResult("Error starting transaction: " + msg, -1, false);
            }
        } else if (trimmedUpper.startsWith("COMMIT")) {
            // --- COMMIT ---
            if (!session.isInTransaction()) {
                return new SqlExecResult(
                    "No active transaction. Call BEGIN first before COMMIT.", -1, false);
            }
            try {
                session.getTransactionalConnection().commit();
                return new SqlExecResult("OK, transaction committed.", 0, true);
            } catch (Exception e) {
                return new SqlExecResult("Commit failed: "
                    + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()), -1, false);
            } finally {
                session.closeTransactionalConnection();
            }
        } else if (trimmedUpper.startsWith("ROLLBACK")) {
            // --- ROLLBACK (may include ROLLBACK TO SAVEPOINT xxx) ---
            if (!session.isInTransaction()) {
                return new SqlExecResult(
                    "No active transaction. Nothing to rollback.", 0, true);
            }
            boolean isRollbackToSavepoint = trimmedUpper.contains("TO");
            try {
                if (isRollbackToSavepoint) {
                    // ROLLBACK TO SAVEPOINT — execute as SQL, keep connection
                    return executeOnSharedConnection(
                        session.getTransactionalConnection(), sql, false);
                } else {
                    // Full ROLLBACK — close transaction
                    session.getTransactionalConnection().rollback();
                    return new SqlExecResult("OK, transaction rolled back.", 0, true);
                }
            } catch (Exception e) {
                return new SqlExecResult("Rollback failed: "
                    + (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()), -1, false);
            } finally {
                if (!isRollbackToSavepoint) {
                    session.closeTransactionalConnection();
                }
            }
        } else {
            // --- SAVEPOINT ---
            if (!session.isInTransaction()) {
                return new SqlExecResult(
                    "No active transaction. Call BEGIN first before setting a savepoint.",
                    -1, false);
            }
            return executeOnSharedConnection(
                session.getTransactionalConnection(), sql, false);
        }
    }

    /**
     * Execute SQL on an existing shared transactional connection.
     *
     * @param conn the shared connection (must not be null)
     * @param sql the SQL to execute
     * @param isQuery true if the SQL is a query (SELECT/SHOW/etc.), false for DML
     * @return execution result
     */
    private static SqlExecResult executeOnSharedConnection(
        java.sql.Connection conn, String sql, boolean isQuery) {
        // Intercept SHOW DATABASES even in transaction mode
        SqlExecResult showDbResult = tryHandleShowDatabases(sql);
        if (showDbResult != null) {
            return showDbResult;
        }

        try (Statement stmt = conn.createStatement()) {
            if (isQuery) {
                ResultSet rs = stmt.executeQuery(sql);
                if (rs == null) {
                    return new SqlExecResult("(empty result set, 0 rows)", 0, true);
                }
                return resultSetToResult(rs, MAX_RESULT_ROWS);
            } else {
                int affected = stmt.executeUpdate(sql);
                return new SqlExecResult("OK, " + affected + " rows affected.", affected, true);
            }
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            nl2sqlLog.warn("Error executing on shared connection: " + sql, e);
            return new SqlExecResult("SQL Error: " + msg, -1, false);
        }
    }

    /**
     * Check if the SQL is allowed to execute based on NL2SQL_ALLOWED_SQL_TYPES policy.
     *
     * @return null if allowed, otherwise the rejection reason
     */
    static String checkSqlTypeAllowed(String sql) {
        String policy = InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES);
        if (policy == null || policy.isEmpty()) {
            policy = "READ_ONLY";
        }
        policy = policy.trim().toUpperCase();

        if ("ALL".equals(policy)) {
            return null;
        }

        // Use ServerParse for known types
        int type = ServerParse.parse(sql);
        int cmdType = type & 0xff;

        // Transaction control statements are always allowed regardless of policy
        if (TRANSACTION_ALLOWED.contains(cmdType)) {
            return null;
        }

        // For OTHER/unrecognized, check prefix to identify DESCRIBE/EXPLAIN (allowed) vs DDL (blocked)
        if (type == ServerParse.OTHER) {
            String trimmed = sql.trim().toUpperCase();
            if (trimmed.startsWith("DESC") || trimmed.startsWith("EXPLAIN")) {
                return null; // Always allowed
            }
        }

        if ("READ_ONLY".equals(policy)) {
            if (!READ_ONLY_ALLOWED.contains(cmdType)) {
                return "Statement type not allowed in READ_ONLY mode. Only SELECT/SHOW/DESCRIBE/EXPLAIN are permitted.";
            }
            return null;
        }

        if ("READ_WRITE".equals(policy)) {
            if (!READ_WRITE_ALLOWED.contains(cmdType)) {
                return "Statement not allowed in READ_WRITE mode. Only SELECT/SHOW/DESCRIBE/EXPLAIN/INSERT/UPDATE/DELETE are permitted.";
            }
            return null;
        }

        // Unrecognized policy value — fail-safe: treat as READ_ONLY
        logger.warn("NL2SQL: unrecognized NL2SQL_ALLOWED_SQL_TYPES policy '" + policy
            + "', defaulting to READ_ONLY");
        if (!READ_ONLY_ALLOWED.contains(cmdType)) {
            return "Statement type not allowed (unrecognized policy '" + policy
                + "', defaulting to READ_ONLY). Only SELECT/SHOW/DESCRIBE/EXPLAIN are permitted.";
        }
        return null;
    }

    /**
     * Convert ResultSet to SqlExecResult with text table, row count, and raw data.
     */
    private static SqlExecResult resultSetToResult(ResultSet rs, int maxRows) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            // Capture column names for raw display
            List<String> columns = new ArrayList<>(colCount);
            // Header (for LLM text)
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= colCount; i++) {
                if (i > 1) {
                    sb.append(" | ");
                }
                String label = meta.getColumnLabel(i);
                columns.add(label);
                sb.append(label);
            }
            sb.append("\n");

            // Separator
            sb.append("---\n");

            // Data rows — wrap rs.next() in try-catch because some virtual table
            // ResultSet implementations throw NPE when result is empty
            List<List<String>> rawRows = new ArrayList<>();
            int rowCount = 0;
            boolean hasMore = false;
            boolean nextResult;
            try {
                nextResult = rs.next();
            } catch (NullPointerException npe) {
                rs.close();
                return new SqlExecResult("(empty result set, 0 rows)", 0, true, columns, rawRows);
            }
            while (nextResult) {
                if (rowCount >= maxRows) {
                    hasMore = true;
                    break;
                }
                List<String> rawRow = new ArrayList<>(colCount);
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) {
                        sb.append(" | ");
                    }
                    String val;
                    try {
                        val = rs.getString(i);
                    } catch (Exception ex) {
                        val = "<error>";
                    }
                    rawRow.add(val);
                    sb.append(val == null ? "NULL" : val);
                }
                rawRows.add(rawRow);
                sb.append("\n");
                rowCount++;
                try {
                    nextResult = rs.next();
                } catch (NullPointerException npe) {
                    break;
                }
            }

            if (hasMore) {
                sb.append("... (truncated, showing first ").append(maxRows).append(" rows)\n");
            }
            rs.close();

            if (rowCount == 0) {
                return new SqlExecResult("(empty result set, 0 rows)", 0, true,
                    columns, rawRows);
            }

            return new SqlExecResult(sb.toString(), rowCount, true, columns, rawRows);
        } catch (NullPointerException npe) {
            return new SqlExecResult("(empty result set, 0 rows)", 0, true);
        } catch (Exception e) {
            return new SqlExecResult("SQL Error: " + e.getClass().getSimpleName()
                + (e.getMessage() != null ? ": " + e.getMessage() : ""), -1, false);
        }
    }

    /**
     * Try to intercept AI_GET_REFERENCE / AI_GET_SKILL_PROMPT calls and serve them directly
     * from SkillManager in-memory cache, bypassing InnerConnection which may truncate long results.
     *
     * <p>Return value semantics:
     * <ul>
     *   <li>Non-null  — content string; AI_GET_SKILL_PROMPT is pre-formatted, AI_GET_REFERENCE
     *                   returns raw ref content (caller wraps with header/footer).
     *   <li>null      — not an interceptable call, fall through to normal SQL execution.
     * </ul>
     */
    private static String tryGetReferenceDirectly(String sql) {
        if (sql == null) {
            return null;
        }
        String upper = sql.trim().toUpperCase();
        try {
            if (upper.contains("AI_LIST_SKILLS")) {
                return SkillManager.getInstance().listSkills();
            }
            if (upper.contains("AI_GET_SKILL_PROMPT")) {
                Matcher m = Pattern.compile("'([^']*)'").matcher(sql);
                if (!m.find()) {
                    return null;
                }
                String skillName = m.group(1);
                SkillConfigRecord skill = SkillManager.getInstance().getSkillConfig(skillName);
                if (skill == null) {
                    return null;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("技能: ").append(skill.name).append("\n");
                if (skill.description != null && !skill.description.isEmpty()) {
                    sb.append("描述: ").append(skill.description).append("\n");
                }
                sb.append("\n=== 技能指令 ===\n").append(skill.prompt).append("\n=== 技能指令结束 ===\n");
                List<SkillReferenceRecord> refs = SkillManager.getInstance().getReferences(skillName);
                if (refs != null) {
                    for (SkillReferenceRecord ref : refs) {
                        sb.append("- ").append(ref.refName).append(" (")
                            .append(ref.content != null ? ref.content.length() : 0)
                            .append(" 字符, 获取: SELECT AI_GET_REFERENCE('")
                            .append(skillName).append("', '").append(ref.refName).append("'))\n");
                    }
                }
                return "=== 技能完整指令 ===\n" + sb + "\n=== 技能指令加载完毕 ===";
            }
            if (!upper.contains("AI_GET_REFERENCE")) {
                return null;
            }
            Matcher m = Pattern.compile("'([^']*)'").matcher(sql);
            if (!m.find()) {
                return null;
            }
            String skillName = m.group(1);
            if (!m.find()) {
                return null;
            }
            String refName = m.group(1);
            List<SkillReferenceRecord> refs = SkillManager.getInstance().getReferences(skillName);
            if (refs == null) {
                return null;
            }
            for (SkillReferenceRecord ref : refs) {
                if (refName.equals(ref.refName)) {
                    return ref.content;
                }
            }
            return null;
        } catch (Exception e) {
            nl2sqlLog.warn("Failed to serve AI skill helper from SQL: " + sql, e);
            return null;
        }
    }

}
