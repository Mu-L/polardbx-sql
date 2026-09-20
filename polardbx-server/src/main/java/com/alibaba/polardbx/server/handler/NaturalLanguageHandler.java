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

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ai.AgentSession;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;

import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.charset.CharsetName.POLAR_DB_X_STANDARD_UTF8_CHARSET_NAME;

/**
 * Entry point for Natural Language to SQL Agent.
 * Handles NL input detection, privilege check, Agent invocation, and result delivery.
 */
public final class NaturalLanguageHandler {

    private static final Logger logger = LoggerFactory.getLogger(NaturalLanguageHandler.class);

    private static final int SQL_DISPLAY_MAX_LEN = 120;

    /**
     * Force UTF-8 for all NL2SQL result sets, regardless of connection charset.
     * This prevents garbled Chinese when clients connect with latin1 or other non-UTF8 charsets.
     */
    private static final String NL_CHARSET = POLAR_DB_X_STANDARD_UTF8_CHARSET_NAME;
    private static final int NL_CHARSET_INDEX = 45; // utf8mb4

    /**
     * Handle a natural language input with streaming output.
     *
     * @param c the server connection
     * @param nlInput the natural language text
     * @param hasMore whether there are more statements in the batch
     */
    public static void handle(ServerConnection c, ByteString nlInput, boolean hasMore) {
        // 1. Check schema
        String schema = c.getSchema();
        if (schema == null || schema.isEmpty()) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                "No database selected. Use 'USE <database>' first.");
            return;
        }

        // 2. Get AgentSession
        AgentSession session = c.getAgentSession();

        // 3. Get model name
        String modelName = getModelName();

        // 3a. Check whether to show intermediate steps
        final boolean showSteps = InstConfUtil.getBool(ConnectionParams.NL2SQL_SHOW_STEPS);

        // 4. Streaming: maintain continuous packetId across all result sets
        final AtomicInteger packetIdHolder = new AtomicInteger(0);

        // 5. Create StreamingStepListener for progressive output
        AgentService.StreamingStepListener listener = new AgentService.StreamingStepListener() {
            private StreamingResultSetWriter currentWriter;

            @Override
            public void onStreamBegin(AgentService.AgentStep.StepType type) {
                String colName = "Answer";
                currentWriter = new StreamingResultSetWriter(c, colName, packetIdHolder);
            }

            @Override
            public void onStreamChunk(String chunk) {
                if (currentWriter != null) {
                    currentWriter.writeRow(chunk);
                }
            }

            @Override
            public void onStreamEnd(AgentService.AgentStep.StepType type) {
                if (currentWriter != null) {
                    boolean moreAfter = (type != AgentService.AgentStep.StepType.ANSWER) || hasMore;
                    currentWriter.finish(moreAfter);
                    currentWriter = null;
                }
            }

            @Override
            public void onStep(AgentService.AgentStep step) {
                // Skip intermediate steps when showSteps is false
                if (!showSteps && isIntermediateStep(step.type)) {
                    return;
                }

                ByteBufferHolder buf = c.allocate();
                IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buf);
                proxy.packetBegin();

                byte lastId = (byte) packetIdHolder.get();
                switch (step.type) {
                case SQL_ACTION:
                    lastId = writeSqlActionResultSet(proxy, c, step, (byte) packetIdHolder.get());
                    break;
                case SQL_RESULT:
                    lastId = writeSqlResultResultSet(proxy, c, step, (byte) packetIdHolder.get());
                    break;
                case PLAN:
                    lastId = writePlanResultSet(proxy, c, step, (byte) packetIdHolder.get());
                    break;
                }
                packetIdHolder.set(lastId);
                proxy.packetEnd();
            }
        };

        // 6. Call AgentService with streaming
        // Reset cancellation flag from any previous KILL QUERY on this connection.
        c.resetQueryCancelled();
        AgentConnectionContext connectionContext = AgentConnectionContext.capture(c);
        try {
            AgentService.chatStreaming(session, nlInput.toString(), schema, modelName,
                c.getUser(), c.getHost(), connectionContext, listener,
                () -> !c.isClosed() && !c.isQueryCancelled(),
                // Schema-switch callback: invoked when the agent runs USE <db>.
                // Sync the connection-level schema so subsequent native SQL
                // (typed by the user, not via the agent) also sees the new
                // database. Without this the user would hit "No database
                // selected" right after asking the agent to switch.
                newSchema -> {
                    if (newSchema != null && !newSchema.isEmpty()) {
                        c.setSchema(newSchema);
                        c.updateMDC();
                    }
                });
        } catch (Exception e) {
            logger.warn("NL Agent streaming error", e);
            c.writeErrMessage(ErrorCode.ERR_EXECUTOR, "NL Agent error: " + e.getMessage());
        }
    }

    /**
     * Check if NL2SQL is enabled and the user has privilege.
     * Session-level SET ENABLE_NL2SQL takes precedence over instance-level config.
     */
    public static boolean isEnabled(ServerConnection c) {
        boolean confEnabled;
        Object sessionValue = c.getConnectionVariables().get(
            ConnectionParams.ENABLE_NL2SQL.getName());
        if (sessionValue != null) {
            confEnabled = Boolean.parseBoolean(sessionValue.toString());
        } else {
            confEnabled = InstConfUtil.getBool(ConnectionParams.ENABLE_NL2SQL);
        }
        if (!confEnabled) {
            return false;
        }
        boolean hasPriv = hasNl2SqlPrivilege(c);
        if (!hasPriv) {
            logger.warn("NL2SQL: privilege check failed for user");
        }
        return hasPriv;
    }

    private static boolean hasNl2SqlPrivilege(ServerConnection c) {
        try {
            // PolarDB-X root user always has access
            if (c.isPolardbxRoot()) {
                return true;
            }
            // Get user info from PolarPrivManager directly (matchPolarUserInfo may not be set yet)
            PolarAccountInfo userInfo = c.getMatchPolarUserInfo();
            if (userInfo == null) {
                userInfo = PolarPrivManager.getInstance().getMatchUser(c.getUser(), c.getHost());
            }
            if (userInfo == null) {
                logger.warn("NL2SQL: cannot find user info for: " + c.getUser() + "@" + c.getHost());
                return false;
            }
            // God/DBA accounts always have access
            if (userInfo.getAccountType().isGod() || userInfo.getAccountType().isDBA()) {
                return true;
            }
            // Check NL2SQL instance-level privilege
            boolean hasPriv = userInfo.getInstPriv().hasPrivilege(PrivilegeKind.NL2SQL);
            if (!hasPriv) {
                logger.warn("NL2SQL: user " + c.getUser() + " has no NL2SQL priv. "
                    + "instPriv privileges: " + userInfo.getInstPriv().getPrivileges());
            }
            return hasPriv;
        } catch (Exception e) {
            logger.warn("NL2SQL: exception in privilege check for " + c.getUser(), e);
            return false;
        }
    }

    /**
     * Write SQL action result set — shows what SQL is being executed + reason.
     * Columns: #, SQL. Two rows: first is the SQL, second is the reason/intent.
     */
    private static byte writeSqlActionResultSet(IPacketOutputProxy proxy, ServerConnection c,
                                                AgentService.AgentStep step, byte startPacketId) {
        int fieldCount = 2;
        byte packetId = startPacketId;

        // Header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;
        proxy = header.write(proxy);

        // Fields — use VAR_STRING for # to allow empty string in reason row
        FieldPacket f1 = PacketUtil.getField("#", Fields.FIELD_TYPE_VAR_STRING);
        f1.charsetIndex = NL_CHARSET_INDEX;
        f1.length = 3;
        f1.packetId = ++packetId;
        proxy = f1.write(proxy);

        FieldPacket f2 = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        f2.charsetIndex = NL_CHARSET_INDEX;
        f2.length = 50;
        f2.packetId = ++packetId;
        proxy = f2.write(proxy);

        // EOF after fields
        byte[] pid = {packetId};
        proxy = writeEofIfDeprecated(c, proxy, pid);
        packetId = pid[0];

        // Row 1: SQL command
        RowDataPacket row = new RowDataPacket(fieldCount);
        row.add(StringUtil.encode(String.valueOf(step.sqlIndex), NL_CHARSET));
        row.add(StringUtil.encode(truncateSql(step.sql), NL_CHARSET));
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // Row 2: reason/intent (if provided)
        if (step.reason != null && !step.reason.isEmpty()) {
            RowDataPacket reasonRow = new RowDataPacket(fieldCount);
            reasonRow.add(StringUtil.encode("", NL_CHARSET));
            reasonRow.add(StringUtil.encode("-> " + step.reason, NL_CHARSET));
            reasonRow.packetId = ++packetId;
            proxy = reasonRow.write(proxy);
        }

        // EOF with MORE_RESULTS (always more after action)
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        proxy = lastEof.write(proxy);

        return packetId;
    }

    /**
     * Write SQL result set — directly sends the raw result set to client.
     * If raw columns/rows data is available, write it as a proper MySQL result set.
     * Otherwise, fall back to a simple text result (for DML/errors/references).
     */
    private static byte writeSqlResultResultSet(IPacketOutputProxy proxy, ServerConnection c,
                                                AgentService.AgentStep step, byte startPacketId) {
        byte packetId = startPacketId;

        // If we have raw result set data, send it directly
        if (step.columns != null && !step.columns.isEmpty()) {
            int fieldCount = step.columns.size();

            // Pre-calculate max display width per column for proper alignment
            int[] maxWidths = new int[fieldCount];
            for (int ci = 0; ci < fieldCount; ci++) {
                maxWidths[ci] = step.columns.get(ci).length();
            }
            if (step.rows != null) {
                for (java.util.List<String> rawRow : step.rows) {
                    for (int ci = 0; ci < fieldCount && ci < rawRow.size(); ci++) {
                        String val = rawRow.get(ci);
                        if (val != null && val.length() > maxWidths[ci]) {
                            maxWidths[ci] = val.length();
                        }
                    }
                }
            }

            // Header
            ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
            header.packetId = ++packetId;
            proxy = header.write(proxy);

            // Fields — use original column names with UTF-8 encoding
            for (int ci = 0; ci < fieldCount; ci++) {
                String colName = step.columns.get(ci);
                FieldPacket fp = new FieldPacket();
                fp.charsetIndex = NL_CHARSET_INDEX;
                fp.name = StringUtil.encode(colName, NL_CHARSET);
                fp.type = (byte) Fields.FIELD_TYPE_VAR_STRING;
                fp.length = maxWidths[ci];
                fp.packetId = ++packetId;
                proxy = fp.write(proxy);
            }

            // EOF after fields
            byte[] pid = {packetId};
            proxy = writeEofIfDeprecated(c, proxy, pid);
            packetId = pid[0];

            // Data rows
            if (step.rows != null) {
                for (java.util.List<String> rawRow : step.rows) {
                    RowDataPacket row = new RowDataPacket(fieldCount);
                    for (String val : rawRow) {
                        if (val == null) {
                            row.add(null);
                        } else {
                            row.add(StringUtil.encode(val, NL_CHARSET));
                        }
                    }
                    row.packetId = ++packetId;
                    proxy = row.write(proxy);
                }
            }

            // EOF with MORE_RESULTS
            EOFPacket lastEof = new EOFPacket();
            lastEof.packetId = ++packetId;
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
            proxy = lastEof.write(proxy);

            return packetId;
        }

        // Fallback: simple text result (for DML, errors, reference loads)
        int fieldCount = 1;
        String resultDisplay = step.resultText != null ? step.resultText : "";

        // Header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;
        proxy = header.write(proxy);

        // Field
        FieldPacket f1 = PacketUtil.getField("Result", Fields.FIELD_TYPE_VAR_STRING);
        f1.charsetIndex = NL_CHARSET_INDEX;
        f1.length = Math.max(24, resultDisplay.length());
        f1.packetId = ++packetId;
        proxy = f1.write(proxy);

        // EOF after fields
        byte[] pid = {packetId};
        proxy = writeEofIfDeprecated(c, proxy, pid);
        packetId = pid[0];

        // Single row with result text
        RowDataPacket row = new RowDataPacket(fieldCount);
        row.add(StringUtil.encode(resultDisplay, NL_CHARSET));
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // EOF with MORE_RESULTS
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        proxy = lastEof.write(proxy);

        return packetId;
    }

    /**
     * Write plan result set — shows the execution plan steps.
     * Columns: Step (index), Plan (description).
     */
    private static byte writePlanResultSet(IPacketOutputProxy proxy, ServerConnection c,
                                           AgentService.AgentStep step, byte startPacketId) {
        int fieldCount = 2;
        byte packetId = startPacketId;

        // Header
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        header.packetId = ++packetId;
        proxy = header.write(proxy);

        // Fields
        FieldPacket f1 = PacketUtil.getField("Step", Fields.FIELD_TYPE_VAR_STRING);
        f1.charsetIndex = NL_CHARSET_INDEX;
        f1.length = 4;
        f1.packetId = ++packetId;
        proxy = f1.write(proxy);

        FieldPacket f2 = PacketUtil.getField("Plan", Fields.FIELD_TYPE_VAR_STRING);
        f2.charsetIndex = NL_CHARSET_INDEX;
        f2.length = 60;
        f2.packetId = ++packetId;
        proxy = f2.write(proxy);

        // EOF after fields
        byte[] pid = {packetId};
        proxy = writeEofIfDeprecated(c, proxy, pid);
        packetId = pid[0];

        // Data rows — one row per plan step
        if (step.planSteps != null) {
            for (int i = 0; i < step.planSteps.size(); i++) {
                RowDataPacket row = new RowDataPacket(fieldCount);
                row.add(StringUtil.encode(String.valueOf(i + 1), NL_CHARSET));
                row.add(StringUtil.encode(step.planSteps.get(i), NL_CHARSET));
                row.packetId = ++packetId;
                proxy = row.write(proxy);
            }
        }

        // EOF with MORE_RESULTS (always more after plan)
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        proxy = lastEof.write(proxy);

        return packetId;
    }

    private static IPacketOutputProxy writeEofIfDeprecated(ServerConnection c,
                                                           IPacketOutputProxy proxy,
                                                           byte[] packetId) {
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++packetId[0];
            return eof.write(proxy);
        }
        return proxy;
    }

    private static String truncateSql(String sql) {
        if (sql == null) {
            return "";
        }
        // Remove newlines for display
        sql = sql.replace('\n', ' ').replace('\r', ' ');
        if (sql.length() > SQL_DISPLAY_MAX_LEN) {
            return sql.substring(0, SQL_DISPLAY_MAX_LEN) + "...";
        }
        return sql;
    }

    private static String getModelName() {
        String name = InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_MODEL_NAME);
        return (name == null || name.trim().isEmpty()) ? null : name.trim();
    }

    /**
     * Determine if a step type is an intermediate step that can be suppressed
     * when NL2SQL_SHOW_STEPS is false.
     */
    static boolean isIntermediateStep(AgentService.AgentStep.StepType type) {
        return type == AgentService.AgentStep.StepType.SQL_ACTION
            || type == AgentService.AgentStep.StepType.SQL_RESULT
            || type == AgentService.AgentStep.StepType.PLAN;
    }

    /**
     * Helper for streaming text result sets with progressive row flushing.
     * Writes header+fields+EOF immediately on construction, then each writeRow() flushes one row.
     * finish() sends the trailing EOF packet.
     * <p>
     * Single-column layout: Answer (content) only.
     */
    private static class StreamingResultSetWriter {
        private final ServerConnection c;
        private final AtomicInteger packetIdHolder;

        StreamingResultSetWriter(ServerConnection c, String columnName,
                                 AtomicInteger packetIdHolder) {
            this.c = c;
            this.packetIdHolder = packetIdHolder;

            // Immediately send header + fields + field-EOF
            ByteBufferHolder buf = c.allocate();
            IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buf);
            proxy.packetBegin();

            byte packetId = (byte) packetIdHolder.get();

            // Header: 1 field
            ResultSetHeaderPacket header = PacketUtil.getHeader(1);
            header.packetId = ++packetId;
            proxy = header.write(proxy);

            // Field: Answer content
            FieldPacket f1 = PacketUtil.getField(columnName, Fields.FIELD_TYPE_VAR_STRING);
            f1.charsetIndex = NL_CHARSET_INDEX;
            f1.length = 100;
            f1.packetId = ++packetId;
            proxy = f1.write(proxy);

            // EOF after fields
            byte[] pid = {packetId};
            proxy = writeEofIfDeprecated(c, proxy, pid);
            packetId = pid[0];

            packetIdHolder.set(packetId);
            proxy.packetEnd(); // Flush header to client
        }

        void writeRow(String text) {
            if (text == null) {
                return;
            }
            // Preserve blank lines explicitly emitted by TextChunker.
            if (!text.isEmpty()) {
                // Strip markdown formatting for plain-text MySQL terminal display
                text = stripMarkdown(text);
            }
            emitRow(text);
        }

        private void emitRow(String content) {
            ByteBufferHolder buf = c.allocate();
            IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buf);
            proxy.packetBegin();

            byte packetId = (byte) packetIdHolder.get();
            RowDataPacket row = new RowDataPacket(1);
            row.add(StringUtil.encode(content, NL_CHARSET));
            row.packetId = ++packetId;
            proxy = row.write(proxy);

            packetIdHolder.set(packetId);
            proxy.packetEnd(); // Flush this row to client immediately
        }

        void finish(boolean hasMore) {
            ByteBufferHolder buf = c.allocate();
            IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buf);
            proxy.packetBegin();

            byte packetId = (byte) packetIdHolder.get();

            EOFPacket lastEof = new EOFPacket();
            lastEof.packetId = ++packetId;
            if (hasMore) {
                lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
            }
            proxy = lastEof.write(proxy);

            packetIdHolder.set(packetId);
            proxy.packetEnd(); // Flush
        }
    }

    /**
     * Strip common markdown formatting for plain-text MySQL terminal display.
     * Handles: bold/italic, headings, code fences, inline code, list bullets.
     */
    private static String stripMarkdown(String text) {
        if (text == null) {
            return "";
        }
        // Remove code fence lines (```sql, ```, etc.)
        text = text.replaceAll("(?m)^\\s*```[a-z]*\\s*$", "");
        // Remove bold/italic markers: **text** -> text, *text* -> text, __text__ -> text
        text = text.replaceAll("\\*\\*(.+?)\\*\\*", "$1");
        text = text.replaceAll("__(.+?)__", "$1");
        text = text.replaceAll("(?<!\\*)\\*(?!\\*)(.+?)(?<!\\*)\\*(?!\\*)", "$1");
        // Remove heading markers: ## Title -> Title
        text = text.replaceAll("(?m)^#{1,6}\\s+", "");
        // Remove inline code backticks: `code` -> code
        text = text.replaceAll("`([^`]+)`", "$1");
        // Remove unordered list bullets: - item -> item, * item -> item
        text = text.replaceAll("(?m)^\\s*[\\-\\*]\\s+", "");
        return text;
    }
}
