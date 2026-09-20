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

package com.alibaba.polardbx.executor.ai;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Session-level conversation state for NL2SQL Agent.
 * Stores the message history (OpenAI messages format) for multi-turn conversations.
 * Also manages transactional connection lifecycle and effective schema for USE support.
 * Lifecycle: bound to ServerConnection, cleared on connection close.
 */
public class AgentSession {

    private static final Logger logger = LoggerFactory.getLogger(AgentSession.class);

    /**
     * Unique identifier for this conversation session.
     * Used to correlate event logs across multiple turns.
     */
    private final String sessionId;

    private final JSONArray messages;
    private final int maxHistory;
    /**
     * Whether a system message has been stored at index 0.
     */
    private boolean hasSystemMessage;

    /**
     * Transactional connection shared across tool calls.
     * Non-null only when an explicit transaction is active (after BEGIN, before COMMIT/ROLLBACK).
     * Lifecycle: created on BEGIN, closed on COMMIT/ROLLBACK/clear().
     */
    private volatile IInnerConnection transactionalConnection;

    /**
     * Effective schema (database) for Agent SQL execution.
     * Updated by USE statements. When null, uses the ServerConnection's current schema.
     */
    private volatile String effectiveSchema;

    public AgentSession(int maxHistory) {
        this.sessionId = UUID.randomUUID().toString();
        this.messages = new JSONArray();
        this.maxHistory = maxHistory;
        this.hasSystemMessage = false;
        this.transactionalConnection = null;
        this.effectiveSchema = null;
    }

    /**
     * Ensure the system message is stored in session history.
     * Called by AgentService before adding conversation messages.
     */
    public synchronized void ensureSystemMessage(JSONObject systemMsg) {
        if (!hasSystemMessage) {
            messages.add(0, systemMsg);
            hasSystemMessage = true;
        } else {
            // Update existing system message (schema may have changed)
            messages.set(0, systemMsg);
        }
    }

    /**
     * Append a single message to history.
     */
    public synchronized void addMessage(JSONObject message) {
        messages.add(message);
        trimIfNeeded();
    }

    /**
     * Append multiple messages produced by one Agent loop iteration.
     */
    public synchronized void addMessages(List<JSONObject> msgs) {
        messages.addAll(msgs);
        trimIfNeeded();
    }

    /**
     * Get a copy of current history messages (for building LLM request).
     */
    public synchronized JSONArray getMessages() {
        return new JSONArray(new ArrayList<>(messages));
    }

    /**
     * Clear all history (called on connection close or schema switch).
     * Also closes any active transactional connection.
     */
    public String getSessionId() {
        return sessionId;
    }

    public synchronized void clear() {
        messages.clear();
        hasSystemMessage = false;
        closeTransactionalConnection();
        effectiveSchema = null;
    }

    // ==================== Transaction Connection Management ====================

    /**
     * Get the current transactional connection (null if no active transaction).
     */
    public IInnerConnection getTransactionalConnection() {
        return transactionalConnection;
    }

    /**
     * Set the transactional connection (called after BEGIN).
     */
    public void setTransactionalConnection(IInnerConnection conn) {
        this.transactionalConnection = conn;
    }

    /**
     * Close and clear the transactional connection.
     * Called on COMMIT/ROLLBACK/clear()/connection close.
     */
    public void closeTransactionalConnection() {
        IInnerConnection conn = this.transactionalConnection;
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.warn("Failed to close transactional connection", e);
            }
            this.transactionalConnection = null;
        }
    }

    /**
     * Check if a transaction is currently active.
     */
    public boolean isInTransaction() {
        return transactionalConnection != null;
    }

    // ==================== Effective Schema (USE support) ====================

    /**
     * Get the effective schema (null means use default from ServerConnection).
     */
    public String getEffectiveSchema() {
        return effectiveSchema;
    }

    /**
     * Set the effective schema (called by USE statement).
     */
    public void setEffectiveSchema(String schema) {
        this.effectiveSchema = schema;
    }

    public synchronized int size() {
        return messages.size();
    }

    /**
     * Replace all history with a compressed summary + specified conversation messages.
     * Removes ALL existing history (tool calls, tool results, old turns), then inserts
     * summary pair followed by the preserved conversation messages (user Q + final A).
     *
     * @param summaryContent compressed summary text produced by LLM
     * @param preservedConversation conversation messages to keep (user questions + final answers)
     */
    public synchronized void replaceHistoryWithSummaryAndConversation(String summaryContent,
                                                                      List<JSONObject> preservedConversation) {
        int startIdx = hasSystemMessage ? 1 : 0;

        // Remove all history after system message
        while (messages.size() > startIdx) {
            messages.remove(startIdx);
        }

        // Insert summary as a user message
        JSONObject summaryMsg = new JSONObject();
        summaryMsg.put("role", "user");
        summaryMsg.put("content",
            "[以下是之前对话的压缩摘要，请基于此摘要继续对话]\n" + summaryContent);
        messages.add(summaryMsg);

        JSONObject ackMsg = new JSONObject();
        ackMsg.put("role", "assistant");
        ackMsg.put("content", "好的，我已了解之前的对话内容，请继续。");
        messages.add(ackMsg);

        // Re-append the preserved conversation messages (user questions + final answers)
        for (JSONObject msg : preservedConversation) {
            messages.add(msg);
        }
    }

    /**
     * Trim old messages, keeping system message (index 0) + most recent messages.
     * Removes oldest user/assistant/tool messages in pairs to maintain conversation structure.
     * Keeps at most maxHistory*2 messages after the system message.
     */
    private void trimIfNeeded() {
        int startIdx = hasSystemMessage ? 1 : 0;
        int maxMessages = maxHistory * 2;
        while (messages.size() - startIdx > maxMessages) {
            // Remove the oldest message after system prompt
            messages.remove(startIdx);
        }
    }
}
