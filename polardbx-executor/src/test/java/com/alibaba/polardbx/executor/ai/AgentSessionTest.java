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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link AgentSession}.
 * Covers message management, system message handling, trimIfNeeded,
 * transaction connection lifecycle, and effective schema.
 */
public class AgentSessionTest {

    // ==================== Constructor & basic state ====================

    @Test
    public void testConstructor_defaults() {
        AgentSession session = new AgentSession(10);
        Assert.assertEquals(0, session.size());
        Assert.assertFalse(session.isInTransaction());
        Assert.assertNull(session.getEffectiveSchema());
        Assert.assertNull(session.getTransactionalConnection());
    }

    // ==================== ensureSystemMessage ====================

    @Test
    public void testEnsureSystemMessage_firstTime() {
        AgentSession session = new AgentSession(10);
        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "test system prompt");

        session.ensureSystemMessage(sysMsg);

        JSONArray msgs = session.getMessages();
        Assert.assertEquals(1, msgs.size());
        Assert.assertEquals("system", msgs.getJSONObject(0).getString("role"));
        Assert.assertEquals("test system prompt", msgs.getJSONObject(0).getString("content"));
    }

    @Test
    public void testEnsureSystemMessage_update() {
        AgentSession session = new AgentSession(10);

        JSONObject sys1 = new JSONObject();
        sys1.put("role", "system");
        sys1.put("content", "first prompt");
        session.ensureSystemMessage(sys1);

        JSONObject sys2 = new JSONObject();
        sys2.put("role", "system");
        sys2.put("content", "updated prompt");
        session.ensureSystemMessage(sys2);

        JSONArray msgs = session.getMessages();
        Assert.assertEquals(1, msgs.size());
        Assert.assertEquals("updated prompt", msgs.getJSONObject(0).getString("content"));
    }

    @Test
    public void testEnsureSystemMessage_preservesOtherMessages() {
        AgentSession session = new AgentSession(10);

        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "system");
        session.ensureSystemMessage(sysMsg);

        JSONObject userMsg = new JSONObject();
        userMsg.put("role", "user");
        userMsg.put("content", "hello");
        session.addMessage(userMsg);

        // Update system message
        JSONObject sysMsg2 = new JSONObject();
        sysMsg2.put("role", "system");
        sysMsg2.put("content", "system v2");
        session.ensureSystemMessage(sysMsg2);

        JSONArray msgs = session.getMessages();
        Assert.assertEquals(2, msgs.size());
        Assert.assertEquals("system v2", msgs.getJSONObject(0).getString("content"));
        Assert.assertEquals("hello", msgs.getJSONObject(1).getString("content"));
    }

    // ==================== addMessage / addMessages ====================

    @Test
    public void testAddMessage() {
        AgentSession session = new AgentSession(10);

        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "hello");
        session.addMessage(msg);

        Assert.assertEquals(1, session.size());
    }

    @Test
    public void testAddMessages() {
        AgentSession session = new AgentSession(10);

        List<JSONObject> msgs = new ArrayList<>();
        JSONObject m1 = new JSONObject();
        m1.put("role", "user");
        m1.put("content", "hello");
        msgs.add(m1);

        JSONObject m2 = new JSONObject();
        m2.put("role", "assistant");
        m2.put("content", "world");
        msgs.add(m2);

        session.addMessages(msgs);
        Assert.assertEquals(2, session.size());
    }

    @Test
    public void testAddMessages_emptyList() {
        AgentSession session = new AgentSession(10);
        session.addMessages(new ArrayList<>());
        Assert.assertEquals(0, session.size());
    }

    // ==================== getMessages returns copy ====================

    @Test
    public void testGetMessages_returnsCopy() {
        AgentSession session = new AgentSession(10);

        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "hello");
        session.addMessage(msg);

        JSONArray copy1 = session.getMessages();
        JSONArray copy2 = session.getMessages();

        // Modifying the copy should not affect the session
        copy1.remove(0);
        Assert.assertEquals(1, session.size());
        Assert.assertEquals(1, copy2.size());
    }

    // ==================== trimIfNeeded ====================

    @Test
    public void testTrimIfNeeded_withinLimit() {
        AgentSession session = new AgentSession(5);

        // Add system message + 4 user messages = 5 total (under limit of 5*2=10 non-system messages)
        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "system");
        session.ensureSystemMessage(sysMsg);

        for (int i = 0; i < 4; i++) {
            JSONObject msg = new JSONObject();
            msg.put("role", "user");
            msg.put("content", "msg" + i);
            session.addMessage(msg);
        }

        // System + 4 user = 5 messages
        Assert.assertEquals(5, session.size());
    }

    @Test
    public void testTrimIfNeeded_exceedsLimit() {
        AgentSession session = new AgentSession(2);

        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "system");
        session.ensureSystemMessage(sysMsg);

        // Add 6 messages beyond system. Max is 2*2=4, so should trim to 4
        for (int i = 0; i < 6; i++) {
            JSONObject msg = new JSONObject();
            msg.put("role", "user");
            msg.put("content", "msg" + i);
            session.addMessage(msg);
        }

        // System + max 4 = 5 messages total
        Assert.assertEquals(5, session.size());
        // System message preserved
        JSONArray msgs = session.getMessages();
        Assert.assertEquals("system", msgs.getJSONObject(0).getString("role"));
    }

    // ==================== clear ====================

    @Test
    public void testClear() {
        AgentSession session = new AgentSession(10);

        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "system");
        session.ensureSystemMessage(sysMsg);

        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "hello");
        session.addMessage(msg);

        session.setEffectiveSchema("testdb");

        session.clear();

        Assert.assertEquals(0, session.size());
        Assert.assertNull(session.getEffectiveSchema());
        Assert.assertFalse(session.isInTransaction());
    }

    // ==================== Transaction connection management ====================

    @Test
    public void testTransactionalConnection_setAndGet() {
        AgentSession session = new AgentSession(10);
        IInnerConnection mockConn = Mockito.mock(IInnerConnection.class);

        session.setTransactionalConnection(mockConn);
        Assert.assertTrue(session.isInTransaction());
        Assert.assertSame(mockConn, session.getTransactionalConnection());
    }

    @Test
    public void testTransactionalConnection_close() throws Exception {
        AgentSession session = new AgentSession(10);
        IInnerConnection mockConn = Mockito.mock(IInnerConnection.class);

        session.setTransactionalConnection(mockConn);
        session.closeTransactionalConnection();

        Assert.assertFalse(session.isInTransaction());
        Assert.assertNull(session.getTransactionalConnection());
        Mockito.verify(mockConn, Mockito.times(1)).close();
    }

    @Test
    public void testTransactionalConnection_closeWhenNull() {
        AgentSession session = new AgentSession(10);
        // Should not throw
        session.closeTransactionalConnection();
        Assert.assertFalse(session.isInTransaction());
    }

    @Test
    public void testTransactionalConnection_closeOnException() throws Exception {
        AgentSession session = new AgentSession(10);
        IInnerConnection mockConn = Mockito.mock(IInnerConnection.class);
        Mockito.doThrow(new RuntimeException("close error")).when(mockConn).close();

        session.setTransactionalConnection(mockConn);
        // Should not throw, just log
        session.closeTransactionalConnection();

        Assert.assertFalse(session.isInTransaction());
        Mockito.verify(mockConn, Mockito.times(1)).close();
    }

    @Test
    public void testClear_closesTransactionalConnection() throws Exception {
        AgentSession session = new AgentSession(10);
        IInnerConnection mockConn = Mockito.mock(IInnerConnection.class);

        session.setTransactionalConnection(mockConn);
        session.clear();

        Assert.assertFalse(session.isInTransaction());
        Mockito.verify(mockConn, Mockito.times(1)).close();
    }

    // ==================== Effective schema ====================

    @Test
    public void testEffectiveSchema_setAndGet() {
        AgentSession session = new AgentSession(10);

        Assert.assertNull(session.getEffectiveSchema());

        session.setEffectiveSchema("mydb");
        Assert.assertEquals("mydb", session.getEffectiveSchema());
    }

    @Test
    public void testEffectiveSchema_overwrite() {
        AgentSession session = new AgentSession(10);

        session.setEffectiveSchema("db1");
        session.setEffectiveSchema("db2");
        Assert.assertEquals("db2", session.getEffectiveSchema());
    }

    @Test
    public void testEffectiveSchema_clearResets() {
        AgentSession session = new AgentSession(10);

        session.setEffectiveSchema("mydb");
        session.clear();
        Assert.assertNull(session.getEffectiveSchema());
    }

    // ==================== size ====================

    @Test
    public void testSize_withSystemMessage() {
        AgentSession session = new AgentSession(10);

        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "system");
        session.ensureSystemMessage(sysMsg);

        Assert.assertEquals(1, session.size());

        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "hello");
        session.addMessage(msg);

        Assert.assertEquals(2, session.size());
    }

    @Test
    public void testSize_afterClear() {
        AgentSession session = new AgentSession(10);

        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "hello");
        session.addMessage(msg);

        session.clear();
        Assert.assertEquals(0, session.size());
    }

    // ==================== replaceHistoryWithSummaryAndConversation ====================

    @Test
    public void testReplaceHistoryWithSummaryAndConversation_basic() {
        AgentSession session = new AgentSession(50);
        JSONObject sysMsg = new JSONObject();
        sysMsg.put("role", "system");
        sysMsg.put("content", "system prompt");
        session.ensureSystemMessage(sysMsg);

        // Simulate: user Q1, tool_call, tool_result, final answer A1, user Q2, tool_call, tool_result, final answer A2
        for (int i = 0; i < 8; i++) {
            JSONObject msg = new JSONObject();
            msg.put("role", "user");
            msg.put("content", "msg" + i);
            session.addMessage(msg);
        }

        // Preserved conversation messages (simulating last Q+A pair)
        List<JSONObject> preserved = new ArrayList<>();
        JSONObject userQ = new JSONObject();
        userQ.put("role", "user");
        userQ.put("content", "recent question");
        preserved.add(userQ);
        JSONObject assistA = new JSONObject();
        assistA.put("role", "assistant");
        assistA.put("content", "recent answer");
        preserved.add(assistA);

        session.replaceHistoryWithSummaryAndConversation("tool calls summary", preserved);

        JSONArray msgs = session.getMessages();
        // system + summary_user + summary_ack + 2 preserved = 5
        Assert.assertEquals(5, msgs.size());
        Assert.assertEquals("system", msgs.getJSONObject(0).getString("role"));
        Assert.assertTrue(msgs.getJSONObject(1).getString("content").contains("tool calls summary"));
        Assert.assertEquals("assistant", msgs.getJSONObject(2).getString("role"));
        Assert.assertEquals("recent question", msgs.getJSONObject(3).getString("content"));
        Assert.assertEquals("recent answer", msgs.getJSONObject(4).getString("content"));
    }

    @Test
    public void testReplaceHistoryWithSummaryAndConversation_noSystem() {
        AgentSession session = new AgentSession(50);

        for (int i = 0; i < 6; i++) {
            JSONObject msg = new JSONObject();
            msg.put("role", "user");
            msg.put("content", "msg" + i);
            session.addMessage(msg);
        }

        List<JSONObject> preserved = new ArrayList<>();
        JSONObject q = new JSONObject();
        q.put("role", "user");
        q.put("content", "last Q");
        preserved.add(q);

        session.replaceHistoryWithSummaryAndConversation("summary", preserved);

        JSONArray msgs = session.getMessages();
        // summary_user + summary_ack + 1 preserved = 3
        Assert.assertEquals(3, msgs.size());
        Assert.assertTrue(msgs.getJSONObject(0).getString("content").contains("summary"));
        Assert.assertEquals("last Q", msgs.getJSONObject(2).getString("content"));
    }
}
