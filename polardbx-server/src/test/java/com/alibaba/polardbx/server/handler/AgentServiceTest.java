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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit tests for {@link AgentService}.
 * Covers USE-statement parsing helpers, SQL type classification helpers,
 * checkSqlTypeAllowed policy logic, AgentStep/SqlExecResult
 * data classes, tryGetReferenceDirectly, ROWCOUNT_PATTERN/PHYSICAL_ROWS_PATTERN.
 */
public class AgentServiceTest {

    // ==================== USE-statement parsing ====================

    @Test
    public void testExtractSchema_simple() {
        Assert.assertEquals("wumu", AgentService.extractSchemaFromUse("USE wumu"));
        Assert.assertEquals("wumu", AgentService.extractSchemaFromUse("use wumu"));
        Assert.assertEquals("wumu", AgentService.extractSchemaFromUse("  USE   wumu  "));
    }

    @Test
    public void testExtractSchema_withTrailingSemicolon() {
        Assert.assertEquals("wumu", AgentService.extractSchemaFromUse("USE wumu;"));
        Assert.assertEquals("wumu", AgentService.extractSchemaFromUse("USE wumu ; "));
    }

    @Test
    public void testExtractSchema_multiStatement_takesOnlyFirstIdentifier() {
        Assert.assertEquals("ads",
            AgentService.extractSchemaFromUse("USE ads; SELECT * FROM ads_log LIMIT 3;"));
        Assert.assertEquals("wumu",
            AgentService.extractSchemaFromUse("USE wumu;SELECT 1"));
    }

    @Test
    public void testExtractSchema_backtickQuoted() {
        Assert.assertEquals("my db", AgentService.extractSchemaFromUse("USE `my db`"));
        Assert.assertEquals("ads", AgentService.extractSchemaFromUse("USE `ads`; SELECT 1"));
    }

    @Test
    public void testExtractSchema_doubleQuoted() {
        Assert.assertEquals("ads", AgentService.extractSchemaFromUse("USE \"ads\""));
    }

    @Test
    public void testExtractSchema_emptyOrInvalid() {
        Assert.assertNull(AgentService.extractSchemaFromUse("USE"));
        Assert.assertNull(AgentService.extractSchemaFromUse("USE   "));
        Assert.assertNull(AgentService.extractSchemaFromUse("USE `unterminated"));
    }

    @Test
    public void testHasTrailing_singleUse_false() {
        Assert.assertFalse(AgentService.hasTrailingStatementAfterUse("USE wumu", "wumu"));
        Assert.assertFalse(AgentService.hasTrailingStatementAfterUse("USE wumu;", "wumu"));
        Assert.assertFalse(AgentService.hasTrailingStatementAfterUse("USE wumu ; ; ", "wumu"));
        Assert.assertFalse(AgentService.hasTrailingStatementAfterUse("  USE   wumu  ", "wumu"));
    }

    @Test
    public void testHasTrailing_multiStatement_true() {
        Assert.assertTrue(AgentService.hasTrailingStatementAfterUse(
            "USE ads; SELECT * FROM ads_log LIMIT 3;", "ads"));
        Assert.assertTrue(AgentService.hasTrailingStatementAfterUse(
            "USE wumu;SELECT 1", "wumu"));
    }

    @Test
    public void testHasTrailing_backtickQuoted() {
        Assert.assertFalse(AgentService.hasTrailingStatementAfterUse("USE `ads`;", "ads"));
        Assert.assertTrue(AgentService.hasTrailingStatementAfterUse(
            "USE `ads`; SELECT 1", "ads"));
    }

    @Test
    public void testHasTrailing_doubleQuoted() {
        Assert.assertFalse(AgentService.hasTrailingStatementAfterUse("USE \"ads\"", "ads"));
        Assert.assertTrue(AgentService.hasTrailingStatementAfterUse(
            "USE \"ads\";SHOW TABLES", "ads"));
    }

    // ==================== SQL type classification (private, via reflection) ====================

    private boolean invokeIsQueryStatement(String sql) throws Exception {
        Method m = AgentService.class.getDeclaredMethod("isQueryStatement", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, sql);
    }

    private boolean invokeIsTransactionStatement(String sql) throws Exception {
        Method m = AgentService.class.getDeclaredMethod("isTransactionStatement", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, sql);
    }

    private boolean invokeIsUseStatement(String sql) throws Exception {
        Method m = AgentService.class.getDeclaredMethod("isUseStatement", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, sql);
    }

    private boolean invokeIsSelectStatement(String sql) throws Exception {
        Method m = AgentService.class.getDeclaredMethod("isSelectStatement", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, sql);
    }

    private boolean invokeIsDmlStatement(String sql) throws Exception {
        Method m = AgentService.class.getDeclaredMethod("isDmlStatement", String.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, sql);
    }

    @Test
    public void testIsQueryStatement() throws Exception {
        Assert.assertTrue(invokeIsQueryStatement("SELECT 1"));
        Assert.assertTrue(invokeIsQueryStatement("select * from t"));
        Assert.assertTrue(invokeIsQueryStatement("SHOW TABLES"));
        Assert.assertTrue(invokeIsQueryStatement("DESC t"));
        Assert.assertTrue(invokeIsQueryStatement("EXPLAIN SELECT 1"));
        Assert.assertFalse(invokeIsQueryStatement("INSERT INTO t VALUES(1)"));
        Assert.assertFalse(invokeIsQueryStatement("UPDATE t SET a=1"));
        Assert.assertFalse(invokeIsQueryStatement("DELETE FROM t"));
        Assert.assertFalse(invokeIsQueryStatement("BEGIN"));
        Assert.assertFalse(invokeIsQueryStatement("USE db"));
    }

    @Test
    public void testIsTransactionStatement() throws Exception {
        Assert.assertTrue(invokeIsTransactionStatement("BEGIN"));
        Assert.assertTrue(invokeIsTransactionStatement("COMMIT"));
        Assert.assertTrue(invokeIsTransactionStatement("ROLLBACK"));
        Assert.assertTrue(invokeIsTransactionStatement("SAVEPOINT sp1"));
        Assert.assertTrue(invokeIsTransactionStatement("START TRANSACTION"));
        Assert.assertFalse(invokeIsTransactionStatement("SELECT 1"));
        Assert.assertFalse(invokeIsTransactionStatement("INSERT INTO t VALUES(1)"));
        Assert.assertFalse(invokeIsTransactionStatement("USE db"));
    }

    @Test
    public void testIsUseStatement() throws Exception {
        Assert.assertTrue(invokeIsUseStatement("USE db"));
        Assert.assertTrue(invokeIsUseStatement("use db"));
        Assert.assertFalse(invokeIsUseStatement("SELECT 1"));
        Assert.assertTrue(invokeIsUseStatement("USE"));
        Assert.assertFalse(invokeIsUseStatement("USER TABLE"));
    }

    @Test
    public void testIsSelectStatement() throws Exception {
        Assert.assertTrue(invokeIsSelectStatement("SELECT 1"));
        Assert.assertTrue(invokeIsSelectStatement("  select * from t"));
        Assert.assertFalse(invokeIsSelectStatement("SHOW TABLES"));
        Assert.assertFalse(invokeIsSelectStatement("INSERT INTO t VALUES(1)"));
    }

    @Test
    public void testIsDmlStatement() throws Exception {
        Assert.assertTrue(invokeIsDmlStatement("UPDATE t SET a=1"));
        Assert.assertTrue(invokeIsDmlStatement("DELETE FROM t"));
        Assert.assertFalse(invokeIsDmlStatement("SELECT 1"));
        Assert.assertFalse(invokeIsDmlStatement("INSERT INTO t VALUES(1)"));
        Assert.assertFalse(invokeIsDmlStatement("SHOW TABLES"));
    }

    // ==================== checkSqlTypeAllowed (package-private) ====================

    private void mockInstConfUtilReadOnly(Runnable testBody) {
        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES))
                .thenReturn("");  // empty → default READ_ONLY
            testBody.run();
        }
    }

    private void mockInstConfUtilReadWrite(Runnable testBody) {
        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES))
                .thenReturn("READ_WRITE");
            testBody.run();
        }
    }

    private void mockInstConfUtilAll(Runnable testBody) {
        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES))
                .thenReturn("ALL");
            testBody.run();
        }
    }

    @Test
    public void testCheckSqlTypeAllowed_readOnly_select() {
        mockInstConfUtilReadOnly(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("SELECT 1"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readOnly_show() {
        mockInstConfUtilReadOnly(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("SHOW TABLES"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readOnly_use() {
        mockInstConfUtilReadOnly(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("USE testdb"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readOnly_insertBlocked() {
        mockInstConfUtilReadOnly(() -> {
            String result = AgentService.checkSqlTypeAllowed("INSERT INTO t VALUES(1)");
            Assert.assertNotNull(result);
            Assert.assertTrue(result.contains("READ_ONLY"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readOnly_updateBlocked() {
        mockInstConfUtilReadOnly(() -> {
            Assert.assertNotNull(AgentService.checkSqlTypeAllowed("UPDATE t SET a=1"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readOnly_deleteBlocked() {
        mockInstConfUtilReadOnly(() -> {
            Assert.assertNotNull(AgentService.checkSqlTypeAllowed("DELETE FROM t"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_transactionAlwaysAllowed() {
        mockInstConfUtilReadOnly(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("BEGIN"));
            Assert.assertNull(AgentService.checkSqlTypeAllowed("COMMIT"));
            Assert.assertNull(AgentService.checkSqlTypeAllowed("ROLLBACK"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readWrite_insertAllowed() {
        mockInstConfUtilReadWrite(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("INSERT INTO t VALUES(1)"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readWrite_updateAllowed() {
        mockInstConfUtilReadWrite(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("UPDATE t SET a=1"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_readWrite_deleteAllowed() {
        mockInstConfUtilReadWrite(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("DELETE FROM t"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_allPolicy_everythingAllowed() {
        mockInstConfUtilAll(() -> {
            Assert.assertNull(AgentService.checkSqlTypeAllowed("SELECT 1"));
            Assert.assertNull(AgentService.checkSqlTypeAllowed("INSERT INTO t VALUES(1)"));
            Assert.assertNull(AgentService.checkSqlTypeAllowed("UPDATE t SET a=1"));
        });
    }

    @Test
    public void testCheckSqlTypeAllowed_nullPolicy_defaultsToReadOnly() {
        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.NL2SQL_ALLOWED_SQL_TYPES))
                .thenReturn(null);
            Assert.assertNull(AgentService.checkSqlTypeAllowed("SELECT 1"));
            Assert.assertNotNull(AgentService.checkSqlTypeAllowed("INSERT INTO t VALUES(1)"));
        }
    }

    // ==================== AgentStep factory methods ====================

    @Test
    public void testAgentStep_sqlAction() {
        AgentService.AgentStep step = AgentService.AgentStep.sqlAction(1, "SELECT 1", "test reason");
        Assert.assertEquals(AgentService.AgentStep.StepType.SQL_ACTION, step.type);
        Assert.assertEquals(1, step.sqlIndex);
        Assert.assertEquals("SELECT 1", step.sql);
        Assert.assertEquals("test reason", step.reason);
        Assert.assertNull(step.resultText);
        Assert.assertTrue(step.success);
    }

    @Test
    public void testAgentStep_sqlResult() {
        AgentService.AgentStep step = AgentService.AgentStep.sqlResult(
            2, "result text", 10, 50L, true, null, null);
        Assert.assertEquals(AgentService.AgentStep.StepType.SQL_RESULT, step.type);
        Assert.assertEquals(2, step.sqlIndex);
        Assert.assertEquals("result text", step.resultText);
        Assert.assertEquals(10, step.rowCount);
        Assert.assertEquals(50L, step.timeMs);
        Assert.assertTrue(step.success);
    }

    @Test
    public void testAgentStep_sqlResult_withColumns() {
        List<String> cols = Arrays.asList("a", "b");
        List<List<String>> rows = new ArrayList<>();
        rows.add(Arrays.asList("1", "2"));
        AgentService.AgentStep step = AgentService.AgentStep.sqlResult(
            1, "text", 1, 10L, true, cols, rows);
        Assert.assertEquals(cols, step.columns);
        Assert.assertEquals(rows, step.rows);
    }

    @Test
    public void testAgentStep_plan() {
        List<String> steps = Arrays.asList("step1", "step2", "step3");
        AgentService.AgentStep step = AgentService.AgentStep.plan(steps);
        Assert.assertEquals(AgentService.AgentStep.StepType.PLAN, step.type);
        Assert.assertEquals(3, step.planSteps.size());
        Assert.assertEquals("step1", step.planSteps.get(0));
    }

    @Test
    public void testAgentStep_plan_empty() {
        AgentService.AgentStep step = AgentService.AgentStep.plan(new ArrayList<>());
        Assert.assertEquals(AgentService.AgentStep.StepType.PLAN, step.type);
        Assert.assertTrue(step.planSteps.isEmpty());
    }

    // ==================== SqlExecResult ====================

    @Test
    public void testSqlExecResult_simpleConstructor() {
        AgentService.SqlExecResult r = new AgentService.SqlExecResult("OK", 5, true);
        Assert.assertEquals("OK", r.text);
        Assert.assertEquals(5, r.rowCount);
        Assert.assertTrue(r.success);
        Assert.assertNull(r.columns);
        Assert.assertNull(r.rows);
    }

    @Test
    public void testSqlExecResult_fullConstructor() {
        List<String> cols = Arrays.asList("col1", "col2");
        List<List<String>> rows = new ArrayList<>();
        rows.add(Arrays.asList("val1", "val2"));
        AgentService.SqlExecResult r = new AgentService.SqlExecResult("result", 1, true, cols, rows);
        Assert.assertEquals("result", r.text);
        Assert.assertEquals(1, r.rowCount);
        Assert.assertTrue(r.success);
        Assert.assertEquals(cols, r.columns);
        Assert.assertEquals(rows, r.rows);
    }

    @Test
    public void testSqlExecResult_failure() {
        AgentService.SqlExecResult r = new AgentService.SqlExecResult("Error: bad", -1, false);
        Assert.assertFalse(r.success);
        Assert.assertEquals(-1, r.rowCount);
    }

    // ==================== tryGetReferenceDirectly (private, via reflection) ====================

    private String invokeTryGetReferenceDirectly(String sql) throws Exception {
        Method m = AgentService.class.getDeclaredMethod("tryGetReferenceDirectly", String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, sql);
    }

    @Test
    public void testTryGetReferenceDirectly_nullSql() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly(null));
    }

    @Test
    public void testTryGetReferenceDirectly_nonAiFunction() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly("SELECT 1"));
        Assert.assertNull(invokeTryGetReferenceDirectly("SHOW TABLES"));
    }

    @Test
    public void testTryGetReferenceDirectly_aiGetSkillPrompt_noQuote() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly("SELECT AI_GET_SKILL_PROMPT(no_quotes)"));
    }

    @Test
    public void testTryGetReferenceDirectly_aiGetSkillPrompt_unclosedQuote() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly("SELECT AI_GET_SKILL_PROMPT('unclosed)"));
    }

    @Test
    public void testTryGetReferenceDirectly_aiGetReference_noQuote() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly("SELECT AI_GET_REFERENCE()"));
    }

    @Test
    public void testTryGetReferenceDirectly_aiGetReference_missingSecondParam() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly("SELECT AI_GET_REFERENCE('skill')"));
    }

    @Test
    public void testTryGetReferenceDirectly_aiGetReference_missingFourthQuote() throws Exception {
        Assert.assertNull(invokeTryGetReferenceDirectly("SELECT AI_GET_REFERENCE('skill', 'ref"));
    }

    @Test
    public void testTryGetReferenceDirectly_aiListSkills() throws Exception {
        // AI_LIST_SKILLS may fail due to SkillManager not initialized, but should not throw
        try {
            invokeTryGetReferenceDirectly("SELECT AI_LIST_SKILLS()");
        } catch (Exception e) {
            // Expected in test environment without MetaDB
        }
    }

    // ==================== ROWCOUNT_PATTERN & PHYSICAL_ROWS_PATTERN (regex) ====================

    private Pattern getRowCountPattern() throws Exception {
        Field f = AgentService.class.getDeclaredField("ROWCOUNT_PATTERN");
        f.setAccessible(true);
        return (Pattern) f.get(null);
    }

    private Pattern getPhysicalRowsPattern() throws Exception {
        Field f = AgentService.class.getDeclaredField("PHYSICAL_ROWS_PATTERN");
        f.setAccessible(true);
        return (Pattern) f.get(null);
    }

    @Test
    public void testRowcountPattern_simple() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("rowcount=1000.0");
        Assert.assertTrue(m.find());
        Assert.assertEquals("1000.0", m.group(1));
    }

    @Test
    public void testRowcountPattern_integer() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("rowcount=500");
        Assert.assertTrue(m.find());
        Assert.assertEquals("500", m.group(1));
    }

    @Test
    public void testRowcountPattern_scientificNotation() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("rowcount=1.0E7");
        Assert.assertTrue(m.find());
        Assert.assertEquals("1.0E7", m.group(1));
    }

    @Test
    public void testRowcountPattern_negativeExponent() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("rowcount=1.5E-3");
        Assert.assertTrue(m.find());
        Assert.assertEquals("1.5E-3", m.group(1));
    }

    @Test
    public void testRowcountPattern_withSpaces() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("rowcount = 100.0");
        Assert.assertTrue(m.find());
        Assert.assertEquals("100.0", m.group(1));
    }

    @Test
    public void testRowcountPattern_noMatch() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("no rowcount here");
        Assert.assertFalse(m.find());
    }

    @Test
    public void testPhysicalRowsPattern() throws Exception {
        Pattern p = getPhysicalRowsPattern();
        Matcher m = p.matcher("rows:5000000");
        Assert.assertTrue(m.find());
        Assert.assertEquals("5000000", m.group(1));
    }

    @Test
    public void testPhysicalRowsPattern_noMatch() throws Exception {
        Pattern p = getPhysicalRowsPattern();
        Matcher m = p.matcher("no rows here");
        Assert.assertFalse(m.find());
    }

    @Test
    public void testRowcountPattern_multipleInLine() throws Exception {
        Pattern p = getRowCountPattern();
        Matcher m = p.matcher("rowcount=100.0 and rowcount=200.0");
        Assert.assertTrue(m.find());
        Assert.assertEquals("100.0", m.group(1));
        Assert.assertTrue(m.find());
        Assert.assertEquals("200.0", m.group(1));
    }

    // ==================== AgentStep.StepType enum ====================

    @Test
    public void testStepType_values() {
        AgentService.AgentStep.StepType[] types = AgentService.AgentStep.StepType.values();
        Assert.assertEquals(6, types.length);
        Assert.assertEquals(AgentService.AgentStep.StepType.SQL_ACTION, types[0]);
        Assert.assertEquals(AgentService.AgentStep.StepType.SQL_RESULT, types[1]);
        Assert.assertEquals(AgentService.AgentStep.StepType.THOUGHT, types[2]);
        Assert.assertEquals(AgentService.AgentStep.StepType.ANSWER, types[3]);
        Assert.assertEquals(AgentService.AgentStep.StepType.PLAN, types[4]);
        Assert.assertEquals(AgentService.AgentStep.StepType.CONTEXT_COMPRESS, types[5]);
    }

    @Test
    public void testStepType_valueOf() {
        Assert.assertEquals(AgentService.AgentStep.StepType.SQL_ACTION,
            AgentService.AgentStep.StepType.valueOf("SQL_ACTION"));
        Assert.assertEquals(AgentService.AgentStep.StepType.ANSWER,
            AgentService.AgentStep.StepType.valueOf("ANSWER"));
        Assert.assertEquals(AgentService.AgentStep.StepType.CONTEXT_COMPRESS,
            AgentService.AgentStep.StepType.valueOf("CONTEXT_COMPRESS"));
    }

    // ==================== estimateTokens ====================

    @Test
    public void testEstimateTokens_null() {
        Assert.assertEquals(0, AgentService.estimateTokens(null));
    }

    @Test
    public void testEstimateTokens_empty() {
        Assert.assertEquals(0, AgentService.estimateTokens(""));
    }

    @Test
    public void testEstimateTokens_asciiOnly() {
        // "hello world" = 11 ASCII chars => (11+3)/4 = 3 tokens
        Assert.assertEquals(3, AgentService.estimateTokens("hello world"));
    }

    @Test
    public void testEstimateTokens_cjkOnly() {
        // 3 CJK characters => 3 * 2 = 6 tokens
        Assert.assertEquals(6, AgentService.estimateTokens("你好吗"));
    }

    @Test
    public void testEstimateTokens_mixed() {
        // "hi你好" = 2 ASCII chars + 2 CJK chars
        // ASCII "hi" = max(1, (2+3)/4) = 1 token
        // CJK "你好" = 2 * 2 = 4 tokens
        // Total = 5
        Assert.assertEquals(5, AgentService.estimateTokens("hi你好"));
    }

    @Test
    public void testEstimateTokens_singleAscii() {
        // 1 ASCII char => max(1, (1+3)/4) = 1 token
        Assert.assertEquals(1, AgentService.estimateTokens("a"));
    }

    // ==================== estimateMessagesTokens ====================

    @Test
    public void testEstimateMessagesTokens_empty() {
        JSONArray messages = new JSONArray();
        // Only priming tokens = 2
        Assert.assertEquals(2, AgentService.estimateMessagesTokens(messages));
    }

    @Test
    public void testEstimateMessagesTokens_singleMessage() {
        JSONArray messages = new JSONArray();
        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "hello world");
        messages.add(msg);
        // 4 (overhead) + 3 ("hello world" tokens) + 2 (priming) = 9
        Assert.assertEquals(9, AgentService.estimateMessagesTokens(messages));
    }

    @Test
    public void testEstimateMessagesTokens_multipleMessages() {
        JSONArray messages = new JSONArray();
        JSONObject m1 = new JSONObject();
        m1.put("role", "user");
        m1.put("content", "hi");
        messages.add(m1);
        JSONObject m2 = new JSONObject();
        m2.put("role", "assistant");
        m2.put("content", "hello");
        messages.add(m2);
        // msg1: 4 + 1("hi") = 5
        // msg2: 4 + 2("hello"=(5+3)/4=2) = 6
        // priming: 2
        // total = 13
        Assert.assertEquals(13, AgentService.estimateMessagesTokens(messages));
    }

    @Test
    public void testEstimateMessagesTokens_withToolCalls() {
        JSONArray messages = new JSONArray();
        JSONObject msg = new JSONObject();
        msg.put("role", "assistant");
        msg.put("content", "");
        JSONArray toolCalls = new JSONArray();
        JSONObject tc = new JSONObject();
        JSONObject func = new JSONObject();
        func.put("name", "test");
        func.put("arguments", "{}");
        tc.put("function", func);
        toolCalls.add(tc);
        msg.put("tool_calls", toolCalls);
        messages.add(msg);
        // 4 (overhead) + 0 (empty content) + 1 ("test"=4/4=1) + 1 ("{}"=2/4→max1=1) + 2 (priming) = 8
        Assert.assertEquals(8, AgentService.estimateMessagesTokens(messages));
    }

    @Test
    public void testEstimateMessagesTokens_nullContent() {
        JSONArray messages = new JSONArray();
        JSONObject msg = new JSONObject();
        msg.put("role", "assistant");
        // no content key
        messages.add(msg);
        // 4 (overhead) + 0 (null content) + 2 (priming) = 6
        Assert.assertEquals(6, AgentService.estimateMessagesTokens(messages));
    }

    // ==================== isConversationMessage ====================

    @Test
    public void testIsConversationMessage_userMessage() {
        JSONObject msg = new JSONObject();
        msg.put("role", "user");
        msg.put("content", "查一下慢SQL");
        Assert.assertTrue(AgentService.isConversationMessage(msg));
    }

    @Test
    public void testIsConversationMessage_assistantFinalAnswer() {
        JSONObject msg = new JSONObject();
        msg.put("role", "assistant");
        msg.put("content", "查询结果如下...");
        Assert.assertTrue(AgentService.isConversationMessage(msg));
    }

    @Test
    public void testIsConversationMessage_assistantWithToolCalls() {
        JSONObject msg = new JSONObject();
        msg.put("role", "assistant");
        msg.put("content", "");
        JSONArray toolCalls = new JSONArray();
        JSONObject tc = new JSONObject();
        JSONObject func = new JSONObject();
        func.put("name", "query_sql");
        func.put("arguments", "{\"sql\":\"SELECT 1\"}");
        tc.put("function", func);
        toolCalls.add(tc);
        msg.put("tool_calls", toolCalls);
        Assert.assertFalse(AgentService.isConversationMessage(msg));
    }

    @Test
    public void testIsConversationMessage_assistantEmptyToolCalls() {
        JSONObject msg = new JSONObject();
        msg.put("role", "assistant");
        msg.put("content", "好的");
        msg.put("tool_calls", new JSONArray());
        Assert.assertTrue(AgentService.isConversationMessage(msg));
    }

    @Test
    public void testIsConversationMessage_toolMessage() {
        JSONObject msg = new JSONObject();
        msg.put("role", "tool");
        msg.put("content", "query result...");
        Assert.assertFalse(AgentService.isConversationMessage(msg));
    }
}
