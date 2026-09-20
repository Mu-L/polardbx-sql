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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link NaturalLanguageHandler}.
 * Covers stripMarkdown and truncateSql private static methods via reflection,
 * and the isIntermediateStep helper.
 */
public class NaturalLanguageHandlerTest {

    // ==================== stripMarkdown (private static, via reflection) ====================

    private String invokeStripMarkdown(String text) throws Exception {
        Method m = NaturalLanguageHandler.class.getDeclaredMethod("stripMarkdown", String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, text);
    }

    @Test
    public void testStripMarkdown_null() throws Exception {
        Assert.assertEquals("", invokeStripMarkdown(null));
    }

    @Test
    public void testStripMarkdown_noMarkdown() throws Exception {
        Assert.assertEquals("Hello world", invokeStripMarkdown("Hello world"));
    }

    @Test
    public void testStripMarkdown_bold() throws Exception {
        Assert.assertEquals("bold text", invokeStripMarkdown("**bold text**"));
    }

    @Test
    public void testStripMarkdown_italic() throws Exception {
        Assert.assertEquals("italic text", invokeStripMarkdown("*italic text*"));
    }

    @Test
    public void testStripMarkdown_underline() throws Exception {
        Assert.assertEquals("underline text", invokeStripMarkdown("__underline text__"));
    }

    @Test
    public void testStripMarkdown_heading() throws Exception {
        Assert.assertEquals("Title", invokeStripMarkdown("## Title"));
    }

    @Test
    public void testStripMarkdown_headingMultiple() throws Exception {
        Assert.assertEquals("Deep heading", invokeStripMarkdown("### Deep heading"));
    }

    @Test
    public void testStripMarkdown_codeFence() throws Exception {
        String input = "Before\n```sql\nSELECT 1\n```\nAfter";
        String result = invokeStripMarkdown(input);
        Assert.assertTrue(result.contains("Before"));
        Assert.assertTrue(result.contains("SELECT 1"));
        Assert.assertTrue(result.contains("After"));
        Assert.assertFalse(result.contains("```"));
    }

    @Test
    public void testStripMarkdown_inlineCode() throws Exception {
        Assert.assertEquals("use SELECT command", invokeStripMarkdown("use `SELECT` command"));
    }

    @Test
    public void testStripMarkdown_unorderedListDash() throws Exception {
        Assert.assertEquals("item1", invokeStripMarkdown("- item1").trim());
    }

    @Test
    public void testStripMarkdown_unorderedListStar() throws Exception {
        Assert.assertEquals("item2", invokeStripMarkdown("* item2").trim());
    }

    @Test
    public void testStripMarkdown_mixedFormatting() throws Exception {
        String input = "## **Bold Title**\n- item with `code`\n";
        String result = invokeStripMarkdown(input);
        Assert.assertFalse(result.contains("##"));
        Assert.assertFalse(result.contains("**"));
        Assert.assertFalse(result.contains("`"));
        Assert.assertTrue(result.contains("Bold Title"));
        Assert.assertTrue(result.contains("code"));
    }

    @Test
    public void testStripMarkdown_emptyString() throws Exception {
        Assert.assertEquals("", invokeStripMarkdown(""));
    }

    // ==================== truncateSql (private static, via reflection) ====================

    private String invokeTruncateSql(String sql) throws Exception {
        Method m = NaturalLanguageHandler.class.getDeclaredMethod("truncateSql", String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, sql);
    }

    @Test
    public void testTruncateSql_null() throws Exception {
        Assert.assertEquals("", invokeTruncateSql(null));
    }

    @Test
    public void testTruncateSql_shortSql() throws Exception {
        Assert.assertEquals("SELECT 1", invokeTruncateSql("SELECT 1"));
    }

    @Test
    public void testTruncateSql_exactLimit() throws Exception {
        // SQL_DISPLAY_MAX_LEN = 120
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 120; i++) {
            sb.append('x');
        }
        String sql = sb.toString();
        Assert.assertEquals(sql, invokeTruncateSql(sql));
    }

    @Test
    public void testTruncateSql_overLimit() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 130; i++) {
            sb.append('x');
        }
        String result = invokeTruncateSql(sb.toString());
        Assert.assertEquals(123, result.length()); // 120 + "..."
        Assert.assertTrue(result.endsWith("..."));
    }

    @Test
    public void testTruncateSql_newlinesReplaced() throws Exception {
        String sql = "SELECT\n*\nFROM\nt";
        String result = invokeTruncateSql(sql);
        Assert.assertFalse(result.contains("\n"));
        Assert.assertTrue(result.contains(" "));
    }

    // ==================== isIntermediateStep ====================

    @Test
    public void testIsIntermediateStep_sqlAction() {
        Assert.assertTrue(NaturalLanguageHandler.isIntermediateStep(
            AgentService.AgentStep.StepType.SQL_ACTION));
    }

    @Test
    public void testIsIntermediateStep_sqlResult() {
        Assert.assertTrue(NaturalLanguageHandler.isIntermediateStep(
            AgentService.AgentStep.StepType.SQL_RESULT));
    }

    @Test
    public void testIsIntermediateStep_plan() {
        Assert.assertTrue(NaturalLanguageHandler.isIntermediateStep(
            AgentService.AgentStep.StepType.PLAN));
    }

    @Test
    public void testIsIntermediateStep_thought_notIntermediate() {
        Assert.assertFalse(NaturalLanguageHandler.isIntermediateStep(
            AgentService.AgentStep.StepType.THOUGHT));
    }

    @Test
    public void testIsIntermediateStep_answer_notIntermediate() {
        Assert.assertFalse(NaturalLanguageHandler.isIntermediateStep(
            AgentService.AgentStep.StepType.ANSWER));
    }

    // ==================== isEnabled (session vs instance level) ====================

    @Test
    public void testIsEnabled_sessionTrue_overridesInstanceFalse() {
        ServerConnection c = Mockito.mock(ServerConnection.class);
        Map<String, Object> vars = new HashMap<>();
        vars.put(ConnectionParams.ENABLE_NL2SQL.getName(), "true");
        Mockito.when(c.getConnectionVariables()).thenReturn(vars);
        Mockito.when(c.isPolardbxRoot()).thenReturn(true);

        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_NL2SQL)).thenReturn(false);
            Assert.assertTrue(NaturalLanguageHandler.isEnabled(c));
        }
    }

    @Test
    public void testIsEnabled_sessionFalse_overridesInstanceTrue() {
        ServerConnection c = Mockito.mock(ServerConnection.class);
        Map<String, Object> vars = new HashMap<>();
        vars.put(ConnectionParams.ENABLE_NL2SQL.getName(), "false");
        Mockito.when(c.getConnectionVariables()).thenReturn(vars);

        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_NL2SQL)).thenReturn(true);
            Assert.assertFalse(NaturalLanguageHandler.isEnabled(c));
        }
    }

    @Test
    public void testIsEnabled_sessionAbsent_fallbackInstanceTrue() {
        ServerConnection c = Mockito.mock(ServerConnection.class);
        Mockito.when(c.getConnectionVariables()).thenReturn(new HashMap<>());
        Mockito.when(c.isPolardbxRoot()).thenReturn(true);

        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_NL2SQL)).thenReturn(true);
            Assert.assertTrue(NaturalLanguageHandler.isEnabled(c));
        }
    }

    @Test
    public void testIsEnabled_sessionAbsent_fallbackInstanceFalse() {
        ServerConnection c = Mockito.mock(ServerConnection.class);
        Mockito.when(c.getConnectionVariables()).thenReturn(new HashMap<>());

        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_NL2SQL)).thenReturn(false);
            Assert.assertFalse(NaturalLanguageHandler.isEnabled(c));
        }
    }

    @Test
    public void testIsEnabled_sessionTrue_notPolardbxRoot_noPriv() {
        ServerConnection c = Mockito.mock(ServerConnection.class);
        Map<String, Object> vars = new HashMap<>();
        vars.put(ConnectionParams.ENABLE_NL2SQL.getName(), "true");
        Mockito.when(c.getConnectionVariables()).thenReturn(vars);
        Mockito.when(c.isPolardbxRoot()).thenReturn(false);
        Mockito.when(c.getUser()).thenReturn("testuser");
        Mockito.when(c.getHost()).thenReturn("127.0.0.1");

        try (MockedStatic<InstConfUtil> mock = Mockito.mockStatic(InstConfUtil.class)) {
            mock.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_NL2SQL)).thenReturn(false);
            // PolarPrivManager.getInstance() returns null in unit test → hasNl2SqlPrivilege returns false
            Assert.assertFalse(NaturalLanguageHandler.isEnabled(c));
        }
    }
}
