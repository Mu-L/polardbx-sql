package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerLoadDataHandlerTest {
    List<SQLExpr> columnsListExpr = new ArrayList<>();
    List<String> columnsList = new ArrayList<>();

    @Before
    public void setUp() {
        columnsListExpr.clear();
        columnsList.clear();
    }

    /**
     * 测试用例1：正常情况 - 提供一组有效的 SQL 表达式列表，检查是否能正确转换并存储到目标列表中。
     */
    @Test
    public void testHandleColumnName_NormalCase() {
        // 准备
        SQLExpr expr1 = mock(SQLExpr.class);
        when(expr1.toString()).thenReturn("column1");
        SQLExpr expr2 = mock(SQLExpr.class);
        when(expr2.toString()).thenReturn("column2");

        columnsListExpr.add(expr1);
        columnsListExpr.add(expr2);

        // 执行
        ServerLoadDataHandler.handleColumnName(columnsListExpr, columnsList);

        // 验证
        assertEquals(2, columnsListExpr.size());
        assertTrue(columnsList.contains("column1"));
        assertTrue(columnsList.contains("column2"));
    }

    /**
     * 测试用例2：空列表 - 提供一个空的 SQL 表达式列表，检查方法的行为。
     */
    @Test
    public void testHandleColumnName_EmptyList() {
        // 执行
        ServerLoadDataHandler.handleColumnName(columnsListExpr, columnsList);

        // 验证
        assertEquals(0, columnsList.size());
    }

    /**
     * 测试用例3：包含异常的情况 - 模拟 SQL 表达式的转换过程中抛出异常，检查异常处理机制。
     */
    @Test
    public void testHandleColumnName_ExceptionHandling() {
        // 准备
        SQLExpr expr = mock(SQLExpr.class);
        when(expr.toString()).thenThrow(new RuntimeException("Simulated exception"));

        columnsListExpr.add(expr);

        // 执行
        ServerLoadDataHandler.handleColumnName(columnsListExpr, columnsList);

        // 验证
        assertEquals(0, columnsList.size()); // 确保没有添加任何元素
    }

    /**
     * 测试用例4：特殊字符处理 - 提供一些含有特殊字符的 SQL 表达式，检查其是否被正确规范化。
     */
    @Test
    public void testHandleColumnName_SpecialCharacters() {
        // 准备
        SQLExpr expr = mock(SQLExpr.class);
        when(expr.toString()).thenReturn("col\"umn'1");

        columnsListExpr.add(expr);

        // 执行
        ServerLoadDataHandler.handleColumnName(columnsListExpr, columnsList);

        // 验证
        assertEquals(1, columnsList.size());
        assertTrue(columnsList.get(0).equals("col\"umn'1")); // 假设 normalizeNoTrim 不会改变字符串内容
    }
}
